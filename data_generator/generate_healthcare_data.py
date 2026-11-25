"""
Synthetic health-plan data generator.

Writes daily batches the way payer source systems usually deliver them:

    raw/members/batch_date=YYYY-MM-DD/members.csv            member demographics (changes only after day 1)
    raw/eligibility/batch_date=YYYY-MM-DD/eligibility.csv    coverage spans
    raw/providers/batch_date=YYYY-MM-DD/providers.json       provider master (JSON lines)
    raw/claims/batch_date=YYYY-MM-DD/claim_headers.json      medical claim headers (JSON lines, dx array)
    raw/claim_lines/batch_date=YYYY-MM-DD/claim_lines.csv    medical claim service lines
    raw/pharmacy/batch_date=YYYY-MM-DD/pharmacy.parquet      pharmacy (Rx) claims

Realistic problems are injected so the pipeline has work to do: duplicate
submissions, replacement (freq 7) and void (freq 8) claims, invalid ICD-10
codes, NPIs that fail the check digit, unknown members, services outside the
coverage span, negative paid amounts, malformed NDCs and future dates.

Everything is fake. SSNs use the 900-999 area range, which is never issued,
and every email/phone/address is invented.

Usage:
    python data_generator/generate_healthcare_data.py --out data/raw --days 3
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

FIRST = [
    "James",
    "Mary",
    "Robert",
    "Linda",
    "Michael",
    "Susan",
    "David",
    "Karen",
    "Priya",
    "Arjun",
    "Wei",
    "Mei",
    "Carlos",
    "Sofia",
    "Ahmed",
    "Fatima",
    "Olivia",
    "Noah",
    "Emma",
    "Liam",
]
LAST = [
    "Smith",
    "Johnson",
    "Brown",
    "Garcia",
    "Miller",
    "Davis",
    "Patel",
    "Reddy",
    "Chen",
    "Nguyen",
    "Kim",
    "Khan",
    "Lopez",
    "Wilson",
    "Taylor",
]
GEO = [
    ("Hartford", "CT", "06103"),
    ("Bloomfield", "CT", "06002"),
    ("Nashville", "TN", "37203"),
    ("Philadelphia", "PA", "19103"),
    ("Denver", "CO", "80202"),
    ("Phoenix", "AZ", "85004"),
    ("Dallas", "TX", "75201"),
    ("Chicago", "IL", "60601"),
    ("Atlanta", "GA", "30303"),
]
PLANS = ["PPO-GOLD", "PPO-SILVER", "HMO-BASIC", "HDHP-HSA"]
SPECIALTIES = [
    ("Family Medicine", "207Q00000X"),
    ("Internal Medicine", "207R00000X"),
    ("Cardiology", "207RC0000X"),
    ("Orthopedic Surgery", "207X00000X"),
    ("Emergency Medicine", "207P00000X"),
    ("Radiology", "2085R0202X"),
    ("General Acute Care Hospital", "282N00000X"),
]
ICD10 = [
    "E11.9",
    "I10",
    "J06.9",
    "M54.50",
    "Z00.00",
    "F41.1",
    "K21.9",
    "E78.5",
    "N39.0",
    "R07.9",
    "S93.401A",
    "J45.909",
]
CPT = [
    ("99213", 95, 140),
    ("99214", 135, 200),
    ("99203", 110, 170),
    ("80053", 12, 35),
    ("85025", 8, 25),
    ("71046", 30, 90),
    ("93000", 18, 45),
    ("97110", 30, 60),
    ("73721", 220, 600),
    ("99284", 250, 700),
]
POS = ["11", "11", "11", "22", "23", "81"]
NDCS = [
    "00093-7146-56",
    "00378-0208-01",
    "00071-0155-23",
    "00006-0749-54",
    "59762-3304-01",
    "00173-0682-20",
    "68180-0513-01",
]


def npi_check_digit(first9: str) -> str:
    """Luhn check digit for an NPI (computed over the '80840' prefix + 9 digits)."""
    digits = [int(c) for c in "80840" + first9]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:  # positions that get doubled once the check digit is appended
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return str((10 - total % 10) % 10)


def make_npi(rng: random.Random) -> str:
    first9 = str(rng.choice([1, 2])) + "".join(str(rng.randint(0, 9)) for _ in range(8))
    return first9 + npi_check_digit(first9)


def _ts(day: date, rng: random.Random) -> str:
    t = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86_399))
    return t.strftime("%Y-%m-%d %H:%M:%S")


class HealthPlanSimulator:
    def __init__(self, seed: int, n_members: int, claims_per_day: int, start: date) -> None:
        self.rng = random.Random(seed)
        self.n_members = n_members
        self.claims_per_day = claims_per_day
        self.start = start
        self.members: dict[str, dict] = {}
        self.eligibility: list[dict] = []
        self.providers: list[dict] = []
        self.claims: dict[str, dict] = {}
        self._claim_seq = 0
        self._rx_seq = 0

    # ----------------------------------------------------------------- members
    def _new_member(self, idx: int, updated_at: str) -> dict:
        r = self.rng
        city, state, zipc = r.choice(GEO)
        dob = date(1940, 1, 1) + timedelta(days=r.randint(0, 365 * 80))
        return {
            "member_id": f"M{idx:07d}",
            "first_name": r.choice(FIRST),
            "last_name": r.choice(LAST),
            "ssn": f"{r.randint(900, 999)}-{r.randint(10, 99)}-{r.randint(1000, 9999)}",
            "date_of_birth": dob.isoformat(),
            "gender": r.choice(["F", "M"]),
            "address_line1": f"{r.randint(10, 9999)} {r.choice(['Oak', 'Elm', 'Main', 'Park', 'Lake'])} St",
            "city": city,
            "state": state,
            "zip_code": zipc,
            "phone": f"555-{r.randint(100, 999)}-{r.randint(1000, 9999)}",
            "plan_id": r.choice(PLANS),
            "updated_at": updated_at,
        }

    def member_batch(self, day: date, first: bool) -> list[dict]:
        rows = []
        if first:
            for i in range(1, self.n_members + 1):
                m = self._new_member(i, "2026-01-01 00:00:00")
                self.members[m["member_id"]] = m
                rows.append(dict(m))
            return rows
        # Plan changes and relocations -> SCD2 history in dim_member
        for m in self.rng.sample(list(self.members.values()), k=max(1, self.n_members // 25)):
            if self.rng.random() < 0.5:
                m["plan_id"] = self.rng.choice([p for p in PLANS if p != m["plan_id"]])
            else:
                m["city"], m["state"], m["zip_code"] = self.rng.choice(GEO)
            m["updated_at"] = _ts(day, self.rng)
            rows.append(dict(m))
        bad = dict(self.rng.choice(rows))
        rows.append({**bad, "member_id": "", "updated_at": _ts(day, self.rng)})  # missing key
        rows.append({**bad, "member_id": "M9999999", "date_of_birth": "2099-01-01"})  # DOB in future
        return rows

    def eligibility_batch(self, first: bool) -> list[dict]:
        if not first:
            return []
        rows = []
        for m in self.members.values():
            r = self.rng
            if r.random() < 0.9:  # continuously covered
                rows.append(
                    {
                        "member_id": m["member_id"],
                        "plan_id": m["plan_id"],
                        "coverage_start": "2026-01-01",
                        "coverage_end": "",
                    }
                )
            else:  # coverage lapsed before the claim period
                rows.append(
                    {
                        "member_id": m["member_id"],
                        "plan_id": m["plan_id"],
                        "coverage_start": "2025-01-01",
                        "coverage_end": "2026-09-30",
                    }
                )
        self.eligibility = rows
        return rows

    # --------------------------------------------------------------- providers
    def provider_batch(self, first: bool) -> list[dict]:
        if not first:
            return []
        rows = []
        for i in range(120):
            spec, taxonomy = self.rng.choice(SPECIALTIES)
            rows.append(
                {
                    "npi": make_npi(self.rng),
                    "provider_name": f"{self.rng.choice(LAST)} {'Medical Group' if i % 3 else 'Clinic'}",
                    "specialty": spec,
                    "taxonomy_code": taxonomy,
                    "state": self.rng.choice(GEO)[1],
                }
            )
        bad = dict(rows[0])
        bad["npi"] = bad["npi"][:-1] + str((int(bad["npi"][-1]) + 1) % 10)  # wrong check digit
        bad["provider_name"] = "Bad Checksum Provider"
        rows.append(bad)
        self.providers = rows[:-1]
        return rows

    # ------------------------------------------------------------------ claims
    def _new_claim(self, day: date) -> tuple[dict, list[dict]]:
        r = self.rng
        self._claim_seq += 1
        claim_id = f"CLM{day:%y%m%d}{self._claim_seq:07d}"
        member = r.choice(list(self.members.values()))
        prov = r.choice(self.providers)
        svc = day - timedelta(days=r.randint(1, 45))
        n_lines = r.choice([1, 1, 2, 3, 4])
        lines = []
        for ln in range(1, n_lines + 1):
            code, lo, hi = r.choice(CPT)
            units = r.choice([1, 1, 1, 2])
            billed = round(r.uniform(lo, hi) * 1.8 * units, 2)
            allowed = round(billed * r.uniform(0.45, 0.8), 2)
            paid = round(allowed * r.uniform(0.7, 1.0), 2)
            lines.append(
                {
                    "claim_id": claim_id,
                    "line_number": ln,
                    "procedure_code": code,
                    "units": units,
                    "billed_amount": billed,
                    "allowed_amount": allowed,
                    "paid_amount": paid,
                }
            )
        header = {
            "claim_id": claim_id,
            "original_claim_id": None,
            "frequency_code": "1",
            "member_id": member["member_id"],
            "billing_npi": prov["npi"],
            "claim_type": "INSTITUTIONAL" if prov["specialty"].endswith("Hospital") else "PROFESSIONAL",
            "place_of_service": r.choice(POS),
            "service_from_date": svc.isoformat(),
            "service_to_date": (svc + timedelta(days=r.choice([0, 0, 0, 1, 3]))).isoformat(),
            "diagnosis_codes": r.sample(ICD10, k=r.choice([1, 1, 2, 3])),
            "received_ts": _ts(day, r),
            "total_billed": round(sum(x["billed_amount"] for x in lines), 2),
        }
        return header, lines

    def claim_batch(self, day: date, first: bool) -> tuple[list[dict], list[dict]]:
        r = self.rng
        headers, lines = [], []
        for _ in range(self.claims_per_day):
            h, ls = self._new_claim(day)
            headers.append(h)
            lines.extend(ls)
            self.claims[h["claim_id"]] = {"header": h, "lines": ls}

        if not first:
            previous = [c for cid, c in self.claims.items() if c["header"]["received_ts"][:10] < day.isoformat()]
            # Replacement claims (freq 7): corrected amounts for an earlier claim
            for c in r.sample(previous, k=max(1, len(previous) // 50)):
                h, ls = self._new_claim(day)
                orig = c["header"]
                h.update(
                    {
                        k: orig[k]
                        for k in (
                            "member_id",
                            "billing_npi",
                            "claim_type",
                            "service_from_date",
                            "service_to_date",
                            "diagnosis_codes",
                            "place_of_service",
                        )
                    }
                )
                h.update({"frequency_code": "7", "original_claim_id": orig["claim_id"]})
                headers.append(h)
                lines.extend(ls)
            # Voids (freq 8): the earlier claim is cancelled
            for c in r.sample(previous, k=max(1, len(previous) // 100)):
                self._claim_seq += 1
                orig = c["header"]
                headers.append(
                    {
                        **orig,
                        "claim_id": f"CLM{day:%y%m%d}{self._claim_seq:07d}",
                        "frequency_code": "8",
                        "original_claim_id": orig["claim_id"],
                        "received_ts": _ts(day, r),
                        "total_billed": 0.0,
                    }
                )
            # Duplicate submissions (exact re-send)
            for h in r.sample(headers[: self.claims_per_day], k=max(1, self.claims_per_day // 100)):
                headers.append(dict(h))
            # Data-quality defects
            t, tl = self._new_claim(day)
            headers.append({**t, "diagnosis_codes": ["ZZZ.999"]})  # invalid ICD-10
            lines.extend(tl)
            t, tl = self._new_claim(day)
            headers.append({**t, "member_id": "M0000000"})  # unknown member
            lines.extend(tl)
            t, tl = self._new_claim(day)
            headers.append({**t, "billing_npi": "1234567890"})  # fails NPI check digit
            lines.extend(tl)
            t, tl = self._new_claim(day)
            headers.append({**t, "service_from_date": (day + timedelta(days=30)).isoformat()})  # future
            lines.extend(tl)
            t, tl = self._new_claim(day)
            headers.append(t)
            lines.extend([{**x, "paid_amount": -abs(x["paid_amount"])} for x in tl])  # negative paid
        return headers, lines

    def pharmacy_batch(self, day: date) -> list[dict]:
        r = self.rng
        rows = []
        for _ in range(self.claims_per_day // 2):
            self._rx_seq += 1
            qty = r.choice([30, 60, 90])
            cost = round(r.lognormvariate(3.2, 1.0), 2)
            rows.append(
                {
                    "rx_claim_id": f"RX{day:%y%m%d}{self._rx_seq:07d}",
                    "member_id": r.choice(list(self.members)) if r.random() > 0.002 else "M0000000",
                    "ndc": r.choice(NDCS),
                    "fill_date": (day - timedelta(days=r.randint(0, 5))).isoformat(),
                    "days_supply": qty,
                    "quantity": float(qty),
                    "ingredient_cost": cost,
                    "dispensing_fee": 1.75,
                    "paid_amount": round(cost * r.uniform(0.6, 0.95) + 1.75, 2),
                    "prescriber_npi": r.choice(self.providers)["npi"],
                }
            )
        rows.append({**rows[0], "rx_claim_id": f"RX{day:%y%m%d}BADNDC", "ndc": "12345"})
        return rows


def _csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def _jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def generate(out: Path, start: date, days: int, seed: int, members: int, claims: int) -> None:
    sim = HealthPlanSimulator(seed, members, claims, start)
    for i in range(days):
        day = start + timedelta(days=i)
        part = f"batch_date={day.isoformat()}"
        first = i == 0
        m = sim.member_batch(day, first)
        e = sim.eligibility_batch(first)
        p = sim.provider_batch(first)
        h, ln = sim.claim_batch(day, first)
        rx = sim.pharmacy_batch(day)
        _csv(out / "members" / part / "members.csv", m)
        _csv(out / "eligibility" / part / "eligibility.csv", e)
        _jsonl(out / "providers" / part / "providers.json", p)
        _jsonl(out / "claims" / part / "claim_headers.json", h)
        _csv(out / "claim_lines" / part / "claim_lines.csv", ln)
        (out / "pharmacy" / part).mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rx), out / "pharmacy" / part / "pharmacy.parquet")
        print(
            f"{day}: members={len(m):>5} eligibility={len(e):>5} providers={len(p):>4} "
            f"claims={len(h):>5} lines={len(ln):>6} rx={len(rx):>5}"
        )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=Path("data/raw"))
    p.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 11, 1))
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--members", type=int, default=2_000)
    p.add_argument("--claims-per-day", type=int, default=3_000)
    a = p.parse_args()
    generate(a.out, a.start_date, a.days, a.seed, a.members, a.claims_per_day)


if __name__ == "__main__":
    main()
