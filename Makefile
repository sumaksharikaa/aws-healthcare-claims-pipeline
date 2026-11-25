PY    ?= python
START ?= 2026-11-01
END   ?= 2026-11-03
DAYS  ?= 3

.PHONY: install data run demo test lint fmt wheel redshift-sql clean

install:      ## Install package with local Spark + dev tools
	$(PY) -m pip install -e ".[local,dev]"

data:         ## Generate synthetic raw files
	$(PY) data_generator/generate_healthcare_data.py --out data/raw --start-date $(START) --days $(DAYS)

run:          ## raw -> cleansed -> curated for START..END
	$(PY) -m claims_pipeline.pipeline --start-date $(START) --end-date $(END)

demo: clean data run

test:
	$(PY) -m pytest -q

lint:
	ruff check . && ruff format --check .

fmt:
	ruff check --fix . && ruff format .

wheel:
	$(PY) -m pip wheel . --no-deps -w dist/

redshift-sql: ## Print the Redshift load SQL without running it
	$(PY) -m claims_pipeline.redshift --dry-run --curated-prefix s3://my-bucket/curated \
		--iam-role-arn arn:aws:iam::123456789012:role/redshift-s3 --workgroup claims

clean:
	rm -rf data/ dist/ build/ spark-warehouse/ .pytest_cache
