import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "data_generator"))

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_MASTER", "local[2]")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    from claims_pipeline.spark import get_spark

    s = get_spark("claims-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return ROOT


@pytest.fixture(scope="session")
def cfg(repo_root):
    from claims_pipeline.config import load_config

    return load_config(repo_root / "conf" / "pipeline.yaml", root_override="/tmp/claims-test-root")
