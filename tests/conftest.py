import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FIXTURES = Path(__file__).parent / "fixtures"

_jh = os.environ.get("ALTERYX_MIG_JAVA_HOME")
if _jh:
    os.environ["JAVA_HOME"] = _jh
    os.environ["PATH"] = str(Path(_jh) / "bin") + os.pathsep + os.environ["PATH"]
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """Local Spark for the tests that run the generated SQL. Needs a JDK 17/21: set ALTERYX_MIG_JAVA_HOME. Without one those tests are SKIPPED (a run with skips is not a full pass)."""
    pytest.importorskip("pyspark")
    if not shutil.which("java"):
        pytest.skip("Java not found - Spark tests not executed")
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[1]").appName("alteryx-migrate-tests").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield session
    session.stop()
