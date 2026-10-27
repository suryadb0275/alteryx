"""Job entry point (serverless spark_python_task): convert the workflows in a Volume folder into notebooks.

    run_convert.py --input /Volumes/<c>/<s>/<v>/alteryx --out /Workspace/Users/<you>/migrations --catalog C --schema S [--decisions file.json]

A Volume is a normal folder on serverless compute, so the paths are used as they are. The exit code is 0 when everything converted; a tool that
needs an update does not fail the job (it is listed in conversion_report.md and in the job output), only a failure of the command itself does.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alteryx_migrate import cli  # noqa: E402

if __name__ == "__main__":
    code = cli.main(["convert", *sys.argv[1:]])
    sys.exit(1 if code == cli.FAILED else 0)
