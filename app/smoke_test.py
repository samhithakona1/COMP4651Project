# Origin: original
"""Execute the home page and every dashboard page headlessly (streamlit.testing AppTest) and fail
on any exception. Used by `make dashboard-test` and the memory profile's dashboard stage."""
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parent
PAGES = [APP / "streamlit_app.py", *sorted((APP / "pages").glob("*.py"))]


def main() -> int:
    failed = 0
    for page in PAGES:
        at = AppTest.from_file(str(page), default_timeout=120).run()
        errors = [e.value for e in at.exception]
        status = "ok" if not errors else f"FAILED: {errors}"
        failed += bool(errors)
        print(f"{page.relative_to(APP.parent)}: {status} ({len(at.get('plotly_chart'))} charts, "
              f"{len(at.metric)} metrics)", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
