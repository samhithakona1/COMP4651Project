# Origin: original
"""Draw 200 random HN postings for manual skill labelling -> results/labels/label_sample.csv.

Fill `true_skills` with the taxonomy names a human finds in the text, separated by "; "
(use "-" for "no taxonomy skill"). Leave it empty for rows not yet labelled.
An existing file is never overwritten (it may hold manual labels) unless --force is given.
"""
from __future__ import annotations

import argparse

import pandas as pd

from src.common import config
from src.common.log import get_logger

log = get_logger("label_sample")
SEED = 4651
OUT = config.RESULTS_DIR / "labels" / "label_sample.csv"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if OUT.exists() and not args.force:
        log.info("%s exists; keeping it (pass --force to redraw)", OUT)
        return
    silver = pd.read_parquet(config.SILVER_DIR, columns=["id", "source", "year", "month", "text_clean", "skills"])
    hn = silver[silver.source == "hn"]
    sample = hn.sample(n=min(args.n, len(hn)), random_state=SEED).sort_values(["year", "month", "id"])
    out = pd.DataFrame({
        "posting_key": "hn:" + sample.id.astype(str),
        "thread_month": sample.year.astype(str) + "-" + sample.month.astype(str).str.zfill(2),
        "text": sample.text_clean,
        "predicted_skills": sample.skills.map(lambda s: "; ".join(s)),
        "true_skills": "",
        "notes": "",
    })
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    log.info("wrote %d postings to %s", len(out), OUT)


if __name__ == "__main__":
    main()
