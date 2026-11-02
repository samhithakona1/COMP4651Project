# Origin: original
import pandas as pd
import pytest

from src.baseline.pandas_pipeline import enrich


def parse(texts):
    df = pd.DataFrame({"id": range(len(texts)), "text": texts, "source": "hn", "year": 2026, "month": 1})
    return enrich(df)


@pytest.mark.parametrize("text,expected", [
    ("Acme | SWE | REMOTE (US only) | $150k", "Remote"),
    ("Acme | SWE | Remote | Full-time", "Remote"),
    ("Acme | SWE | NYC | Hybrid (3 days)", "Hybrid"),
    ("Acme | SWE | SF | ONSITE", "Onsite"),
    ("Acme | SWE | SF | On-site", "Onsite"),
    ("Acme | SWE | Austin | In office", "Onsite"),
    ("Acme | SWE | Austin | On site (office!)", "Onsite"),
    ("Acme | SWE | London | No remote", "Unspecified"),
    ("Acme | SWE | Berlin<p>This role is fully remote.", "Remote"),          # body fallback
    ("Acme | SWE | Berlin | ONSITE<p>Some of the team is remote.", "Onsite"),  # header wins
    ("Acme | SWE | Berlin<p>We use hybrid cloud infrastructure.", "Unspecified"),
    ("Acme | SWE | Berlin", "Unspecified"),
])
def test_modality(text, expected):
    assert parse([text])["work_modality"].iloc[0] == expected


@pytest.mark.parametrize("text,level,yoe", [
    ("Acme | Senior Backend Engineer | SF", "Senior", None),
    ("Acme | Sr. Data Engineer | SF", "Senior", None),
    ("Acme | Staff Engineer | SF", "Staff_Principal", None),
    ("Acme | Principal ML Engineer | NYC", "Staff_Principal", None),
    ("Acme | Tech Lead | NYC", "Staff_Principal", None),
    ("Acme | Solutions Architect | NYC", "Staff_Principal", None),
    ("Acme | Software Engineering Intern | NYC", "Junior", None),
    ("Acme | Junior Developer | NYC", "Junior", None),
    ("Acme | New Grad SWE | NYC", "Junior", None),
    ("Acme | Mid-level Engineer | NYC", "Mid", None),
    ("Acme | Software Engineer | NYC<p>You have 5+ years of experience with Go.", "Senior", 5),
    ("Acme | Software Engineer | NYC<p>3-5 years of professional experience", "Mid", 3),
    ("Acme | Software Engineer | NYC<p>1+ years experience", "Junior", 1),
    ("Acme | Senior Engineer | NYC<p>2+ years of experience", "Senior", 2),   # title keyword wins
    ("Acme | Software Engineer | NYC<p>We were founded 10 years ago.", "Unspecified", None),
])
def test_experience(text, level, yoe):
    row = parse([text]).iloc[0]
    assert row["experience_level"] == level
    assert (None if pd.isna(row["yoe_min"]) else int(row["yoe_min"])) == yoe


@pytest.mark.parametrize("text,company,city,hub", [
    ("Acme Corp | SWE | San Francisco, CA | REMOTE", "Acme Corp", "San Francisco Bay Area", True),
    ("Baserow (https://baserow.io) | Product | REMOTE (Europe)", "Baserow", None, False),
    ("Stripe | SWE | NYC or Seattle", "Stripe", "New York City", True),
    ("Initech | SWE | Bellevue, WA", "Initech", "Seattle", True),
    ("Globex | SWE | London, UK", "Globex", "London", False),
    ("Hooli - Senior SWE - Mountain View", "Hooli", "San Francisco Bay Area", True),
])
def test_company_location(text, company, city, hub):
    row = parse([text]).iloc[0]
    assert row["company_name_parsed"] == company
    assert row["city"] == city
    assert bool(row["is_tech_hub"]) is hub


def test_spark_matches_pandas_on_fixtures(spark):
    """Spark built-ins and the Pandas baseline agree on every parsed column."""
    from src.spark.clean_parse import enrich as spark_enrich
    texts = ["Acme | Senior SWE | San Francisco | REMOTE (US only) | $140-180k<p>5+ years of experience",
             "Globex - Staff Eng - London<p>Hybrid. &pound;90k",
             "Hooli | Intern | Seattle | ONSITE | $45/hr<p>We&#x27;re hiring &amp; growing",
             "no header pipes at all, just prose about remote work"]
    pdf = pd.DataFrame({"id": range(len(texts)), "text": texts, "source": "hn", "year": 2026, "month": 1})
    s = spark_enrich(spark.createDataFrame(pdf)).toPandas().sort_values("id").reset_index(drop=True)
    p = enrich(pdf).sort_values("id").reset_index(drop=True)
    cols = ["text_clean", "header_line", "company_name_parsed", "company_key", "city", "state_province", "country",
            "is_tech_hub", "work_modality", "experience_level", "yoe_min", "salary_min_usd", "salary_max_usd",
            "salary_currency", "salary_flag"]
    for c in cols:
        a = [None if pd.isna(x) else x for x in s[c]]
        b = [None if pd.isna(x) else x for x in p[c]]
        assert a == b, (c, a, b)
