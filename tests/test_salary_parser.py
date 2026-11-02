# Origin: original
import pytest

from src.common.salary import parse_salary

K = 1000
CASES = [
    # --- ranges -------------------------------------------------------------------------------
    ("Acme | SWE | SF | $140k - $180k", (140 * K, 180 * K, "USD", "ok")),
    ("Acme | SWE | $140-180k + equity", (140 * K, 180 * K, "USD", "ok")),          # k on one bound
    ("Salary: 140,000 - 180,000 USD", (140 * K, 180 * K, "USD", "ok")),
    ("REMOTE | $150K–$200K", (150 * K, 200 * K, "USD", "ok")),                      # en dash, caps
    ("Comp: $140k to $180k base", (140 * K, 180 * K, "USD", "ok")),
    ("$120,000-$160,000 per year", (120 * K, 160 * K, "USD", "ok")),
    ("$170-240K + equity", (170 * K, 240 * K, "USD", "ok")),
    ("$113-197k + equity", (113 * K, 197 * K, "USD", "ok")),
    ("$130k–200k + equity", (130 * K, 200 * K, "USD", "ok")),
    ("Salary range $200,000 — $250,000", (200 * K, 250 * K, "USD", "ok")),
    ("USD 140k-180k", (140 * K, 180 * K, "USD", "ok")),
    ("US$150k - US$190k", (150 * K, 190 * K, "USD", "ok")),
    ("Salary: 120k-150k", (120 * K, 150 * K, "USD", "ok")),                         # no symbol, keyword
    ("$180,000 - 220,000", (180 * K, 220 * K, "USD", "ok")),
    ("pay is $95.5k-$120k", (95_500, 120 * K, "USD", "ok")),
    ("$180k-$140k", (140 * K, 180 * K, "USD", "ok")),                               # reversed bounds
    # --- single values ------------------------------------------------------------------------
    ("Senior Eng | up to $220k", (None, 220 * K, "USD", "ok")),
    ("compensation up to ~$750k cash", (None, 750 * K, "USD", "ok")),
    ("$160,000 base + bonus", (160 * K, 160 * K, "USD", "ok")),
    ("Base: 130k", (130 * K, 130 * K, "USD", "ok")),
    ("Remote (USA) | $90k", (90 * K, 90 * K, "USD", "ok")),
    ("$150k+ DOE", (150 * K, None, "USD", "ok")),
    # --- hourly / monthly ---------------------------------------------------------------------
    ("Contract | $85/hr", (85 * 2080, 85 * 2080, "USD", "hourly")),
    ("$100 - $125 per hour", (100 * 2080, 125 * 2080, "USD", "hourly")),
    ("GROWTH HACKER - $50/HR", (50 * 2080, 50 * 2080, "USD", "hourly")),
    ("$90-110 hourly", (90 * 2080, 110 * 2080, "USD", "hourly")),
    ("$8k/month", (96 * K, 96 * K, "USD", "ok")),
    # --- non-USD -----------------------------------------------------------------------------
    ("Berlin | €70k-€90k", (None, None, "EUR", "non_usd")),
    ("€70.000 - €90.000 salary", (None, None, "EUR", "non_usd")),
    ("London | £60,000 - £80,000", (None, None, "GBP", "non_usd")),
    ("Zurich | CHF 120k-150k", (None, None, "CHF", "non_usd")),
    ("Bangalore | ₹20,00,000 - ₹30,00,000", (None, None, "INR", "non_usd")),
    ("Toronto | $109K–$136K CAD", (None, None, "CAD", "non_usd")),
    ("Sydney | AU$120–160k", (None, None, "AUD", "non_usd")),
    ("CAD $120k - $140k", (None, None, "CAD", "non_usd")),
    ("Salary: 90,000 EUR", (None, None, "EUR", "non_usd")),
    # USD wins when both are present
    ("€80k (EU) or $120k-$150k (US)", (120 * K, 150 * K, "USD", "ok")),
    # --- no salary ----------------------------------------------------------------------------
    ("Salary: DOE", (None, None, None, "none")),
    ("Competitive salary and benefits", (None, None, None, "none")),
    ("Equity only (2-5%)", (None, None, None, "none")),
    ("Founded in 2015, 50 employees, 3 offices", (None, None, None, "none")),
    ("competitive salary, 401k matching", (None, None, None, "none")),
    ("competitive salary, 401(k)", (None, None, None, "none")),
    ("we engage 200,000+ traders globally", (None, None, None, "none")),
    ("profitable with 3k+ paid users", (None, None, None, "none")),
    ("", (None, None, None, "none")),
    (None, (None, None, None, "none")),
    # --- funding / perks false positives ---------------------------------------------------------
    ("We raised a $2M seed round", (None, None, None, "none")),
    ("Backed by a $50M Series B", (None, None, None, "none")),
    ("We just raised $500k pre-seed", (None, None, None, "none")),
    ("$500k in funding from top VCs", (None, None, None, "none")),
    ("We recently raised $200M at a $5B valuation", (None, None, None, "none")),
    ("$10k signing bonus", (None, None, None, "none")),
    ("$2,000 learning budget", (None, None, None, "none")),
    ("Raised $5.4M. Salary $150k-$180k", (150 * K, 180 * K, "USD", "ok")),
    ("$42M Series B. Pay: $160k - $200k + equity", (160 * K, 200 * K, "USD", "ok")),
    # --- bare k amounts in the header (no symbol, no keyword) -------------------------------------
    ("Acme | Scala Engineer | 90-150k | EQUITY", (90 * K, 150 * K, "USD", "ok")),
    ("SOLTECH | Technical Project Manager (90-115K) + Architect (120-150K)", (90 * K, 115 * K, "USD", "ok")),
    ("Acme | Engineers | 100K - 200K + equity | FULLTIME", (100 * K, 200 * K, "USD", "ok")),
    ("Acme | Oklahoma City | ONSITE | 150K + Full Benefits", (150 * K, 150 * K, "USD", "ok")),
    ("Acme | NYC\nOur iOS codebase is 75K+ lines of Objective-C, 90-150k users", (None, None, None, "none")),
    ("Acme | We serve 50K+ teachers | NYC", (None, None, None, "none")),
    # --- out of range ------------------------------------------------------------------------
    ("Freelance | $20k - $35k", (None, None, "USD", "out_of_range")),
    ("Intern | $25k", (None, None, "USD", "out_of_range")),
    ("Salary: $1,500,000 - $2,000,000", (None, None, "USD", "out_of_range")),
    ("$7.25/hr", (None, None, "USD", "out_of_range")),
    # out-of-range candidate skipped in favour of a later valid one
    ("$12k relocation and $15k sign-on; base $150k-$170k", (150 * K, 170 * K, "USD", "ok")),
]


@pytest.mark.parametrize("text,expected", CASES, ids=[str(i) for i in range(len(CASES))])
def test_parse_salary(text, expected):
    got = parse_salary(text)
    assert (got.min_usd, got.max_usd, got.currency, got.flag) == expected, text


def test_case_count():
    assert len(CASES) >= 40


def test_k_applies_to_both_bounds():
    """The report's regex read `$140-180k` as $140 - $180,000; both bounds must be thousands."""
    got = parse_salary("$140-180k")
    assert got.min_usd == 140_000 and got.max_usd == 180_000
