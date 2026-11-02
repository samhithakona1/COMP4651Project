# Origin: original
"""Salary extraction -> annual USD (salary_min_usd, salary_max_usd, salary_currency, salary_flag).

Shared by the Spark job (inside a pandas UDF; the multi-step logic below has no clean
built-in-function equivalent) and by the Pandas baseline, so both engines parse identically.

Rules (CLAUDE.md §4.3):
- ranges `$140k - $180k`, `$140-180k`, `140,000 - 180,000 USD`, `$150K–$200K`, `$140k to $180k`;
  singles `up to $220k` (max only), `$160,000 base`, `Base: 130k`, `$150k+` (min only)
- a `k` on either bound applies to every bound below 1000 (`$140-180k` = 140,000-180,000)
- hourly (`/hr`, `per hour`, `hourly`) x 2080; monthly (`/month`, `per month`) x 12
- non-USD (€, £, CHF, ₹, CAD, ...) -> NULL amounts with the currency recorded, flag `non_usd`
- no number (DOE, competitive, equity-only) -> flag `none`
- annualised < $30k or > $1.2M -> NULL, flag `out_of_range`
- funding / revenue / valuation amounts ("$2M seed", "$50M Series B", "raised $500k") are ignored,
  as are bonuses, stipends, budgets and 401k.
The first in-range USD candidate in text order wins; otherwise a non-USD candidate, otherwise an
out-of-range one.
"""
from __future__ import annotations

import re
from typing import NamedTuple

MIN_ANNUAL = 30_000
MAX_ANNUAL = 1_200_000
HOURS_PER_YEAR = 2080

_CUR_PREFIX = (r"US\$|USD\s?\$?|CAD\s?\$?|CA\$|C\$|AUD\s?\$?|AU\$|A\$|NZD\s?\$?|NZ\$|SGD\s?\$?|S\$|HKD\s?\$?|HK\$"
               r"|R\$|\$|€|£|¥|₹|EUR\s?|GBP\s?|CHF\s?|INR\s?|Rs\.?\s?|JPY\s?|SEK\s?|NOK\s?|DKK\s?|PLN\s?"
               r"|BRL\s?|ILS\s?|ZAR\s?|MXN\s?")
_CUR_SUFFIX = r"USD|EUR|GBP|CHF|CAD|AUD|NZD|SGD|HKD|INR|JPY|SEK|NOK|DKK|PLN|BRL|ILS|ZAR|MXN|€|£"
# Lakh grouping, comma thousands, dot thousands (European), plain/decimal.
_NUM = r"\d{1,2}(?:,\d{2})+,\d{3}(?!\d)|\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{1,3}(?:\.\d{3})+(?![\d,])|\d+(?:\.\d+)?"
_MULT = r"k(?![a-z])|(?:mm|mn|m|million|bn|b|billion)(?![a-z])"
_SEP = r"\s*(?:-|–|—|‑|to)\s*"
_PERIOD = (r"\s*(?:/\s?(?:hr|hour|h)\b|per\s+hour|an\s+hour|hourly|/\s?yr\b|/\s?year|per\s+(?:year|annum)"
           r"|annually|a\s+year|/\s?mo(?:nth)?\b|per\s+month|a\s+month|monthly)")

SALARY_RE = re.compile(
    rf"(?<![\w.,])(?P<cur1>{_CUR_PREFIX})?\s?(?P<a1>{_NUM})\s?(?P<m1>{_MULT})?"
    rf"(?:{_SEP}(?P<cur2>{_CUR_PREFIX})?\s?(?P<a2>{_NUM})\s?(?P<m2>{_MULT})?)?"
    rf"(?:\s?(?P<cur3>{_CUR_SUFFIX})(?![a-z]))?"
    rf"(?P<period>{_PERIOD})?",
    re.I,
)

_CURRENCY_CODES = {
    "$": "USD", "US$": "USD", "USD": "USD",
    "CAD": "CAD", "CA$": "CAD", "C$": "CAD",
    "AUD": "AUD", "AU$": "AUD", "A$": "AUD", "NZD": "NZD", "NZ$": "NZD",
    "SGD": "SGD", "S$": "SGD", "HKD": "HKD", "HK$": "HKD", "R$": "BRL", "BRL": "BRL",
    "€": "EUR", "EUR": "EUR", "£": "GBP", "GBP": "GBP", "CHF": "CHF",
    "₹": "INR", "INR": "INR", "RS": "INR", "RS.": "INR", "¥": "JPY", "JPY": "JPY",
    "SEK": "SEK", "NOK": "NOK", "DKK": "DKK", "PLN": "PLN", "ILS": "ILS", "ZAR": "ZAR", "MXN": "MXN",
}

_KEYWORD_PRE = re.compile(r"\b(?:salary|salaries|comp|compensation|base|pay|pays|paying|range|ote|tc"
                          r"|total comp|wage|wages|rate|earn|earning)\b", re.I)
_KEYWORD_POST = re.compile(r"^\W{0,3}(?:base|salary|/yr|per year|annual|ote|tc\b|depending|doe\b)", re.I)
_FUNDING_PRE = re.compile(r"\b(?:raised|raise|raising|funding|funded|financing|seed|series\s+[a-f]|valuation"
                          r"|valued|revenue|arr|mrr|investors?|invested|investment|backed|round|grant|grants"
                          r"|acquired|acquisition|gmv|sales|bookings|market|tam|worth|prize|loan|budget|spent"
                          r"|spend|savings|saved|processed|processing|transactions?)\b[^$€£\d.;!?|\n]{0,25}$", re.I)
_FUNDING_POST = re.compile(r"^\s{0,2}(?:in\s+)?(?:funding|seed|pre-seed|series|round|revenue|arr|mrr"
                           r"|valuation|raised|investment|grant|in\s+sales|gmv|users|customers|monthly active"
                           r"|transactions|payments|loans?|assets|aum|market)", re.I)
_PERK_PRE = re.compile(r"\b(?:bonus(?:es)?|stipends?|budget|allowance|relocation|referral|credits?|reimburse\w*"
                       r"|401\s?\(?k\)?(?:\s+match\w*)?|learning|education|conference|equipment|home office"
                       r"|signing|sign-on|gift)\b\W{0,6}(?:of\s+|up\s+to\s+|worth\s+)?$", re.I)
_PERK_POST = re.compile(r"^\s{0,2}(?:(?:signing|sign-on|annual|yearly|performance|referral|joining)\s+)?"
                        r"(?:bonus|stipend|budget|allowance|relocation|credit|401|equity grant|in equity"
                        r"|in options|in rsus?|rsus?|options|per employee|learning|education|conference"
                        r"|equipment|home office|referral|gift|for (?:the )?(?:right|best))", re.I)
_UP_TO = re.compile(r"\bup\s*to\s*~?\s*$", re.I)
_FOUR01K = re.compile(r"^40[13]\s?\(?k\)?", re.I)


class Salary(NamedTuple):
    min_usd: float | None
    max_usd: float | None
    currency: str | None
    flag: str  # ok | hourly | non_usd | out_of_range | none


NONE = Salary(None, None, None, "none")


def _num(s: str) -> float:
    s = s.strip()
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+", s):        # European thousands: 70.000
        return float(s.replace(".", ""))
    return float(s.replace(",", ""))


def _currency(m: re.Match) -> str | None:
    """An explicit non-USD marker anywhere in the match wins over a bare `$` ("$109K–$136K CAD")."""
    found = []
    for g in ("cur1", "cur2", "cur3"):
        raw = m.group(g)
        if raw:
            key = re.sub(r"\s+", "", raw).upper()
            key = key.rstrip("$") if key not in _CURRENCY_CODES and key.rstrip("$") in _CURRENCY_CODES else key
            if key in _CURRENCY_CODES:
                found.append(_CURRENCY_CODES[key])
    non_usd = [c for c in found if c != "USD"]
    return non_usd[0] if non_usd else (found[0] if found else None)


_FIELD_PRE = re.compile(r"(?:^|[|(/\-–—:])\s*$")
_FIELD_POST = re.compile(r"^\s*(?:$|[|)+,/\-–—]|\n)")


def _candidate(text: str, m: re.Match, header_end: int):
    """Return (lo, hi, currency, kind) for a salary-like match, or None if it is not a salary."""
    start, end = m.span()
    pre = text[max(0, start - 60):start]
    post = text[end:end + 40]
    span = m.group(0)
    if _FOUR01K.match(span.strip()):
        return None
    mults = [(x or "").lower() for x in (m.group("m1"), m.group("m2"))]
    if any(x and x != "k" for x in mults):
        return None  # $2M, $50MM, $1.5B: funding, revenue or valuation, never a salary
    if _FUNDING_PRE.search(pre) or _FUNDING_POST.search(post):
        return None
    if _PERK_PRE.search(pre) or _PERK_POST.search(post):
        return None

    a1 = _num(m.group("a1"))
    a2 = _num(m.group("a2")) if m.group("a2") else None
    has_k = "k" in mults
    period = (m.group("period") or "").lower()
    hourly = bool(re.search(r"hr|hour|/\s?h\b", period))
    monthly = bool(re.search(r"mo", period))
    currency = _currency(m)
    keyword = bool(_KEYWORD_PRE.search(pre[-40:]) or _KEYWORD_POST.search(post))

    if has_k:
        a1 = a1 * 1000 if a1 < 1000 else a1
        a2 = (a2 * 1000 if a2 < 1000 else a2) if a2 is not None else None
    if currency is None and not keyword:
        # HN headers often carry a bare "| 90-150k |": accept a k-amount in the header line when it is
        # a range or stands alone as a delimited field (never "50K+ users" or "75K lines").
        in_header = end <= header_end
        field = bool(_FIELD_PRE.search(text[max(0, start - 3):start]) and _FIELD_POST.match(text[end:end + 3]))
        if not (in_header and has_k and (m.group("a2") or field)):
            return None
    if currency is None and not (has_k or hourly or monthly or a1 >= 10_000):
        return None
    if not (has_k or hourly or monthly) and a1 < 1000 and (a2 is None or a2 < 1000):
        if currency not in (None, "USD") and keyword:
            pass  # e.g. "₹20 LPA salary": still a (non-USD) salary mention
        else:
            return None

    if a2 is not None:
        lo, hi = min(a1, a2), max(a1, a2)
        if lo < hi / 5:             # "$140k - 2 weeks": second number is not a bound
            lo, hi = a1, a1
        kind = "range"
    else:
        if _UP_TO.search(pre):
            lo, hi, kind = None, a1, "upto"
        elif post.startswith("+"):
            lo, hi, kind = a1, None, "plus"
        else:
            lo, hi, kind = a1, a1, "single"
    factor = HOURS_PER_YEAR if hourly else 12 if monthly else 1
    lo = lo * factor if lo is not None else None
    hi = hi * factor if hi is not None else None
    return lo, hi, currency or "USD", "hourly" if hourly else "ok"


def parse_salary(text: str | None) -> Salary:
    if not text:
        return NONE
    non_usd: Salary | None = None
    out_of_range: Salary | None = None
    header_end = text.find("\n")
    header_end = len(text) if header_end < 0 else header_end
    for m in SALARY_RE.finditer(text):
        if not m.group("a1"):
            continue
        cand = _candidate(text, m, header_end)
        if cand is None:
            continue
        lo, hi, currency, flag = cand
        if currency != "USD":
            non_usd = non_usd or Salary(None, None, currency, "non_usd")
            continue
        values = [v for v in (lo, hi) if v is not None]
        if min(values) < MIN_ANNUAL or max(values) > MAX_ANNUAL:
            out_of_range = out_of_range or Salary(None, None, "USD", "out_of_range")
            continue
        return Salary(float(lo) if lo is not None else None, float(hi) if hi is not None else None, "USD", flag)
    return non_usd or out_of_range or NONE
