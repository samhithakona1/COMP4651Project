# Origin: original
"""Regex rules shared by the Spark pipeline and the single-node Pandas baseline.

Every pattern here is written in the common subset of Java regex (Spark `rlike`,
`regexp_replace`, `regexp_extract`) and Python `re` (compiled with re.ASCII so `\\b`, `\\w` and
`(?i)` behave like Java's defaults). Flags are inline, e.g. `(?i)`, so one string works in both.
Replacement strings are plain literals; `spark_replacement()` escapes them for Java.
"""
from __future__ import annotations

import re

# ---- 1. HTML -> text ---------------------------------------------------------------------------
# Applied in order. HN comment HTML uses <p> as a paragraph separator, <a>/<i>/<pre>/<code> tags,
# and a handful of numeric/named entities.
HTML_RULES: list[tuple[str, str]] = [
    (r"(?i)<p>|</p>|<br\s*/?>", "\n"),
    (r"<[^>]*>", ""),
    (r"&#x27;|&#39;|&apos;", "'"),
    (r"&quot;|&#x22;|&#34;", '"'),
    (r"(?i)&#x2F;|&#47;", "/"),
    (r"(?i)&gt;|&#x3E;|&#62;", ">"),
    (r"(?i)&lt;|&#x3C;|&#60;", "<"),
    (r"(?i)&nbsp;|&#xA0;|&#160;|\u00a0", " "),
    (r"(?i)&ndash;|&#x2013;|&#8211;", "\u2013"),
    (r"(?i)&mdash;|&#x2014;|&#8212;", "\u2014"),
    (r"(?i)&#x24;|&#36;", "$"),
    (r"(?i)&euro;|&#x20AC;|&#8364;", "\u20ac"),
    (r"(?i)&pound;|&#xA3;|&#163;", "\u00a3"),
    (r"&amp;", "&"),
    (r"[ \t\r\f]+", " "),
    (r" ?\n ?", "\n"),
    (r"\n{2,}", "\n"),
    (r"^\s+|\s+$", ""),          # trim (Spark's trim() only strips spaces, so use a regex in both)
]

# ---- 2. Header line ----------------------------------------------------------------------------
# Company when the header has no `|`: text before a dash / colon / parenthesis.
COMPANY_FALLBACK_RE = r"^([^|,(\[:]{2,80}?)\s*(?:\s[-\u2013\u2014]\s|:|\(|\[)"
COMPANY_CLEAN_RULES: list[tuple[str, str]] = [
    (r"\([^)]*\)", ""),
    (r"(?i)https?://[^\s)]+", ""),
    (r"(?i)\bwww\.[^\s)]+", ""),
    (r"\s+", " "),
]
COMPANY_TRIM_RE = r"^[\s\-\u2013\u2014:,.;/*'\"()\[\]]+|[\s\-\u2013\u2014:,.;/*'\"()\[\]]+$"
COMPANY_SUFFIX_RE = r"\b(?:inc|llc|ltd|gmbh|corp|corporation|co|limited|plc|ag|bv|sas|pty)\b\.?"
COMPANY_KEY_STRIP_RE = r"[^a-z0-9]+"
COMPANY_MAX_LEN = 80

# ---- 4. Work modality --------------------------------------------------------------------------
REMOTE_RE = r"(?i)(?<!\bno )(?<!\bnot )(?<!\bnon-)(?<!\bnon )\bremote\b(?!\s*:\s*no\b)"
HYBRID_RE = (r"(?i)\bhybrid\b(?!\s+(?:cloud|apps?|mobile|infrastructure|environments?|models?|search"
             r"|approach|vehicles?|cars?|work\s+of))")
ONSITE_RE = r"(?i)\b(?:on-site|onsite|on site|in-office|in office|in-person|in person|office-based)\b"
MODALITY_ORDER = [("Remote", REMOTE_RE), ("Hybrid", HYBRID_RE), ("Onsite", ONSITE_RE)]

# ---- 5. Experience level (title keywords in the header; years of experience in the body) -------
STAFF_RE = r"(?i)\b(?:staff|principal|lead|architect|distinguished)\b"
SENIOR_RE = r"(?i)\b(?:senior|sr)\b"
MID_RE = r"(?i)\b(?:mid[- ]?level|mid[- ]career|intermediate)\b"
JUNIOR_RE = (r"(?i)\b(?:intern|interns|internships?|junior|jr|new[- ]grads?|new graduates?"
             r"|entry[- ]level|graduate|early[- ]career)\b")
LEVEL_ORDER = [("Staff_Principal", STAFF_RE), ("Senior", SENIOR_RE), ("Mid", MID_RE), ("Junior", JUNIOR_RE)]
YOE_RE = (r"(?i)\b(\d{1,2})\s*\+?\s*(?:(?:-|\u2013|to)\s*\d{1,2}\s*\+?\s*)?(?:years?|yrs?)"
          r"(?:\s+of)?(?:\s+[a-z/+.-]+){0,3}?\s+(?:experience|exp)\b")
YOE_MAX = 30
# yoe_min -> level, used only when the header has no title keyword.
YOE_LEVELS = [(0, 1, "Junior"), (2, 4, "Mid"), (5, YOE_MAX, "Senior")]

# ---- 6. Location (searched in the header; first match wins) ------------------------------------
US, CA, UK = "United States", "Canada", "United Kingdom"
# (city, state/province, country, is_tech_hub, pattern)
LOCATION_HUBS: list[tuple[str | None, str | None, str, bool, str]] = [
    ("San Francisco Bay Area", "CA", US, True,
     r"(?i)\b(?:san francisco|sf bay|bay area|sf|sfo|palo alto|mountain view|menlo park|san jose|sunnyvale"
     r"|redwood city|oakland|berkeley|san mateo|cupertino|santa clara|silicon valley|burlingame|emeryville"
     r"|south san francisco|foster city|los gatos|san carlos)\b"),
    ("New York City", "NY", US, True, r"(?i)\b(?:new york|nyc|manhattan|brooklyn)\b"),
    ("Seattle", "WA", US, True, r"(?i)\b(?:seattle|bellevue|redmond|kirkland)\b"),
    ("Boston", "MA", US, False, r"(?i)\b(?:boston|cambridge,? ma|somerville)\b"),
    ("Los Angeles", "CA", US, False, r"(?i)\b(?:los angeles|santa monica|culver city|pasadena|venice,? ca|la,? ca)\b"),
    ("Austin", "TX", US, False, r"(?i)\baustin\b"),
    ("Chicago", "IL", US, False, r"(?i)\bchicago\b"),
    ("Denver/Boulder", "CO", US, False, r"(?i)\b(?:denver|boulder)\b"),
    ("Washington", "DC", US, False, r"(?i)\b(?:washington,? d\.?c\.?|washington dc|arlington,? va)\b"),
    ("Atlanta", "GA", US, False, r"(?i)\batlanta\b"),
    ("San Diego", "CA", US, False, r"(?i)\bsan diego\b"),
    ("Portland", "OR", US, False, r"(?i)\bportland\b"),
    ("Miami", "FL", US, False, r"(?i)\bmiami\b"),
    ("Philadelphia", "PA", US, False, r"(?i)\bphiladelphia\b"),
    ("Pittsburgh", "PA", US, False, r"(?i)\bpittsburgh\b"),
    ("Salt Lake City", "UT", US, False, r"(?i)\b(?:salt lake city|lehi|provo)\b"),
    ("Raleigh-Durham", "NC", US, False, r"(?i)\b(?:raleigh|durham|chapel hill)\b"),
    ("Toronto", "ON", CA, False, r"(?i)\btoronto\b"),
    ("Vancouver", "BC", CA, False, r"(?i)\bvancouver\b"),
    ("Montreal", "QC", CA, False, r"(?i)\bmontr[eé]al\b"),
    ("London", None, UK, False, r"(?i)\blondon\b"),
    ("Berlin", None, "Germany", False, r"(?i)\bberlin\b"),
    ("Munich", None, "Germany", False, r"(?i)\b(?:munich|m[uü]nchen)\b"),
    ("Amsterdam", None, "Netherlands", False, r"(?i)\bamsterdam\b"),
    ("Paris", None, "France", False, r"(?i)\bparis\b"),
    ("Dublin", None, "Ireland", False, r"(?i)\bdublin\b"),
    ("Stockholm", None, "Sweden", False, r"(?i)\bstockholm\b"),
    ("Zurich", None, "Switzerland", False, r"(?i)\bz[uü]rich\b"),
    ("Barcelona", None, "Spain", False, r"(?i)\bbarcelona\b"),
    ("Tel Aviv", None, "Israel", False, r"(?i)\btel aviv\b"),
    ("Bangalore", "KA", "India", False, r"(?i)\b(?:bangalore|bengaluru)\b"),
    ("Singapore", None, "Singapore", False, r"(?i)\bsingapore\b"),
    ("Hong Kong", None, "Hong Kong", False, r"(?i)\bhong kong\b"),
    ("Tokyo", None, "Japan", False, r"(?i)\btokyo\b"),
    ("Sydney", "NSW", "Australia", False, r"(?i)\bsydney\b"),
    ("Melbourne", "VIC", "Australia", False, r"(?i)\bmelbourne\b"),
    # Country-level fallbacks (no city).
    (None, None, US, False, r"\b(?:USA|US)\b|U\.S\.|(?i:\bunited states\b)"),
    (None, None, UK, False, r"\bUK\b|(?i:\bunited kingdom\b|\bengland\b)"),
    (None, None, CA, False, r"(?i)\bcanada\b"),
    (None, None, "Germany", False, r"(?i)\bgermany\b"),
    (None, None, "India", False, r"(?i)\bindia\b"),
]


def location_key(city: str | None, state: str | None, country: str | None) -> str:
    if city is None and state is None and country is None:
        return "unknown"
    return "|".join(x or "" for x in (city, state, country))


# ---- compiled Python versions ------------------------------------------------------------------
def py(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.ASCII)


def py_sub(pattern: re.Pattern, repl: str, text: str) -> str:
    # Callable replacement: the literal is never interpreted (mirrors spark_replacement()).
    return pattern.sub(lambda _m: repl, text)


def spark_replacement(repl: str) -> str:
    """Escape a literal replacement for Java's Matcher.replaceAll ($ and \\ are special)."""
    return repl.replace("\\", "\\\\").replace("$", "\\$")


HTML_RULES_PY = [(py(p), r) for p, r in HTML_RULES]
COMPANY_CLEAN_PY = [(py(p), r) for p, r in COMPANY_CLEAN_RULES]


def clean_html(text: str | None) -> str | None:
    """Reference implementation of the Spark cleaning chain (used by tests and the baseline)."""
    if text is None:
        return None
    for pat, repl in HTML_RULES_PY:
        text = py_sub(pat, repl, text)
    return text
