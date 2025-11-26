from __future__ import annotations

import string
from typing import Any

import pandas as pd


def _alphabet_for(code: str) -> str:
    """Return an alphabet for generating mutations based on the code.

    Numeric codes -> digits.
    All-uppercase -> ASCII uppercase.
    All-lowercase -> ASCII lowercase.
    Mixed/other   -> uppercase + digits.
    """
    if code.isdigit():
        return string.digits
    if code.isalpha() and code.isupper():
        return string.ascii_uppercase
    if code.isalpha() and code.islower():
        return string.ascii_lowercase
    return string.ascii_uppercase + string.digits


def _mutations_same_length(code: str) -> list[str]:
    """Generate nearby codes with *the same length* as the input code.

    Allowed edits:
    - increment/decrement the last character within the chosen alphabet
    - replace the last character with the first/last character in the alphabet
    - for numeric codes, also try +1 as integer if the length stays the same
    """
    if not code:
        return []

    alphabet = _alphabet_for(code)
    candidates: list[str] = []
    base_len = len(code)

    # 1) Increment / decrement last char (same length)
    if alphabet:
        last = code[-1]
        if last in alphabet:
            idx = alphabet.index(last)
            if idx + 1 < len(alphabet):
                candidates.append(code[:-1] + alphabet[idx + 1])
            if idx - 1 >= 0:
                candidates.append(code[:-1] + alphabet[idx - 1])

    # 2) Replace last char with alphabet[0] and alphabet[-1]
    #    (only useful if it actually changes the char)
    if alphabet:
        repl_first = code[:-1] + alphabet[0]
        repl_last = code[:-1] + alphabet[-1]
        candidates.append(repl_first)
        candidates.append(repl_last)

    # 3) Numeric-specific: +1, but only keep if same length
    if code.isdigit():
        try:
            plus_one = str(int(code) + 1)
            if len(plus_one) == base_len:
                candidates.append(plus_one)
        except ValueError:
            # Ignore if int conversion fails for some weird reason
            pass

    # De-duplicate and drop the original code if it slipped in
    seen: set[str] = set()
    unique: list[str] = []
    for cand in candidates:
        if cand == code:
            continue
        if cand not in seen and len(cand) == base_len:
            seen.add(cand)
            unique.append(cand)

    return unique


def mutated_extra_codes(
    codes: pd.Series,
    coverage_pct: int = 100,
) -> pd.Series:
    """Return a new Series with similar-but-invalid codes appended.

    For a subset of the original codes (controlled by ``coverage_pct``),
    generate **one** nearby invalid code each and append them to the Series.

    The new codes obey these rules:
    - Same length as the original code they are derived from.
    - Character "type" is preserved:
        * digits -> only digits
        * uppercase letters -> only uppercase
        * lowercase letters -> only lowercase
        * other/mixed -> uppercase + digits

    Args:
        codes:
            Series of valid codes.
        coverage_pct:
            Percentage (0-100) of the original codes to mutate.
            Example: 50 means "roughly every other code" will get one extra
            invalid variant.

    Returns:
        New Series containing the original codes plus the generated invalid ones.
        The name and dtype of the original Series are preserved.
    """
    # Work only on non-null codes when generating mutations
    non_null = codes.dropna()
    base_list: list[str] = [str(c) for c in non_null]

    if not base_list or coverage_pct <= 0:
        return codes

    existing: set[str] = set(base_list)

    # Clamp to [1, 100]
    coverage_pct = max(1, min(coverage_pct, 100))

    # Step-based deterministic selection:
    # 100 -> step 1 (mutate all)
    # 50  -> step 2 (every other)
    # 25  -> step 4 (every 4th), etc.
    step = max(1, round(100 / coverage_pct))

    extras: list[str] = []
    for i, code in enumerate(base_list):
        if i % step != 0:
            continue

        for candidate in _mutations_same_length(code):
            if candidate in existing or candidate in extras:
                continue
            extras.append(candidate)
            break  # exactly one extra per selected code

    if not extras:
        return codes

    extras_series = pd.Series(extras, dtype=codes.dtype)
    extras_series.name = codes.name

    result: pd.Series[Any] = pd.concat(
        [codes.reset_index(drop=True), extras_series],
        ignore_index=True,
    )
    result.name = codes.name
    return result
