#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lightweight token estimation utilities.

Notes:
- This is an estimate (not model-exact tokenization).
- Designed for budgeting / chunk grouping decisions.
"""

import math
from typing import Any


def _is_cjk(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    return (
        0x4E00 <= code <= 0x9FFF
        or 0x3400 <= code <= 0x4DBF
        or 0x20000 <= code <= 0x2A6DF
        or 0x2A700 <= code <= 0x2B73F
        or 0x2B740 <= code <= 0x2B81F
        or 0x2B820 <= code <= 0x2CEAF
    )


def estimate_tokens(text: Any) -> int:
    """
    Estimate token count for mixed Chinese/English text.

    Heuristic:
    - CJK chars:   ~1 token each
    - ASCII chars: ~1 token / 4 chars
    - Other chars: ~1 token / 2 chars
    """
    raw = str(text or "").strip()
    if not raw:
        return 0

    cjk_count = 0
    ascii_count = 0
    other_count = 0

    for ch in raw:
        if ch.isspace():
            continue
        if _is_cjk(ch):
            cjk_count += 1
        elif ord(ch) < 128:
            ascii_count += 1
        else:
            other_count += 1

    estimate = cjk_count + math.ceil(ascii_count / 4.0) + math.ceil(other_count / 2.0)
    return max(1, int(estimate))
