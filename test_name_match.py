"""
Standalone unit tests for name matching logic.
_classify_name_type and _search_person kodi bu yerga ko'chirilgan —
bot.py ga bog'liqlik yo'q, tez ishlaydi.

Run: venv\Scripts\python.exe -m pytest test_name_match.py -v
"""

import re

# ═══════════════════════════════════════════════════════════════════════════════
# bot.py dan ko'chirilgan funksiyalar (sinxronlashtiriladi agar o'zgarsa)
# ═══════════════════════════════════════════════════════════════════════════════

_SUFFIX_STRIP = re.compile(
    r"(ning|ga|da|dan|ni|lar|larning|larga|lardan|larni|"
    r"ning|ni|nda|ndan|niki|dagi|dан|нинг|ни|га|да|дан)$",
    re.IGNORECASE
)

def _strip_suffix(word: str) -> str:
    return _SUFFIX_STRIP.sub("", word.strip().lower()).strip()


def _classify_name_type(word: str) -> str:
    w = word.strip().lower()
    fam_suffixes = (
        "ov", "ev", "yev",
        "ova", "eva", "yeva",
        "off", "eff",
        "in", "ina",
        "skiy", "sky",
        "zadeh", "zoda",
        "bekov", "bekova",
        "jonov", "jonova",
        "xonov", "xonova",
        "qolov", "qolova",
        "boyev", "boyeva",
        "qulov", "qulova",
        "ulov", "ulova",
        "aliev", "alieva",
    )
    for suf in fam_suffixes:
        if w.endswith(suf) and len(w) > len(suf) + 1:
            return "familiya"
    ota_markers = ("o'g'li", "o'g'lining", "qizi", "qizining", "ugli", "ugil", "ogli")
    for m in ota_markers:
        if w.endswith(m):
            return "ota"
    return "ism"


def _to_num(val):
    try:
        s = str(val).replace(",", "").replace(" ", "").strip()
        return float(s) if s else None
    except Exception:
        return None


def _search_person(data_rows: list, header: list, name_q: str) -> list:
    raw = name_q.strip().lower()
    stripped = _strip_suffix(raw)
    candidates = list({raw, stripped})
    name_type = _classify_name_type(stripped)

    name_col_indices = []
    for j, h in enumerate(header):
        hl = str(h).strip().lower()
        if any(w in hl for w in ["f.i.o", "fio", "ism", "name", "ф.и.о", "фио", "имя", "familiya", "fish"]):
            name_col_indices.append(j)
    search_all = len(name_col_indices) == 0

    results = []
    for i, row in enumerate(data_rows):
        cols_to_check = range(len(row)) if search_all else name_col_indices
        matched_j = None
        matched_cell = None
        match_quality = 0
        for j in cols_to_check:
            if j >= len(row):
                continue
            cs = str(row[j]).strip().lower()
            if not cs or len(cs) < 2:
                continue
            for c in candidates:
                if len(c) < 3:
                    continue
                if c == cs:
                    matched_j = j; matched_cell = str(row[j]).strip(); match_quality = 3; break
                words_in_cell = cs.split()
                if cs.startswith(c) and len(cs) > len(c) and cs[len(c)] == " ":
                    if match_quality < 2:
                        matched_j = j; matched_cell = str(row[j]).strip(); match_quality = 2
                if c in words_in_cell and match_quality < 1:
                    pos = words_in_cell.index(c)
                    is_name_col = j in name_col_indices
                    matched = False
                    if is_name_col:
                        if name_type == "familiya" and pos == 0:   matched = True
                        elif name_type == "ism"      and pos == 1:  matched = True
                        elif name_type == "ota"      and pos >= 2:  matched = True
                    else:
                        matched = True
                    if matched:
                        matched_j = j; matched_cell = str(row[j]).strip(); match_quality = 1
            if match_quality == 3:
                break
        if matched_j is not None:
            col = str(header[matched_j]).strip() if matched_j < len(header) else f"Col{matched_j}"
            results.append({
                "row_index": i, "row": row,
                "matched_cell": matched_cell, "matched_col": col,
                "match_quality": match_quality,
            })
    best_quality = max((r["match_quality"] for r in results), default=0)
    if best_quality >= 2:
        results = [r for r in results if r["match_quality"] >= best_quality]
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Test data
# ═══════════════════════════════════════════════════════════════════════════════

HEADER = ["FIO", "Matematika", "Fizika", "Ingliz", "Jami"]
ROWS = [
    ["Karimov Muhammad Salim",    85, 90, 78, 253],
    ["Yusupov Jasur Murod",       70, 65, 80, 215],
    ["Rahimova Zulfiya Hamid",    95, 88, 92, 275],
    ["Karimov Bobur Nodir",       60, 70, 75, 205],
    ["Hasanov Muhammad Jamshid",  88, 77, 82, 247],
]

# ═══════════════════════════════════════════════════════════════════════════════
# Tests: _classify_name_type
# ═══════════════════════════════════════════════════════════════════════════════

def test_classify_familiya_ov():
    assert _classify_name_type("Karimov") == "familiya"

def test_classify_familiya_eva():
    assert _classify_name_type("Karimova") == "familiya"

def test_classify_familiya_yev():
    assert _classify_name_type("Aliyev") == "familiya"

def test_classify_familiya_zoda():
    assert _classify_name_type("Husanzoda") == "familiya"

def test_classify_familiya_jonov():
    assert _classify_name_type("Hasanjonov") == "familiya"

def test_classify_ism_regular():
    assert _classify_name_type("Muhammad") == "ism"

def test_classify_ism_jasur():
    assert _classify_name_type("Jasur") == "ism"

def test_classify_ism_abdulloh():
    assert _classify_name_type("Abdulloh") == "ism"

def test_classify_ota_ogli():
    assert _classify_name_type("Salimjonugli") == "ota"

def test_classify_ota_qizi():
    assert _classify_name_type("Zuhraqizi") == "ota"

def test_classify_short_ali():
    assert _classify_name_type("Ali") == "ism"

# ═══════════════════════════════════════════════════════════════════════════════
# Tests: _search_person — familiya
# ═══════════════════════════════════════════════════════════════════════════════

def test_familiya_karimov_two_results():
    """Karimov — 2 ta natija (familiya collision)"""
    results = _search_person(ROWS, HEADER, "Karimov")
    assert len(results) == 2
    cells = [r["matched_cell"] for r in results]
    assert any("Karimov Muhammad" in c for c in cells)
    assert any("Karimov Bobur" in c for c in cells)

def test_familiya_yusupov_unique():
    """Yusupov — 1 ta"""
    results = _search_person(ROWS, HEADER, "Yusupov")
    assert len(results) == 1
    assert "Yusupov" in results[0]["matched_cell"]

def test_familiya_rahimova():
    """Rahimova — 1 ta"""
    results = _search_person(ROWS, HEADER, "Rahimova")
    assert len(results) == 1
    assert "Rahimova" in results[0]["matched_cell"]

def test_familiya_suffix_stripped():
    """Karimovning → Karimov sifatida qidiradi"""
    results = _search_person(ROWS, HEADER, "Karimovning")
    assert len(results) == 2

# ═══════════════════════════════════════════════════════════════════════════════
# Tests: _search_person — ism
# ═══════════════════════════════════════════════════════════════════════════════

def test_ism_muhammad_two_results():
    """Muhammad — 2 ta (ism collision)"""
    results = _search_person(ROWS, HEADER, "Muhammad")
    assert len(results) == 2

def test_ism_jasur_unique():
    """Jasur — 1 ta"""
    results = _search_person(ROWS, HEADER, "Jasur")
    assert len(results) == 1
    assert "Yusupov Jasur" in results[0]["matched_cell"]

def test_ism_zulfiya():
    """Zulfiya — 1 ta"""
    results = _search_person(ROWS, HEADER, "Zulfiya")
    assert len(results) == 1
    assert "Rahimova Zulfiya" in results[0]["matched_cell"]

# ═══════════════════════════════════════════════════════════════════════════════
# Tests: _search_person — sifat (match_quality)
# ═══════════════════════════════════════════════════════════════════════════════

def test_exact_fullname_quality_3():
    """To'liq ism exact match → quality 3"""
    results = _search_person(ROWS, HEADER, "Karimov Muhammad Salim")
    assert len(results) >= 1
    assert results[0]["match_quality"] == 3

def test_startswith_quality_2():
    """Familiya + ism startswith → quality 2"""
    results = _search_person(ROWS, HEADER, "Yusupov Jasur")
    assert len(results) == 1
    assert results[0]["match_quality"] >= 2

def test_no_match_returns_empty():
    """Mavjud bo'lmagan ism → bo'sh"""
    results = _search_person(ROWS, HEADER, "Petrov")
    assert results == []

def test_short_word_no_match():
    """2 ta harfli so'z → qidirilmasin"""
    results = _search_person(ROWS, HEADER, "Jo")
    assert results == []

# ═══════════════════════════════════════════════════════════════════════════════
# Tests: _search_person — FIO ustunisiz (search_all)
# ═══════════════════════════════════════════════════════════════════════════════

def test_no_name_col_finds_word():
    """FIO ustunisiz — barcha ustunlardan qidiradi"""
    header = ["A", "B", "C"]
    rows = [["Karimov", 90, 100], ["Yusupov", 80, 95]]
    results = _search_person(rows, header, "Karimov")
    assert len(results) == 1

def test_product_list_search():
    """Mahsulot jadvali — 'kamera' topilishi kerak"""
    header = ["Nomi", "Narxi"]
    rows = [["kamera", 300000], ["chiroq", 300000]]
    results = _search_person(rows, header, "kamera")
    assert len(results) == 1
    assert results[0]["matched_cell"] == "kamera"

# ═══════════════════════════════════════════════════════════════════════════════
# Tests: ota-ism
# ═══════════════════════════════════════════════════════════════════════════════

def test_ota_ogli_row():
    """o'g'li bilan tugaydigan so'z pos 2 da topiladi"""
    rows = [["Zokirov Bobur Nosirogli", 80, 75, 90, 245]]
    results = _search_person(rows, HEADER, "Nosirogli")
    assert len(results) == 1
    assert "Nosirogli" in results[0]["matched_cell"]

def test_ota_not_confused_with_ism():
    """Otaism so'z ism sifatida qidirilsa topilmasligi kerak (pos 2, ism pos 1 kutadi)"""
    rows = [["Zokirov Bobur Nosir", 80, 75, 90, 245]]
    # "Nosir" — ism sifatida qidirilsa pos 1 da bo'lishi kerak, lekin u pos 2 da
    results = _search_person(rows, HEADER, "Nosir")
    # Pos 2 da bo'lgani uchun ism tipida mos kelmaydi → bo'sh
    assert results == []
