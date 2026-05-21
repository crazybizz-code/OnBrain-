#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""mcp_server.py uchun to'liq test suite"""
import subprocess, json, sys

PY  = r".\venv\Scripts\python.exe"
SRV = "mcp_server.py"

TESTS = [
    # 1. Fuzzy typo: 'yusupv' → 'Yusupov Jasur'
    {"id": 1, "method": "analyze_excel",
     "params": {"file_path": "tests/sample.csv", "user_query": "yusupv"}},
    # 2. Fuzzy: 'nilufar' (to'liq ism)
    {"id": 2, "method": "analyze_excel",
     "params": {"file_path": "tests/sample.csv", "user_query": "nilufar"}},
    # 3. Aggregate: maksimal ball
    {"id": 3, "method": "analyze_excel",
     "params": {"file_path": "tests/sample.csv", "user_query": "maksimal ball"}},
    # 4. Aggregate: o'rtacha
    {"id": 4, "method": "analyze_excel",
     "params": {"file_path": "tests/sample.csv", "user_query": "ortacha foiz"}},
    # 5. Aggregate: jami
    {"id": 5, "method": "analyze_excel",
     "params": {"file_path": "tests/sample.csv", "user_query": "jami ball"}},
    # 6. Bo'sh query → meta info
    {"id": 6, "method": "analyze_excel",
     "params": {"file_path": "tests/sample.csv", "user_query": ""}},
    # 7. cache_clear
    {"id": 7, "method": "cache_clear", "params": {}},
    # 8. Noma'lum metod
    {"id": 8, "method": "unknown_tool", "params": {}},
    # 9. Buzilgan JSON → keyingi xabar ishlashi kerak
    # (alohida subprocess bilan tekshiriladi)
    # 10. Google Sheets / direct CSV URL
    {"id": 9, "method": "analyze_sheets",
     "params": {"sheet_url": "https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv",
                "user_query": "Stephen"}},
]

payload = "\n".join(json.dumps(t, ensure_ascii=False) for t in TESTS) + "\nquit\n"
r = subprocess.run([PY, SRV], input=payload, capture_output=True,
                   text=True, encoding="utf-8")

print("=" * 60)
print("TEST NATIJALARI")
print("=" * 60)
for i, line in enumerate(r.stdout.strip().splitlines(), 1):
    try:
        d = json.loads(line)
        mid = d.get("id")
        if "result" in d:
            val = str(d["result"])[:110]
            status = "OK"
        else:
            val = str(d.get("error", "?"))[:110]
            status = "ERR"
        print(f"  [{i:02d}] id={mid}  [{status}]  {val}")
    except Exception:
        print(f"  [{i:02d}] RAW: {line[:120]}")

if r.stderr.strip():
    print("\nSTDERR (birinchi 400 belgi):")
    print(r.stderr[:400])

print("=" * 60)
print(f"Jami chiqarish satrlari: {len(r.stdout.strip().splitlines())}")
