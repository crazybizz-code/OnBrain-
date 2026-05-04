import asyncio, sys
sys.path.insert(0, r'c:\Users\Lenovo\OneDrive\OnBrain AI')
from bot import fetch_sheet_public

async def main():
    rows = await fetch_sheet_public(
        'https://docs.google.com/spreadsheets/d/1uwN8lowZaA5uCNFDetWMm8dBqo9q-TF__m6gMbFsT90/edit'
    )
    print("Keys:", list(rows.keys()))
    print("Total sheets:", len(rows))
    for sheet_name, sheet_rows in rows.items():
        print(f'=== SHEET: {sheet_name} ===')
        rlist = list(sheet_rows) if hasattr(sheet_rows, '__iter__') else [sheet_rows]
        print(f'  row count: {len(rlist)}')
        for r in rlist[:8]:
            print(' ', r)

asyncio.run(main())
