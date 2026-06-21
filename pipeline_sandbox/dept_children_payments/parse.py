import fitz, glob, re

REF  = re.compile(r'^(\d{4,7})\s+(.+\S)\s*$')
DATE = re.compile(r'^(\d{2})/(\d{2})/(\d{4})(?:\s+(.*))?$')
AMT  = re.compile(r'^\D{0,3}([\d,]+\.\d{2})\s*(.*)$')
HEADER = {'reference','name','supplier name','amount','total paid','description','payment','payment date','date'}

def parse_file(path):
    """Order-agnostic: after a 'ref supplier' line, classify each following line as date /
    amount+desc / desc-continuation regardless of column order (two layouts exist: date-middle
    and date-last). Flush on the next ref."""
    recs, cur = [], None
    def flush():
        nonlocal cur
        if cur and cur['amount'] is not None and cur['date']:
            recs.append(cur)
        cur = None
    for page in fitz.open(path):
        for raw in page.get_text().splitlines():
            line = raw.strip()
            if not line or line.lower() in HEADER:
                continue
            m = REF.match(line)
            if m:
                flush(); cur = {'ref': m.group(1), 'supplier': m.group(2).strip(), 'date': None, 'amount': None, 'desc': ''}
                continue
            if cur is None:
                continue
            dm = DATE.match(line)
            if dm and cur['date'] is None:
                cur['date'] = f'{dm.group(3)}-{dm.group(2)}-{dm.group(1)}'
                if dm.group(4):                                   # date+amount+desc on one line
                    am = AMT.match(dm.group(4).strip())
                    if am and cur['amount'] is None:
                        cur['amount'] = float(am.group(1).replace(',','')); cur['desc'] = am.group(2).strip()
                continue
            am = AMT.match(line)
            if am and cur['amount'] is None:
                cur['amount'] = float(am.group(1).replace(',','')); cur['desc'] = am.group(2).strip()
                continue
            if cur['amount'] is not None:                          # description continuation
                cur['desc'] = (cur['desc'] + ' ' + line).strip()
    flush()
    return recs

if __name__ == '__main__':
    allr, total_ref = [], 0
    for f in sorted(glob.glob('data/bronze/pdfs/public_body_procurement/dept_children/*.pdf')):
        recs = parse_file(f); allr += recs
        for page in fitz.open(f):
            for raw in page.get_text().splitlines():
                l = raw.strip()
                if l and l.lower() not in HEADER and REF.match(l): total_ref += 1
    amts = [r['amount'] for r in allr]; dts = sorted(r['date'] for r in allr)
    print(f'records: {len(allr)}  | REF-start lines: {total_ref}  | capture {100*len(allr)/total_ref:.1f}%')
    print(f'suppliers: {len(set(r["supplier"] for r in allr))} | EUR {min(amts):,.0f}-{max(amts):,.0f} | total EUR {sum(amts):,.0f}')
    print(f'dates: {dts[0]} -> {dts[-1]}')
    # future/invalid date check
    fut = [d for d in dts if d > '2026-06-21']
    print('future-dated:', len(fut), '| dup (ref+date+amount):', len(allr)-len({(r["ref"],r["date"],r["amount"]) for r in allr}))
