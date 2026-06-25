import fitz, os, re, sys

D = r'c:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/council_minutes/tipperary'

SUBITEM = re.compile(r'^(\d{1,2})\.(\d{1,2})$')
NOISE = re.compile(r'^(For Approval|For Information|For Noting|For Adoption|Powered by TCPDF.*)\b', re.I)

def fix(s):
    # repair common cp-mojibake of Irish fadas / apostrophes from this PDF set
    reps = {
        'Mair�n':'Mairín','Cruinni�':'Cruinniú','M�os�il':'Míosúil','l�thair':'láthair',
        '�rann':'Árann','�':"'",
    }
    for k,v in reps.items():
        s = s.replace(k,v)
    s = s.replace('’',"'")
    return re.sub(r'\s+',' ',s).strip()

def parse_sections(fn):
    """Top-level agenda topic = the heading line immediately before each 'X.1' marker."""
    doc = fitz.open(os.path.join(D, fn))
    lines = []
    for p in doc:
        for ln in p.get_text().split('\n'):
            ln = ln.strip()
            if ln:
                lines.append(ln)
    sections = []
    seen = set()
    for i, ln in enumerate(lines):
        m = SUBITEM.match(ln)
        if m and m.group(2) == '1':  # first sub-item of a section
            # heading is the nearest preceding non-noise, non-marker line
            j = i - 1
            while j >= 0 and (NOISE.match(lines[j]) or SUBITEM.match(lines[j]) or lines[j].lower()=='agenda'):
                j -= 1
            if j >= 0:
                head = fix(lines[j])
                key = (m.group(1), head)
                if head and head not in seen and len(head) > 3:
                    seen.add(head)
                    sections.append(head)
    return sections

if __name__ == '__main__':
    fn = sys.argv[1]
    secs = parse_sections(fn)
    for s in secs:
        print('-', s)
    print('COUNT', len(secs))
