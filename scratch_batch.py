import sys, fitz, urllib.request
SCRATCH=r"C:/Users/pglyn/AppData/Local/Temp/claude/c--Users-pglyn-PycharmProjects-dail-extractor/bf070e14-979c-4da0-a150-059c727bc8bd/scratchpad"
pairs=[a.split("::") for a in sys.argv[1:]]
for name,url in pairs:
    try:
        req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36","Accept":"application/pdf,*/*"})
        data=urllib.request.urlopen(req,timeout=90).read()
        doc=fitz.open(stream=data,filetype="pdf")
        txt="\n".join(p.get_text() for p in doc)
        open(f"{SCRATCH}/{name}.txt","w",encoding="utf-8").write(txt)
        print(f"OK {name} chars={len(txt)}")
    except Exception as e:
        print(f"ERR {name} {url} :: {repr(e)}")
