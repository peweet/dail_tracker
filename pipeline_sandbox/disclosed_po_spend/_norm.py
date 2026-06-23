import re
DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT",
        "INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND",
        "INTERNATIONAL"}
def norm(s):
    if s is None:
        return ""
    s = str(s).upper()
    # drop trailing T/A ...
    s = re.sub(r"\bT/A\b.*$", " ", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t and t not in DROP]
    return " ".join(toks)
