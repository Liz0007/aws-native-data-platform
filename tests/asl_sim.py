"""Tiny interpreter for the ASL subset used by the close-batch state machine.
Task states are mocked; everything else (Choice, Pass, Wait, Fail, counters,
ResultPath, Catch) runs from the rendered definition."""
import json, re, sys, copy

def get(doc, path):
    cur = doc
    for part in path[2:].split("."):
        cur = cur[part]
    return cur

def has(doc, path):
    try: get(doc, path); return True
    except (KeyError, TypeError): return False

def setpath(doc, path, value):
    if path is None: return doc
    if path == "$": return value
    keys = path[2:].split("."); cur = doc
    for k in keys[:-1]: cur = cur.setdefault(k, {})
    cur[keys[-1]] = value
    return doc

def intrinsic(expr, doc):
    m = re.fullmatch(r"States\.MathAdd\((\$[\w.]+), (\d+)\)", expr)
    assert m, f"unsupported intrinsic {expr}"
    return get(doc, m.group(1)) + int(m.group(2))

def resolve(params, doc):
    out = {}
    for k, v in params.items():
        if isinstance(v, dict): out[k] = resolve(v, doc)
        elif k.endswith(".$"):
            out[k[:-2]] = intrinsic(v, doc) if v.startswith("States.") else get(doc, v)
        else: out[k] = v
    return out

def cmp(rule, doc):
    if "And" in rule: return all(cmp(r, doc) for r in rule["And"])
    if "Not" in rule: return not cmp(rule["Not"], doc)
    if "IsPresent" in rule: return has(doc, rule["Variable"]) == rule["IsPresent"]
    v = get(doc, rule["Variable"])
    if "NumericEquals" in rule: return v == rule["NumericEquals"]
    if "NumericGreaterThanEquals" in rule: return v >= rule["NumericGreaterThanEquals"]
    if "NumericGreaterThanPath" in rule: return v > get(doc, rule["NumericGreaterThanPath"])
    if "StringEquals" in rule: return v == rule["StringEquals"]
    raise AssertionError(rule)

def run(asl, env, doc):
    """env: dict of callables for task resources. Returns (outcome, trace)."""
    S, name, trace, steps = asl["States"], asl["StartAt"], [], 0
    while True:
        steps += 1; assert steps < 500, "runaway loop"
        st = S[name]; trace.append(name); t = st["Type"]
        if t == "Fail": return ("FAIL:" + st["Error"], trace)
        if t == "Choice":
            nxt = next((c["Next"] for c in st["Choices"] if cmp(c, doc)), st.get("Default"))
            name = nxt; continue
        if t == "Wait": pass
        elif t == "Pass":
            if "Parameters" in st: doc = resolve(st["Parameters"], doc)
            elif "Result" in st: doc = setpath(doc, st.get("ResultPath", "$"), copy.deepcopy(st["Result"]))
        elif t == "Task":
            try:
                res = env[st["Resource"].rsplit(":", 2)[-2] + ":" + st["Resource"].rsplit(":", 1)[-1]](st["Parameters"])
                sel = st.get("ResultSelector")
                if sel: res = {k[:-2]: get(res, v) for k, v in sel.items()}
                if st.get("ResultPath", "$") is not None or "ResultPath" not in st:
                    doc = setpath(doc, st.get("ResultPath", "$"), res)
            except Exception as exc:
                code = getattr(exc, "code", None)
                hit = next((c for c in st.get("Catch", []) if code in c["ErrorEquals"]), None)
                if not hit: raise
                name = hit["Next"]; continue
        if st.get("End"): return ("SUCCEEDED", trace)
        name = st["Next"]
