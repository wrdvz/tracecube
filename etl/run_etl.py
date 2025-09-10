import os, io, pathlib, hashlib, json, datetime as dt, requests, pandas as pd
from arelle import Cntlr, ModelManager
from arelle.ModelXbrl import ModelXbrl

ROOT = pathlib.Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"; OUT = ROOT / "data" / "out"
RAW.mkdir(parents=True, exist_ok=True); OUT.mkdir(parents=True, exist_ok=True)

# Démo : tags finance (ESEF) + tags climat (ESRS) déjà prévus
FACT_LOCALNAMES = [
    "Revenue", "OperatingProfitLoss",
    "GreenhouseGasScope1Emissions",
    "GreenhouseGasScope2EmissionsLocationBased",
    "GreenhouseGasScope2EmissionsMarketBased",
]

def download(url: str) -> pathlib.Path:
    fn = url.split("/")[-1] or f"file_{hashlib.sha1(url.encode()).hexdigest()}.xbrl"
    p = RAW / fn
    if not p.exists():
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        p.write_bytes(r.content)
    return p

def load_xbrl(path: str) -> ModelXbrl:
    ctrl = Cntlr.Cntlr(logFileName=None)
    mm = ModelManager.initialize(ctrl)
    return mm.load(path)

def extract_facts(x: ModelXbrl, wanted_locals):
    rows = []
    for f in x.facts:
        c = getattr(f, "concept", None)
        if not c: 
            continue
        ln = c.qname.localName
        if ln not in wanted_locals:
            continue
        ctx = f.context
        ent = ctx.entityIdentifier[1] if ctx else None
        end = getattr(ctx, "endDatetime", None)
        start = getattr(ctx, "startDatetime", None)
        unit = None
        if f.unit and f.unit.measures:
            unit = ":".join(".".join(u) for u in f.unit.measures if u)
        rows.append({
            "concept_local": ln,
            "entity_lei": ent,
            "period_start": start.isoformat() if start else None,
            "period_end": end.isoformat() if end else None,
            "value": f.value,
            "unit": unit,
            "decimals": f.decimals,
            "source_doc": x.modelDocument.uri,
        })
    return rows

def main():
    urls = [u.strip() for u in (ROOT/"etl"/"sources_urls.txt").read_text().splitlines() if u.strip() and not u.startswith("#")]
    all_rows, downloaded = [], []
    for u in urls:
        try:
            p = download(u)
            downloaded.append((u, p.name))
            x = load_xbrl(str(p))
            all_rows.extend(extract_facts(x, FACT_LOCALNAMES))
            x.close()
        except Exception as e:
            all_rows.append({"concept_local":"__ERROR__", "source_doc":u, "value":str(e)})

    df = pd.DataFrame(all_rows)
    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    excel_name, csv_name, pq_name = "CarbonTrace_latest.xlsx", "facts_latest.csv", "facts_latest.parquet"

    if not df.empty:
        df.to_csv(OUT/csv_name, index=False)
        df.to_parquet(OUT/pq_name, index=False)
        with pd.ExcelWriter(OUT/excel_name) as w:
            df.to_excel(w, index=False, sheet_name="Facts")
            pd.DataFrame(downloaded, columns=["url","saved_as"]).to_excel(w, index=False, sheet_name="Sources")

    manifest = {
        "version": ts,
        "generated_at_utc": dt.datetime.utcnow().isoformat()+"Z",
        "rows": int(len(df)),
        "columns": list(df.columns) if not df.empty else [],
        "files": {"excel": excel_name, "csv": csv_name, "parquet": pq_name},
        "notes": "Finance (Revenue/OPL) today; ESRS Scope1/2 tags ready when available."
    }
    (OUT/"manifest.json").write_text(json.dumps(manifest, indent=2))

if __name__ == "__main__":
    main()
