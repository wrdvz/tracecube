import pathlib
import hashlib
import json
import datetime as dt
import requests
import pandas as pd
import zipfile
import gzip, shutil

from arelle import Cntlr, ModelManager
from arelle.ModelXbrl import ModelXbrl

ROOT = pathlib.Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "out"
RAW.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

# Démo : tags finance (IFRS/ESEF) + tags climat (ESRS) prêts
FACT_LOCALNAMES = [
    # ventes / revenus
    "Revenue",
    "SalesRevenueNet",
    "RevenueFromContractsWithCustomersExcludingAssessedTax",

    # résultat
    "OperatingProfitLoss",
    "ProfitLoss",

    # climat (quand ESRS sera dispo)
    "GreenhouseGasScope1Emissions",
    "GreenhouseGasScope2EmissionsLocationBased",
    "GreenhouseGasScope2EmissionsMarketBased",
]

# On capte aussi des variantes IFRS usuelles + extensions contenant ces mots-clés
FACT_LOCALNAMES_EXTRA = {
    "Revenues", "SalesRevenueNet",
    "RevenueFromContractsWithCustomersExcludingAssessedTax",
    "ProfitLoss", "ProfitLossFromOperatingActivities",
    "OperatingIncomeLoss", "GrossProfit"
}
FACT_KEYWORDS = ("revenue", "revenu", "sales", "profit", "loss")


def download(url: str) -> pathlib.Path:
    """Télécharge en mode robuste (ZIP lourds ok)."""
    fn = url.split("/")[-1] or f"file_{hashlib.sha1(url.encode()).hexdigest()}.xbrl"
    path = RAW / fn
    if path.exists():
        return path
    with requests.get(
        url,
        timeout=120,
        allow_redirects=True,
        stream=True,
        headers={"User-Agent": "tracecube/0.1"},
    ) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return path

def path_to_instance(p: pathlib.Path) -> pathlib.Path:
    """Si p est un .zip, on extrait et on retourne le premier iXBRL trouvé.
    Supporte *.xhtml, *.html, et leurs variantes *.xhtml.gz / *.html.gz.
    """
    p = pathlib.Path(p)
    if p.suffix.lower() != ".zip":
        return p

    outdir = RAW / (p.stem + "_unzipped")
    outdir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(p, "r") as z:
        z.extractall(outdir)

    # 1) candidats non compressés (priorité iXBRL)
    plain = list(outdir.rglob("*.xhtml")) + list(outdir.rglob("*.html"))
    # 2) candidats .gz (souvent dans /reports/)
    gz = list(outdir.rglob("*.xhtml.gz")) + list(outdir.rglob("*.html.gz"))

    candidates: list[pathlib.Path] = []

    if plain:
        candidates = plain
    elif gz:
        # on décompresse le premier .gz trouvé à côté, et on l'utilise
        gz_path = sorted(gz, key=lambda x: (0 if "reports" in x.parts else 1, len(str(x))))[0]
        target = gz_path.with_suffix("")  # retire l'extension .gz -> .xhtml/.html
        if not target.exists():
            with gzip.open(gz_path, "rb") as fin, open(target, "wb") as fout:
                shutil.copyfileobj(fin, fout)
        candidates = [target]

    if not candidates:
        raise FileNotFoundError(f"Aucune instance iXBRL/XBRL (même .gz) dans {p.name}")

    # petit tri: souvent dans /reports/
    candidates.sort(key=lambda x: (0 if "reports" in x.parts else 1, len(str(x))))
    return candidates[0]

def load_xbrl(path: str) -> ModelXbrl:
    """Charge l'instance XBRL avec Arelle et log dans data/out/arelle.log."""
    log_path = OUT / "arelle.log"
    ctrl = Cntlr.Cntlr(logFileName=str(log_path))
    # important: online pour résoudre les taxos manquantes dans le runner
    ctrl.webCache.workOffline = False
    mm = ModelManager.initialize(ctrl)
    return mm.load(path)

def dump_sample_facts(x: ModelXbrl, limit: int = 200) -> None:
    """Écrit un échantillon des facts (concept qname/local, value…) pour debug."""
    rows = []
    for f in x.facts[:limit]:
        qn = getattr(f, "concept", None).qname if getattr(f, "concept", None) else None
        rows.append({
            "concept_qname": str(qn) if qn else None,
            "local": qn.localName if qn else None,
            "namespace": qn.namespaceURI if qn else None,
            "value": f.value,
            "decimals": getattr(f, "decimals", None),
            "is_nil": getattr(f, "isNil", False),
        })
    pd.DataFrame(rows).to_csv(OUT / "facts_sample.csv", index=False)

def _format_unit(fact) -> str | None:
    """Formate l'unité (ex: ISO4217.EUR)."""
    if not getattr(fact, "unit", None) or not fact.unit.measures:
        return None
    num = ["*".join(u) for u in (fact.unit.measures[0] or [])]
    den = ["*".join(u) for u in (fact.unit.measures[1] or [])]
    parts = []
    if num:
        parts.append(".".join(num))
    if den:
        parts.append("/" + ".".join(den))
    return "".join(parts) if parts else None


def extract_facts(x: ModelXbrl, wanted_locals: list[str]) -> list[dict]:
    rows: list[dict] = []
    for f in x.facts:
        c = getattr(f, "concept", None)
        if not c:
            continue
        local = c.qname.localName

        keep = (
            (local in wanted_locals) or
            (local in FACT_LOCALNAMES_EXTRA) or
            any(k in local.lower() for k in FACT_KEYWORDS)
        )
        if not keep:
            continue

        ctx = getattr(f, "context", None)
        ent = None
        if ctx:
            try:
                # certains filings exposent comme tuple (scheme, value)
                ent = ctx.entityIdentifier[1]
            except Exception:
                ent = getattr(ctx, "entityIdentifierValue", None)

        end = getattr(ctx, "endDatetime", None)
        start = getattr(ctx, "startDatetime", None)

        rows.append(
            {
                "concept_local": local,
                "entity_lei": ent,
                "period_start": start.isoformat() if start else None,
                "period_end": end.isoformat() if end else None,
                "value": f.value,
                "unit": _format_unit(f),
                "decimals": getattr(f, "decimals", None),
                "source_doc": x.modelDocument.uri,
            }
        )
    return rows


def main() -> None:
    urls_file = ROOT / "etl" / "sources_urls.txt"
    urls = [
        u.strip()
        for u in urls_file.read_text().splitlines()
        if u.strip() and not u.strip().startswith("#")
    ]

    all_rows: list[dict] = []
    downloaded: list[tuple[str, str]] = []

    for u in urls:
        try:
            p = download(u)
            inst = path_to_instance(p)
            downloaded.append((u, inst.name)) 
            x = load_xbrl(str(inst)) 
            dump_sample_facts(x, limit=300)
            print(f"[{inst.name}] facts: {len(getattr(x, 'facts', []))}")
            print(f"[unzip] instance: {inst}")
            all_rows.extend(extract_facts(x, FACT_LOCALNAMES))
            x.close()
        except Exception as e:
            all_rows.append(
                {"concept_local": "__ERROR__", "source_doc": u, "value": str(e)}
            )

    df = pd.DataFrame(all_rows)

    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    excel_name, csv_name, pq_name = (
        "CarbonTrace_latest.xlsx",
        "facts_latest.csv",
        "facts_latest.parquet",
    )

    # 💡 Écrire les fichiers même si df est vide (headers), pour qu’ils apparaissent dans R2
    (OUT / csv_name).write_text("" if df.empty else df.to_csv(index=False))

    if df.empty:
        # parquet/Excel ont besoin d’un df : on crée un DF vide avec colonnes attendues
        df = pd.DataFrame(
            columns=[
                "concept_local",
                "entity_lei",
                "period_start",
                "period_end",
                "value",
                "unit",
                "decimals",
                "source_doc",
            ]
        )

    # Parquet + Excel
    df.to_parquet(OUT / pq_name, index=False)
    with pd.ExcelWriter(OUT / excel_name) as w:
        df.to_excel(w, index=False, sheet_name="Facts")
        pd.DataFrame(downloaded, columns=["url", "saved_as"]).to_excel(
            w, index=False, sheet_name="Sources"
        )

    manifest = {
        "version": ts,
        "generated_at_utc": dt.datetime.utcnow().isoformat() + "Z",
        "rows": int(len(df)),
        "columns": list(df.columns),
        "files": {"excel": excel_name, "csv": csv_name, "parquet": pq_name},
        "notes": "Finance (Revenue/OperatingProfitLoss); ESRS Scope1/2 prêts dès disponibilité.",
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
