#!/usr/bin/env python3
# etl_profiles_from_registry.py
#
# Flat, config-first ETL:
#   1) Extract ODS rows (Postgres -> Postgres)
#   2) Derive CIA+S signals (worst per app)
#   3) Apply YAML registry (derived_from + rule)
#   4) Upsert profiles + fields (profile_id deterministic; profile_field.id deterministic)
#
import os
import sys
import yaml
import hashlib
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, DictCursor, Json
from datetime import datetime, timezone

# ---- Config (env) ----
ODS_DSN       = os.getenv("ODS_DSN", "postgresql://postgres:postgres@192.168.1.188:5432/lct_data")
TGT_DSN       = os.getenv("TGT_DSN", "postgresql://postgres:postgres@192.168.1.188:5432/lct_data")
REGISTRY_PATH = os.getenv("REGISTRY_YAML", "fields.v1.yaml")

SCOPE_TYPE = "application"   # matches DDL: profile.scope_type TEXT
VERSION    = 1               # matches DDL: profile.version INT
SRC_SYS    = "ODS"           # provenance

SQL = """
SELECT
  lca.lean_control_service_id,
  lpbd.jira_backlog_id,
  si.it_service_instance,
  so.service_offering_join,
  so.app_criticality_assessment AS app_criticality,
  so.security_rating            AS security_rating,
  so.integrity_rating           AS integrity_rating,
  so.availability_rating        AS availability_rating,
  so.resiliency_category        AS resilience_rating,
  child_app.correlation_id      AS app_correlation_id
FROM public.vwsfitbusinessservice bs
JOIN public.lean_control_application lca
  ON lca.servicenow_app_id = bs.service_correlation_id
JOIN public.vwsfitserviceinstance si
  ON bs.it_business_service_sysid = si.it_business_service_sysid
JOIN public.lean_control_product_backlog_details lpbd
  ON lpbd.lct_product_id = lca.lean_control_service_id AND lpbd.is_parent = TRUE
JOIN public.vwsfbusinessapplication child_app
  ON si.business_application_sysid = child_app.business_application_sys_id
JOIN public.spdw_vwsfservice_offering so
  ON so.service_offering_join = si.service_offering_join
ORDER BY si.it_service_instance;
"""

def load_registry(path: str) -> list[dict]:
    with open(path, "r") as f:
        y = yaml.safe_load(f)
    flat: list[dict] = []
    fields = (y or {}).get("fields", {})
    for domain, items in (fields or {}).items():
        for it in (items or []):
            d = dict(it)
            d["domain"] = domain
            flat.append(d)
    return flat

def extract_df() -> pd.DataFrame:
    # pandas will warn about non-SQLAlchemy connection; safe to ignore
    with psycopg2.connect(ODS_DSN) as conn:
        return pd.read_sql_query(SQL, conn)

def worst_letter(series: pd.Series, order: list[str]) -> pd.Series:
    dtype = pd.CategoricalDtype(categories=order, ordered=True)
    s = series.astype("string").str.strip().str.upper().astype(dtype)
    return s.groupby(level=0).min()

def confidentiality_from_security(sec: pd.Series) -> pd.Series:
    s = sec.astype("string").str.strip().str.upper().replace({"A": "A1"})
    return s.map({"A1": "High", "A2": "High", "B": "Medium", "C": "Low", "D": "Low"})

def aggregate_signals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.set_index("app_correlation_id", drop=True)

    abcd   = ["A", "B", "C", "D"]
    a12bcd = ["A1", "A2", "B", "C", "D"]

    crit  = worst_letter(df["app_criticality"], abcd)
    integ = worst_letter(df["integrity_rating"], abcd)
    avail = worst_letter(df["availability_rating"], abcd)
    resil = worst_letter(df["resilience_rating"], abcd)

    sec = (
        df["security_rating"]
        .astype("string").str.strip().str.upper().replace({"A": "A1"})
        .astype(pd.CategoricalDtype(categories=a12bcd, ordered=True))
        .groupby(level=0).min().astype(str)
    )

    app_ids = crit.index
    agg = pd.DataFrame({
        "app_correlation_id": app_ids,
        "app.criticality": crit.astype(str).values,
        "integrity":        integ.astype(str).values,
        "availability":     avail.astype(str).values,
        "resilience":       resil.astype(str).values,
        "security_rating":  sec.values,
    })

    agg["confidentiality"] = confidentiality_from_security(agg["security_rating"])

    def first_non_null(s: pd.Series):
        s = s.dropna()
        return s.iloc[0] if not s.empty else None

    def mode_or_first(s: pd.Series):
        s = s.dropna()
        if s.empty:
            return None
        m = s.mode()
        return m.iloc[0] if not m.empty else s.iloc[0]

    by_app = df.groupby(level=0)
    agg["lean_control_service_id"] = by_app["lean_control_service_id"].apply(first_non_null).values
    agg["jira_backlog_id"]         = by_app["jira_backlog_id"].apply(first_non_null).values
    agg["service_offering_join"]   = by_app["service_offering_join"].apply(mode_or_first).values

    return agg

def apply_registry(agg: pd.DataFrame, registry: list[dict]) -> pd.DataFrame:
    rows: list[tuple[str, str, object]] = []
    for _, app in agg.iterrows():
        app_id = str(app["app_correlation_id"])
        for f in registry:
            src = f.get("derived_from"); rule = f.get("rule")
            if not src or not rule:
                continue
            src_val = str(app.get(src, "")).strip()
            if not src_val:
                continue
            mapped = rule.get(src_val)
            if mapped is None:
                continue
            rows.append((app_id, f["key"], mapped))
    return pd.DataFrame(rows, columns=["entity_id", "key", "value"])

def _profile_pk(scope_type: str, scope_id: str, version: int) -> str:
    h = hashlib.md5(f"{scope_type}:{scope_id}:{version}".encode("utf-8")).hexdigest()
    return f"prof_{h}"

def upsert_to_target(derived_kv: pd.DataFrame, agg: pd.DataFrame):
    if agg is None or agg.empty:
        print("No profiles to upsert.")
        return

    now_utc = datetime.now(timezone.utc)

    # Anchors (deterministic profile_id)
    profile_rows = []
    for app_id in agg["app_correlation_id"].astype(str):
        pid = _profile_pk(SCOPE_TYPE, app_id, VERSION)
        profile_rows.append((pid, SCOPE_TYPE, app_id, VERSION))

    # Base fields to persist
    base_fields = [
        "app.criticality", "integrity", "availability", "resilience",
        "security_rating", "confidentiality",
        "lean_control_service_id", "jira_backlog_id", "service_offering_join",
    ]

    base_kv = []
    for _, row in agg.iterrows():
        eid = str(row.app_correlation_id)
        src_ref = None if pd.isna(row.get("jira_backlog_id")) else str(row.get("jira_backlog_id"))
        for k in base_fields:
            v = row.get(k)
            if pd.isna(v):
                continue
            base_kv.append((eid, k, Json(v), SRC_SYS, src_ref, now_utc))

    derived_rows = []
    if derived_kv is not None and not derived_kv.empty:
        for _, r in derived_kv.iterrows():
            if pd.isna(r.value):
                continue
            derived_rows.append((str(r.entity_id), str(r.key), Json(r.value), SRC_SYS, None, now_utc))

    all_rows = base_kv + derived_rows

    with psycopg2.connect(TGT_DSN) as tgt, tgt.cursor(cursor_factory=DictCursor) as cur:
        # Upsert anchors
        execute_values(cur, """
            INSERT INTO profile (profile_id, scope_type, scope_id, version)
            VALUES %s
            ON CONFLICT (scope_type, scope_id, version)
            DO UPDATE SET updated_at = now()
        """, profile_rows)

        if all_rows:
            # Stage fields (entity_id text; value jsonb)
            cur.execute("""
                CREATE TEMP TABLE tmp_profile_field (
                  entity_id     text,
                  key           text,
                  value         jsonb,
                  source_system text,
                  source_ref    text,
                  collected_at  timestamptz
                ) ON COMMIT DROP;
            """)
            execute_values(cur, """
                INSERT INTO tmp_profile_field (entity_id, key, value, source_system, source_ref, collected_at)
                VALUES %s
            """, all_rows)

            # Insert/Upsert into profile_field with a deterministic id
            # id = 'pf_' || md5(profile_id || ':' || key)
            cur.execute("""
                INSERT INTO profile_field
                  (id, profile_id, key, value, source_system, source_ref, collected_at, updated_at)
                SELECT
                  'pf_' || md5(p.profile_id || ':' || t.key) AS id,
                  p.profile_id,
                  t.key,
                  t.value,
                  t.source_system,
                  t.source_ref,
                  t.collected_at,
                  now()
                FROM tmp_profile_field t
                JOIN profile p
                  ON p.scope_type = %s AND p.version = %s AND p.scope_id = t.entity_id
                ON CONFLICT (profile_id, key)
                DO UPDATE SET
                  value         = EXCLUDED.value,
                  source_system = EXCLUDED.source_system,
                  source_ref    = EXCLUDED.source_ref,
                  collected_at  = EXCLUDED.collected_at,
                  updated_at    = now();
            """, (SCOPE_TYPE, VERSION))

        tgt.commit()

def main():
    print("Loading registry…")
    registry = load_registry(REGISTRY_PATH)

    print("Extracting rows…")
    df = extract_df()
    if df.empty:
        print("No source rows. Nothing to do.")
        return

    print("Aggregating CIA+S signals…")
    agg = aggregate_signals(df)

    print("Applying registry rules…")
    derived_kv = apply_registry(agg, registry)

    print("Writing to target…")
    upsert_to_target(derived_kv, agg)

    print(f"Done. Profiles: {len(agg)}; derived fields written: {0 if derived_kv is None else len(derived_kv)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
