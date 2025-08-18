#!/usr/bin/env python3
"""
etl_profiles_from_registry_lite.py

Flow:
  1) Stage ODS rows into TEMP tmp_src via COPY
  2) Upsert APPLICATION from tmp_src
  3) Derive fine-grained metrics using YAML registry
  4) Upsert profile + profile_field (deterministic IDs)

Environment:
  ODS_DSN, TGT_DSN, REGISTRY_YAML, PROFILE_SCOPE, PROFILE_VERSION,
  SRC_SYS, DRY_RUN, APP_SCOPE_DEFAULT, APP_ONBOARDING_DEFAULT
"""

import os, sys, io, json, yaml, hashlib
from datetime import datetime, timezone
import psycopg2
from psycopg2 import extras
from psycopg2.extras import DictCursor, execute_values

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
ODS_DSN       = os.getenv("ODS_DSN", "postgresql://postgres:postgres@192.168.1.188:5432/lct_data")
TGT_DSN       = os.getenv("TGT_DSN", "postgresql://postgres:postgres@192.168.1.188:5432/lct_data")
REGISTRY_PATH = os.getenv("REGISTRY_YAML", "fields.v1.yaml")
SCOPE_TYPE      = os.getenv("PROFILE_SCOPE", "application")
PROFILE_VERSION = int(os.getenv("PROFILE_VERSION", "1"))
SRC_SYS         = os.getenv("SRC_SYS", "ODS")
DRY_RUN         = os.getenv("DRY_RUN", "0") == "1"
APP_SCOPE_DEFAULT      = os.getenv("APP_SCOPE_DEFAULT", "application")
APP_ONBOARDING_DEFAULT = os.getenv("APP_ONBOARDING_DEFAULT", "pending")

# --------------------------------------------------------------------------------------
# SQL source query
# --------------------------------------------------------------------------------------
SQL = """
SELECT DISTINCT
  lca.lean_control_service_id,
  lpbd.jira_backlog_id,
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
ORDER BY so.service_offering_join
"""

# --------------------------------------------------------------------------------------
# Helpers: IDs, normalization, registry
# --------------------------------------------------------------------------------------
def profile_pk(scope_type: str, scope_id: str, version: int) -> str:
    return "prof_" + hashlib.md5(f"{scope_type}:{scope_id}:{version}".encode()).hexdigest()

def field_pk(profile_id: str, key: str) -> str:
    return "pf_" + hashlib.md5(f"{profile_id}:{key}".encode()).hexdigest()

VALID_LETTERS    = {"A", "B", "C", "D"}
VALID_SECURITY   = {"A1", "A2", "B", "C", "D"}
VALID_RESILIENCE = {"0", "1", "2", "3", "4"}

def normalize(val, allowed, default, transform=str.upper):
    if not val:
        return default
    v = transform(str(val).strip())
    return v if v in allowed else default

def norm_security(val):
    return normalize(val, VALID_SECURITY, "A2", lambda x: "A1" if x.upper() == "A" else x.upper())

def load_registry(path: str) -> list[dict]:
    with open(path, "r") as f:
        y = yaml.safe_load(f) or {}
    return [
        {"key": i["key"], "derived_from": i["derived_from"], "rule": i["rule"]}
        for i in (y.get("fields") or [])
        if isinstance(i, dict) and i.get("key") and i.get("derived_from") and isinstance(i.get("rule"), dict)
    ]

# --------------------------------------------------------------------------------------
# Stage ODS → tmp_src
# --------------------------------------------------------------------------------------
def stage_source_rows(tgt_conn, ods_conn) -> int:
    with tgt_conn.cursor() as cur_tgt, ods_conn.cursor() as cur_ods:
        cur_tgt.execute("""
            CREATE TEMP TABLE tmp_src (
              lean_control_service_id  text,
              jira_backlog_id          text,
              service_offering_join    text,
              app_criticality          text,
              security_rating          text,
              integrity_rating         text,
              availability_rating      text,
              resilience_rating        text,
              app_correlation_id       text
            ) ON COMMIT DROP
        """)
        buf = io.StringIO()
        cur_ods.copy_expert(f"COPY ({SQL}) TO STDOUT WITH CSV DELIMITER ',' NULL ''", buf)
        data = buf.getvalue()
        row_count = 0 if not data else data.count("\n")
        buf.seek(0)
        cur_tgt.copy_expert("COPY tmp_src FROM STDIN WITH CSV DELIMITER ',' NULL ''", buf)
        return row_count

# --------------------------------------------------------------------------------------
# Upserts
# --------------------------------------------------------------------------------------
def upsert_applications_from_stage(tgt_conn, scope=APP_SCOPE_DEFAULT, onboarding=APP_ONBOARDING_DEFAULT):
    with tgt_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO application (
                app_id, scope, app_criticality_assessment,
                jira_backlog_id, lean_control_service_id,
                onboarding_status, updated_at
            )
            SELECT
                s.app_correlation_id,
                %s,
                MAX(
                  CASE
                    WHEN UPPER(NULLIF(s.app_criticality,'')) IN ('A','B','C','D')
                      THEN UPPER(NULLIF(s.app_criticality,''))
                    ELSE NULL
                  END
                ) as app_criticality_assessment,
                MAX(NULLIF(s.jira_backlog_id,'')) as jira_backlog_id,
                MAX(NULLIF(s.lean_control_service_id,'')) as lean_control_service_id,
                %s,
                now()
            FROM tmp_src s
            WHERE s.app_correlation_id IS NOT NULL
            GROUP BY s.app_correlation_id
            ON CONFLICT (app_id) DO UPDATE SET
                app_criticality_assessment = COALESCE(EXCLUDED.app_criticality_assessment, application.app_criticality_assessment),
                jira_backlog_id            = COALESCE(EXCLUDED.jira_backlog_id, application.jira_backlog_id),
                lean_control_service_id    = COALESCE(EXCLUDED.lean_control_service_id, application.lean_control_service_id),
                scope                      = COALESCE(application.scope, EXCLUDED.scope),
                onboarding_status          = COALESCE(application.onboarding_status, EXCLUDED.onboarding_status),
                updated_at                 = now();
        """, (scope, onboarding))

def upsert_profile(cur, profile_id: str, scope_type: str, scope_id: str, version: int):
    cur.execute("""
        INSERT INTO profile (profile_id, scope_type, scope_id, version)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (scope_type, scope_id, version)
        DO UPDATE SET updated_at = now()
    """, (profile_id, scope_type, scope_id, version))

def upsert_fields(cur, rows):
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO profile_field
            (id, profile_id, key, value, source_system, source_ref, collected_at, updated_at)
        VALUES %s
        ON CONFLICT (profile_id, key) DO UPDATE SET
          value         = EXCLUDED.value,
          source_system = EXCLUDED.source_system,
          source_ref    = EXCLUDED.source_ref,
          collected_at  = EXCLUDED.collected_at,
          updated_at    = now()
    """, rows)

# --------------------------------------------------------------------------------------
# Process staged rows
# --------------------------------------------------------------------------------------
def process_rows(tgt_conn, registry):
    now_utc = datetime.now(timezone.utc)
    reg_by_src = {}
    for item in registry:
        reg_by_src.setdefault(item["derived_from"], []).append(item)

    stats = {"profiles_written": 0, "fields_written": 0,
             "skipped_missing_input": 0, "skipped_no_rule": 0}

    with tgt_conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM tmp_src;")
        for r in cur.fetchall():
            app_id = r["app_correlation_id"]
            if not app_id:
                continue

            # Normalized base ratings
            row_ctx = dict(
                security_rating     = norm_security(r["security_rating"]),
                integrity_rating    = normalize(r["integrity_rating"], VALID_LETTERS, "C"),
                availability_rating = normalize(r["availability_rating"], VALID_LETTERS, "C"),
                resilience_rating   = normalize(r["resilience_rating"], VALID_RESILIENCE, "2", str),
                app_criticality     = normalize(r["app_criticality"], VALID_LETTERS, "C"),
            )

            pid = profile_pk(SCOPE_TYPE, app_id, PROFILE_VERSION)
            upsert_profile(cur, pid, SCOPE_TYPE, app_id, PROFILE_VERSION)
            stats["profiles_written"] += 1

            def make_field(key, value, src_ref):
                return (field_pk(pid, key), pid, key, extras.Json(value),
                        SRC_SYS, src_ref, now_utc, now_utc)


            src_ref = r.get("jira_backlog_id")
            context_rows = [make_field(k, row_ctx[k], src_ref)
                            for k in ("security_rating", "integrity_rating",
                                      "availability_rating", "resilience_rating",
                                      "app_criticality")]
            # copy extra IDs if present
            for k in ("lean_control_service_id","jira_backlog_id","service_offering_join"):
                if r.get(k):
                    context_rows.append(make_field(k, str(r[k]).strip(), src_ref))

            # Derived fields
            derived_rows = []
            for src_key, items in reg_by_src.items():
                src_val = row_ctx.get(src_key)
                if not src_val:
                    stats["skipped_missing_input"] += len(items)
                    continue
                for it in items:
                    out = it["rule"].get(str(src_val))
                    if out is None:
                        stats["skipped_no_rule"] += 1
                    else:
                        derived_rows.append(make_field(it["key"], out, src_ref))

            if not DRY_RUN:
                upsert_fields(cur, context_rows)
                upsert_fields(cur, derived_rows)

            stats["fields_written"] += len(context_rows) + len(derived_rows)
    return stats

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    print("Loading registry:", REGISTRY_PATH)
    registry = load_registry(REGISTRY_PATH)
    print(f"Registry fields: {len(registry)}")

    with psycopg2.connect(TGT_DSN) as tgt_conn, psycopg2.connect(ODS_DSN) as ods_conn:
        rows_staged = stage_source_rows(tgt_conn, ods_conn)
        print(f"Staged {rows_staged} rows.")
        if rows_staged == 0:
            print("No rows. Exiting.")
            return

        print("Upserting applications...")
        upsert_applications_from_stage(tgt_conn)

        print("Processing profiles...")
        stats = process_rows(tgt_conn, registry)

        print("Done.")
        print(json.dumps(stats, indent=2, default=str))
        if DRY_RUN:
            print("NOTE: DRY_RUN=1 → profile/profile_field writes skipped")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
