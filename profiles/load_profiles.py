#!/usr/bin/env python3
"""
etl_profiles_from_registry_bulk.py

Bulk-prepares tables as CSV, then does a single bulk load into target tables.

Flow:
  1) Stage ODS rows into TEMP tmp_src via COPY
  2) Prepare records for application, profile, profile_field as CSV
  3) Load CSVs into target Postgres tables (COPY FROM)

Environment:
  ODS_DSN, TGT_DSN, REGISTRY_YAML, PROFILE_SCOPE, PROFILE_VERSION,
  SRC_SYS, DRY_RUN, APP_SCOPE_DEFAULT, APP_ONBOARDING_DEFAULT
"""

import os, sys, io, csv, json, yaml, hashlib, tempfile
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import DictCursor

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
ODS_DSN       = os.getenv("ODS_DSN", "postgresql://postgres:postgres@192.168.1.188:5432/lct_data")
TGT_DSN       = os.getenv("TGT_DSN", "postgresql://postgres:postgres@192.168.1.188:5432/lct_data")
REGISTRY_PATH = os.getenv("REGISTRY_YAML", "fields.v1.yaml")
SCOPE_TYPE      = os.getenv("PROFILE_SCOPE", "application")
PROFILE_VERSION = int(os.getenv("PROFILE_VERSION", "1"))
SRC_SYS         = os.getenv("SRC_SYS", "SERVICE_NOW")
APP_SCOPE_DEFAULT      = os.getenv("APP_SCOPE_DEFAULT", "application")
APP_ONBOARDING_DEFAULT = os.getenv("APP_ONBOARDING_DEFAULT", "pending")
DRY_RUN         = os.getenv("DRY_RUN", "0") == "1"

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
  child_app.correlation_id      AS app_correlation_id,
  bs.service                    AS business_service_name,

  -- newly requested child_app fields
  child_app.owning_transaction_cycle             AS transaction_cycle,
  child_app.application_type                     AS application_type,
  child_app.application_tier                     AS application_tier,
  child_app.business_application_name            AS business_application_name,
  child_app.architecture_type                    AS architecture_type,
  child_app.install_type                         AS install_type,
  child_app.application_parent_correlation_id    AS application_parent_id,
  child_app.house_position                       AS house_position,
  child_app.application_product_owner             AS product_owner,
  child_app.application_product_owner_brid        AS product_owner_brid,
  child_app.operational_status                   AS operational_status
FROM public.spdw_vwsfitbusinessservice bs
JOIN public.lean_control_application lca
  ON lca.servicenow_app_id = bs.service_correlation_id
JOIN public.spdw_vwsfitserviceinstance si
  ON bs.it_business_service_sysid = si.it_business_service_sysid
JOIN public.lean_control_product_backlog_details lpbd
  ON lpbd.lct_product_id = lca.lean_control_service_id AND lpbd.is_parent = TRUE
JOIN public.spdw_vwsfbusinessapplication child_app
  ON si.business_application_sysid = child_app.business_application_sys_id
JOIN public.spdw_vwsfserviceoffering so
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
# ETL Bulk Processing
# --------------------------------------------------------------------------------------
def stage_source_rows(tgt_conn, ods_conn) -> int:
    with tgt_conn.cursor() as cur_tgt, ods_conn.cursor() as cur_ods:
        cur_tgt.execute("""
            CREATE TEMP TABLE tmp_src (
              lean_control_service_id    text,
              jira_backlog_id            text,
              service_offering_join      text,
              app_criticality            text,
              security_rating            text,
              integrity_rating           text,
              availability_rating        text,
              resilience_rating          text,
              app_correlation_id         text,
              business_service_name      text,
              transaction_cycle          text,
              application_type           text,
              application_tier           text,
              business_application_name  text,
              architecture_type          text,
              install_type               text,
              application_parent_id      text,
              house_position             text,
              product_owner              text,
              product_owner_brid         text,
              operational_status         text
            ) ON COMMIT DROP
        """)
        buf = io.StringIO()
        cur_ods.copy_expert(f"COPY ({SQL}) TO STDOUT WITH CSV DELIMITER ',' NULL ''", buf)
        data = buf.getvalue()
        row_count = 0 if not data else data.count("\n")
        buf.seek(0)
        cur_tgt.copy_expert("COPY tmp_src FROM STDIN WITH CSV DELIMITER ',' NULL ''", buf)
        return row_count

def prepare_data_for_bulk(tgt_conn, registry):
    now_utc = datetime.now(timezone.utc).isoformat()
    reg_by_src = {}
    for item in registry:
        reg_by_src.setdefault(item["derived_from"], []).append(item)

    applications = {}
    profiles = {}
    profile_fields_dict = {}   # DEDUP: key = (profile_id, field_key)

    with tgt_conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM tmp_src;")
        rows = cur.fetchall()
        for r in rows:
            app_id = r["app_correlation_id"]
            if not app_id:
                continue

            app_crit = normalize(r["app_criticality"], VALID_LETTERS, None)

            # Build application row (maps requested child_app fields to application table columns)
            app_row = (
                app_id,
                APP_SCOPE_DEFAULT,
                r["application_parent_id"] or None,         # parent_app_id
                r["business_application_name"] or None,
                r["business_service_name"] or None,
                app_crit,
                r["jira_backlog_id"] or None,
                r["lean_control_service_id"] or None,
                None,                                       # repo_id (not provided by source)
                r["operational_status"] or None,
                r["transaction_cycle"] or None,
                r["application_type"] or None,
                r["application_tier"] or None,
                r["architecture_type"] or None,
                r["install_type"] or None,
                r["house_position"] or None,
                r["product_owner"] or None,
                r["product_owner_brid"] or None,
                APP_ONBOARDING_DEFAULT,
                None,                                       # owner_id (unknown)
                now_utc                                     # updated_at
            )
            applications[app_id] = app_row  # dedup by app_id

            # Profile header
            pid = profile_pk(SCOPE_TYPE, app_id, PROFILE_VERSION)
            profiles[pid] = (pid, SCOPE_TYPE, app_id, PROFILE_VERSION, now_utc)

            # Context fields that feed rules
            row_ctx = dict(
                security_rating     = norm_security(r["security_rating"]),
                integrity_rating    = normalize(r["integrity_rating"], VALID_LETTERS, "C"),
                availability_rating = normalize(r["availability_rating"], VALID_LETTERS, "C"),
                resilience_rating   = normalize(r["resilience_rating"], VALID_RESILIENCE, "2", str),
                app_criticality     = normalize(r["app_criticality"], VALID_LETTERS, "C"),
            )

            src_ref = r.get("app_correlation_id")

            # Persist core context fields
            for k in ("security_rating", "integrity_rating", "availability_rating",
                      "resilience_rating", "app_criticality"):
                unique_key = (pid, k)
                profile_fields_dict[unique_key] = (
                    field_pk(pid, k), pid, k, json.dumps(row_ctx[k]),
                    SRC_SYS, src_ref, now_utc, now_utc
                )

            # Useful source refs
            for k in ("lean_control_service_id", "jira_backlog_id", "service_offering_join", "business_service_name"):
                if r.get(k):
                    unique_key = (pid, k)
                    profile_fields_dict[unique_key] = (
                        field_pk(pid, k), pid, k, json.dumps(str(r[k]).strip()),
                        SRC_SYS, src_ref, now_utc, now_utc
                    )

            # Derived fields from registry
            for src_key, items in reg_by_src.items():
                src_val = row_ctx.get(src_key)
                if not src_val:
                    continue
                for it in items:
                    out = it["rule"].get(str(src_val))
                    if out is not None:
                        unique_key = (pid, it["key"])
                        profile_fields_dict[unique_key] = (
                            field_pk(pid, it["key"]), pid, it["key"], json.dumps(out),
                            SRC_SYS, src_ref, now_utc, now_utc
                        )

    profile_fields = list(profile_fields_dict.values())
    return list(applications.values()), list(profiles.values()), profile_fields


def write_csv(filename, rows, headers):
    with open(filename, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

def copy_from_csv(conn, table, filename, columns):
    with conn.cursor() as cur, open(filename, "r") as f:
        cur.copy_expert(
            f"""
            COPY {table} ({', '.join(columns)})
            FROM STDIN WITH CSV HEADER
            """,
            f
        )

def load_bulk_to_postgres(apps, profiles, profile_fields):
    tmp = tempfile.gettempdir()
    apps_file = os.path.join(tmp, "apps.csv")
    profiles_file = os.path.join(tmp, "profiles.csv")
    fields_file = os.path.join(tmp, "profile_fields.csv")

    # application CSV (includes your new fields)
    write_csv(apps_file, apps, [
        "app_id",
        "scope",
        "parent_app_id",
        "name",
        "business_service_name",
        "app_criticality_assessment",
        "jira_backlog_id",
        "lean_control_service_id",
        "repo_id",
        "operational_status",
        "transaction_cycle",
        "application_type",
        "application_tier",
        "architecture_type",
        "install_type",
        "house_position",
        "product_owner",
        "product_owner_brid",
        "onboarding_status",
        "owner_id",
        "updated_at"
    ])

    write_csv(profiles_file, profiles, [
        "profile_id", "scope_type", "scope_id", "version", "updated_at"
    ])
    write_csv(fields_file, profile_fields, [
        "id", "profile_id", "field_key", "value", "source_system", "source_ref", "collected_at", "updated_at"
    ])

    if DRY_RUN:
        print(f"DRY_RUN=1: Would have loaded to Postgres:")
        print(f"  {apps_file}, {profiles_file}, {fields_file}")
        return

    with psycopg2.connect(TGT_DSN) as conn:
        with conn.cursor() as cur:
            print("Truncating profile_field, profile, and application tables...")
            cur.execute("TRUNCATE TABLE profile_field, profile, application CASCADE;")
            conn.commit()

        print("Loading application table from CSV...")
        copy_from_csv(conn, "application", apps_file, [
            "app_id",
            "scope",
            "parent_app_id",
            "name",
            "business_service_name",
            "app_criticality_assessment",
            "jira_backlog_id",
            "lean_control_service_id",
            "repo_id",
            "operational_status",
            "transaction_cycle",
            "application_type",
            "application_tier",
            "architecture_type",
            "install_type",
            "house_position",
            "product_owner",
            "product_owner_brid",
            "onboarding_status",
            "owner_id",
            "updated_at"
        ])

        print("Loading profile table from CSV...")
        copy_from_csv(conn, "profile", profiles_file, [
            "profile_id", "scope_type", "scope_id", "version", "updated_at"
        ])

        print("Loading profile_field table from CSV...")
        copy_from_csv(conn, "profile_field", fields_file, [
            "id", "profile_id", "field_key", "value", "source_system", "source_ref", "collected_at", "updated_at"
        ])
        conn.commit()
        print("Bulk load complete.")


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

        print("Preparing data for bulk load...")
        apps, profiles, profile_fields = prepare_data_for_bulk(tgt_conn, registry)
        print(f"Prepared: {len(apps)} apps, {len(profiles)} profiles, {len(profile_fields)} profile_fields.")

    print("Loading bulk into target database...")
    load_bulk_to_postgres(apps, profiles, profile_fields)

    print("Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
