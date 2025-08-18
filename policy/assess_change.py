#!/usr/bin/env python3

import argparse
import sys
import pandas as pd
import psycopg
import requests
import yaml


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_connection(cfg: dict):
    db = cfg["database"]
    return psycopg.connect(
        host=db["host"],
        port=int(db["port"]),
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
    )


def call_opa(opa_url: str, payload: dict, timeout: int = 5, headers: dict = None) -> dict:
    r = requests.post(opa_url, json={"input": payload}, timeout=timeout, headers=headers or {})
    if r.status_code != 200:
        raise RuntimeError(f"OPA HTTP {r.status_code}: {r.text}")
    data = r.json()
    if "result" not in data:
        raise RuntimeError(f"Unexpected OPA response: {data}")
    return data["result"]


def severity_rank(required: str) -> int:
    return {"No": 0, "Yes – Conditional": 1, "Yes – Mandatory": 2}.get(required, 0)


def canonical_arb_union(domains):
    canon = ["EA", "Security", "Data", "Service Transition"]
    selected = set()
    for d in domains:
        if not d:
            continue
        parts = [p.strip() for p in str(d).split(",")]
        for p in parts:
            if p == "All ARBs":
                return "All ARBs"
            if p in canon:
                selected.add(p)
    if not selected:
        return None
    return "All ARBs" if set(selected) == set(canon) else ", ".join([x for x in canon if x in selected])


def worst_snapshot(df: pd.DataFrame) -> dict:
    sec_rank = {"A1": 1, "A2": 2, "A": 3, "B": 4, "C": 5, "D": 6}
    abd_rank = {"A": 1, "B": 2, "C": 3, "D": 4}
    return {
        "app_criticality": min(df["app_criticality"], key=lambda x: abd_rank.get(x, 99)),
        "security_rating": min(df["security_rating"], key=lambda x: sec_rank.get(x, 99)),
        "integrity_rating": min(df["integrity_rating"], key=lambda x: abd_rank.get(x, 99)),
        "availability_rating": min(df["availability_rating"], key=lambda x: abd_rank.get(x, 99)),
        "resilience_rating": min(df["resilience_rating"], key=lambda x: abd_rank.get(x, 99)),
    }


def main():
    ap = argparse.ArgumentParser(description="Evaluate ARB routing via OPA HTTP")
    ap.add_argument("--config", default="config.yaml", help="Path to config.yaml (default: ./config.yaml)")
    ap.add_argument("--app-id", required=True, help="Application correlation id to evaluate")
    args = ap.parse_args()

    cfg = load_config(args.config)
    sql = cfg["sql"]
    opa_cfg = cfg["opa"]
    opa_url = opa_cfg["url"]
    timeout = int(opa_cfg.get("timeout_seconds", 5))
    headers = opa_cfg.get("headers", {})

    with get_connection(cfg) as conn:
        df = pd.read_sql(sql, conn, params={"app_id": args.app_id})

    if df.empty:
        print(f"No records found for app_id='{args.app_id}'.")
        sys.exit(0)

    for col in ["app_criticality", "security_rating", "integrity_rating", "availability_rating", "resilience_rating"]:
        df[col] = df[col].astype(str).str.strip().str.upper()

    print("\n=== Per-Instance Recommendations (OPA HTTP) ===")
    arb_strings, required_levels = [], []

    for _, row in df.iterrows():
        inp = {
            "criticality": row["app_criticality"],
            "security": row["security_rating"],
            "integrity": row["integrity_rating"],
            "availability": row["availability_rating"],
            "resilience": row["resilience_rating"],
        }
        try:
            decision = call_opa(opa_url, inp, timeout=timeout, headers=headers)
        except Exception as e:
            print(f"- Instance: {row['it_service_instance']}  (OPA ERROR: {e})")
            continue

        arbs_val = decision.get("arb_domains")
        if isinstance(arbs_val, list):
            arbs = ", ".join(arbs_val) if arbs_val else None
        else:
            arbs = arbs_val or None

        arb_strings.append(arbs)
        required_levels.append(decision.get("risk_assessment_required", "No"))

        print(f"- Instance: {row['it_service_instance']}")
        print(f"  Ratings: Criticality={row['app_criticality']}, Security={row['security_rating']}, "
              f"Integrity={row['integrity_rating']}, Availability={row['availability_rating']}, "
              f"Resilience={row['resilience_rating']}")
        print(f"  Recommendation: {decision.get('risk_assessment_required','No')} | "
              f"ARBs: {arbs or '—'} | Mode: {decision.get('review_mode','Express to prod')}\n")

    strongest_required = max(required_levels, key=severity_rank)
    overall_mode = {
        "Yes – Mandatory": "Full review",
        "Yes – Conditional": "Scoped review",
        "No": "Express to prod",
    }.get(strongest_required, "Express to prod")
    overall_arbs = canonical_arb_union(arb_strings)
    worst = worst_snapshot(df)

    print("=== Overall App Recommendation ===")
    print(f"App ID: {args.app_id}")
    print(f"Worst Ratings: Criticality={worst['app_criticality']}, "
          f"Security={worst['security_rating']}, Integrity={worst['integrity_rating']}, "
          f"Availability={worst['availability_rating']}, Resilience={worst['resilience_rating']}")
    print(f"Overall Recommendation: {strongest_required} | ARBs: {overall_arbs or '—'} | Mode: {overall_mode}\n")


if __name__ == "__main__":
    main()
