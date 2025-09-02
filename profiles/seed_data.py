#!/usr/bin/env python3
# seed -> create profiles -> write md+json -> push -> post
# Tables: spdw_vwsfitbusinessservice, spdw_vwsfserviceoffering, spdw_vwsfbusinessapplication, spdw_vwsfitserviceinstance

import os, sys, json, uuid, random, argparse, glob
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import psycopg2
import requests
from git import Repo

# ================== CONFIG ==================
# Cockpit
COCKPIT_API_BASE  = "http://localhost:8080"
COCKPIT_API_TOKEN = os.getenv("COCKPIT_API_TOKEN", "")  # optional

# GitLab (from env)
GITLAB_API_TOKEN = os.getenv("GITLAB_API_TOKEN")
if not GITLAB_API_TOKEN:
    print("Missing $GITLAB_API_TOKEN", file=sys.stderr)
    sys.exit(1)

GIT_SERVER   = "eros.butterflycluster.com"
GIT_GROUP    = "staging"
GIT_PROJECT  = "dummy_evidence"
GIT_BRANCH   = "main"
WORKDIR      = "/tmp/evidence-repo"
EVIDENCE_ROOT= "evidence"

GIT_REMOTE = f"https://oauth2:{GITLAB_API_TOKEN}@{GIT_SERVER}/{GIT_GROUP}/{GIT_PROJECT}.git"
RAW_BASE   = f"https://{GIT_SERVER}/{GIT_GROUP}/{GIT_PROJECT}/-/raw/{GIT_BRANCH}"

# Postgres (hard-coded)
DB = dict(dbname="lct_data", user="postgres", password="postgres", host="helios", port=5432)

# ================== DOMAIN VALUES ==================
APPLICATION_TYPES = ["application component", "business application"]
APPLICATION_TIERS = ["Frontend", "Backend", "Database"]
ARCHITECTURE_TYPES = ["Monolith", "Microservice", "SOA"]
INSTALL_TYPES = ["OnPrem", "Cloud", "Hybrid"]
HOUSE_POSITIONS = ["invest", "divest", "cease", "maintain"]
STATUSES = ["Active", "Inactive", "Decommissioned"]
CYCLES = ["Payments","Trading","HR","Finance","Lending","Treasury"]
SECURITY_RATINGS = ["A1","A2","B","C","D"]
ABCD = ["A","B","C","D"]
RESILIENCY = ["0","1","2","3","4"]
HOSTING = ["AWS","Azure","GCP","OnPremCluster"]

# Adjectives + Nouns for app names
ADJECTIVES = ["swift","prime","stellar","lunar","emerald","cobalt","vivid","crimson","golden","onyx","quantum","silent","rapid","brisk"]
NOUNS      = ["atlas","harbor","forge","matrix","beacon","vertex","cascade","nova","horizon","anchor","circuit","compass","cipher","pillar"]

# ================== HELPERS ==================
def sanitize_key(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-","_") else "-" for ch in (s or ""))

def raw_url(rel_path: str) -> str:
    return f"{RAW_BASE}/{rel_path}"

def _uuid() -> str:
    return str(uuid.uuid4())

def gen_app_name() -> str:
    return f"{random.choice(ADJECTIVES)}-{random.choice(NOUNS)}-{random.randint(100,999)}"

# --- DB-driven "next value" helpers (avoid dupes) ---
def _next_prefixed(cur, table: str, col: str, prefix: str, default_base: int) -> str:
    """
    Returns next id like f"{prefix}{N}" where N = MAX(trailing_digits(col)) + 1,
    or default_base+1 if none. Works even if col is TEXT.
    """
    sql = f"""
        SELECT COALESCE(
                 MAX(CAST(REGEXP_REPLACE({col}, E'\\D', '', 'g') AS INTEGER)),
                 %s
               ) + 1
        FROM {table}
        WHERE REGEXP_REPLACE({col}, E'\\D', '', 'g') ~ '^[0-9]+$'
    """
    cur.execute(sql, (default_base,))
    nxt = int(cur.fetchone()[0])
    return f"{prefix}{nxt}"

def _next_so_join(cur, default_base: int = 400000) -> int:
    """service_offering_join as INT: MAX()+1; if column is INT this works cleanly."""
    sql = "SELECT COALESCE(MAX(service_offering_join), %s) + 1 FROM public.spdw_vwsfserviceoffering"
    cur.execute(sql, (default_base,))
    return int(cur.fetchone()[0])

def _random_cycle_id() -> str:
    """owning_transaction_cycle_id is TEXT: return '1'..'30' as string."""
    return str(random.randint(1, 30))

# ================== STEP 1: Seed apps ==================
def seed_one_with_ids(cur, svc_corr: str, sof_corr: str, app_corr: str, parent_corr: str, so_join: int) -> str:
    cycle_id = _random_cycle_id()  # TEXT "1".."30"

    # Business Service
    bs_sysid = _uuid()
    bs_name  = f"Service-{random.randint(100,999)}"
    cur.execute("""
        INSERT INTO public.spdw_vwsfitbusinessservice
            (it_business_service_sysid, service_correlation_id, service)
        VALUES
            (%s, %s, %s)
    """, (bs_sysid, svc_corr, bs_name))

    # Service Offering
    cur.execute("""
        INSERT INTO public.spdw_vwsfserviceoffering (
            correlation_id,
            resiliency_category,
            app_criticality_assessment,
            availability_rating,
            confidentiality_rating,
            integrity_rating,
            service_offering_join,
            security_rating
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        sof_corr,
        int(random.choice(RESILIENCY)),  # smallint field
        random.choice(ABCD),
        random.choice(ABCD),
        random.choice(ABCD),
        random.choice(ABCD),
        so_join,
        random.choice(SECURITY_RATINGS)
    ))

    # Business Application
    ba_sys_id   = _uuid()        # UUID sys_id
    app_name    = gen_app_name()
    cur.execute("""
        INSERT INTO public.spdw_vwsfbusinessapplication (
            correlation_id, business_application_name, application_type, application_tier,
            architecture_type, install_type, house_position, operational_status,
            owning_transaction_cycle, owning_transaction_cycle_id,
            application_product_owner, application_product_owner_brid,
            system_architect, system_architect_brid,
            business_application_sys_id, application_parent, application_parent_correlation_id, architecture_hosting
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        app_corr,
        app_name,
        random.choice(APPLICATION_TYPES), random.choice(APPLICATION_TIERS),
        random.choice(ARCHITECTURE_TYPES), random.choice(INSTALL_TYPES),
        random.choice(HOUSE_POSITIONS), random.choice(STATUSES),
        random.choice(CYCLES), cycle_id,  # TEXT id
        "Owner Name", "u12345", "Architect Name", "u54321",
        ba_sys_id, "Parent", parent_corr, random.choice(HOSTING)
    ))

    # Service Instance (only columns used by your joins)
    cur.execute("""
        INSERT INTO public.spdw_vwsfitserviceinstance (
            it_business_service_sysid, business_application_sysid, service_offering_join
        ) VALUES (%s,%s,%s)
    """, (bs_sysid, ba_sys_id, so_join))

    return app_corr

def seed_apps(n: int) -> List[str]:
    if n <= 0: return []
    conn = psycopg2.connect(**DB)
    try:
        with conn:
            with conn.cursor() as cur:
                # Pre-allocate all IDs to avoid duplicates
                base_svc = _next_prefixed(cur, "public.spdw_vwsfitbusinessservice", "service_correlation_id", "SVC", 300000)
                base_sof = _next_prefixed(cur, "public.spdw_vwsfserviceoffering", "correlation_id", "SOF", 350000) 
                base_app = _next_prefixed(cur, "public.spdw_vwsfbusinessapplication", "correlation_id", "APM", 100000)
                base_so_join = _next_so_join(cur)
                
                # Extract numeric parts
                svc_num = int(base_svc[3:])  # Remove "SVC" prefix
                sof_num = int(base_sof[3:])  # Remove "SOF" prefix  
                app_num = int(base_app[3:])  # Remove "APM" prefix
                
                ids = []
                for i in range(n):
                    ids.append(seed_one_with_ids(cur, 
                        f"SVC{svc_num + i}",
                        f"SOF{sof_num + i}", 
                        f"APM{app_num + i}",
                        f"APM{app_num + i + n}",  # parent_corr offset by n
                        base_so_join + i
                    ))
        return ids
    finally:
        conn.close()

# ================== STEP 2: Create Cockpit profiles ==================
def create_profile(app_id: str) -> None:
    url = f"{COCKPIT_API_BASE}/api/apps"
    headers = {"Content-Type": "application/json"}
    if COCKPIT_API_TOKEN: headers["Authorization"] = COCKPIT_API_TOKEN
    try:
        r = requests.post(url, headers=headers, data=json.dumps({"appId": app_id}), timeout=30)
        if r.status_code not in (200, 201, 409):  # 409 = already exists
            raise RuntimeError(f"Failed to create profile for {app_id}: HTTP {r.status_code} - {r.text[:200]}")
    except requests.RequestException as e:
        raise RuntimeError(f"Network error creating profile for {app_id}: {e}") from e

# ================== STEP 3: Write MD+JSON and push ==================
def ensure_repo() -> Repo:
    if os.path.isdir(os.path.join(WORKDIR, ".git")):
        return Repo(WORKDIR)
    os.makedirs(WORKDIR, exist_ok=True)
    return Repo.clone_from(GIT_REMOTE, WORKDIR, branch=GIT_BRANCH)

def fetch_profile(app_id: str) -> Optional[Dict[str, Any]]:
    headers = {}
    if COCKPIT_API_TOKEN: headers["Authorization"] = COCKPIT_API_TOKEN
    try:
        r = requests.get(f"{COCKPIT_API_BASE}/api/apps/{app_id}/profile", headers=headers, timeout=30)
        if r.status_code == 404:
            return None  # Profile doesn't exist yet, this is expected
        if r.status_code != 200:
            raise RuntimeError(f"Failed to fetch profile for {app_id}: HTTP {r.status_code} - {r.text[:200]}")
        return r.json()
    except (ValueError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Invalid JSON response for app {app_id}: {r.text[:200]}") from e
    except requests.RequestException as e:
        raise RuntimeError(f"Network error fetching profile for {app_id}: {e}") from e

def flatten_fields(profile: Dict[str, Any]) -> List[Tuple[Dict[str, Any], List[str]]]:
    out: List[Tuple[Dict[str, Any], List[str]]] = []
    for d in profile.get("domains", []):
        fields = d.get("fields", [])
        keys = [f.get("fieldKey") for f in fields if f.get("fieldKey")]
        for f in fields:
            peers = [k for k in keys if k and k != f.get("fieldKey")]
            out.append((f, peers))
    return out

def md_content(app_id: str, field_key: str, profile_field_id: str) -> str:
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    return f"# Evidence for {field_key}\n\nApp: {app_id}\nField: {profile_field_id}\nGenerated: {now}\n"

def json_payload(profile_field_id: str, field_key: str, md_url: str, peers: List[str]) -> str:
    now = datetime.utcnow().replace(microsecond=0)
    rel2 = peers[:2]
    body = {
        "profileFieldId": profile_field_id,
        "document": {
            "title": f"{field_key} Evidence",
            "url": md_url,
            "relatedEvidenceFields": rel2
        },
        "evidence": {
            "type": "document",
            "sourceSystem": "manual",
            "submittedBy": "security_analyst_001",
            "validFrom": now.isoformat(),
            "validUntil": (now + timedelta(days=365)).isoformat(),
            "relatedEvidenceFields": ",".join(rel2)
        }
    }
    return json.dumps(body, indent=2)

def generate_and_push(app_ids: List[str]) -> None:
    repo = ensure_repo()
    for app_id in app_ids:
        prof = fetch_profile(app_id)
        if not prof: continue
        for field, peers in flatten_fields(prof):
            pfid = field.get("profileFieldId"); fkey = field.get("fieldKey")
            if not pfid or not fkey: continue
            safe = sanitize_key(fkey)
            base = f"{EVIDENCE_ROOT}/{app_id}"
            md_rel = f"{base}/{app_id}_{safe}.md"
            json_rel = f"{base}/{app_id}_{safe}.json"
            os.makedirs(os.path.join(WORKDIR, base), exist_ok=True)
            with open(os.path.join(WORKDIR, md_rel), "w", encoding="utf-8") as f:
                f.write(md_content(app_id, fkey, pfid))
            with open(os.path.join(WORKDIR, json_rel), "w", encoding="utf-8") as f:
                f.write(json_payload(pfid, fkey, raw_url(md_rel), peers))
            repo.git.add([md_rel, json_rel])
        repo.index.commit(f"seed evidence for app {app_id}")
    repo.remote().push(GIT_BRANCH)

# ================== STEP 4: POST evidence ==================
def post_evidence(app_id: str) -> None:
    headers = {"Content-Type":"application/json"}
    if COCKPIT_API_TOKEN: headers["Authorization"] = COCKPIT_API_TOKEN
    root = os.path.join(WORKDIR, EVIDENCE_ROOT, app_id)
    if not os.path.exists(root):
        raise RuntimeError(f"No evidence directory found for {app_id} at {root}")
    
    evidence_files = glob.glob(os.path.join(root, f"{app_id}_*.json"))
    if not evidence_files:
        raise RuntimeError(f"No evidence files found for {app_id} in {root}")
    
    for p in evidence_files:
        try:
            with open(p, "r", encoding="utf-8") as f:
                body = json.load(f)
            r = requests.post(
                f"{COCKPIT_API_BASE}/api/apps/{app_id}/evidence/with-document",
                headers=headers, data=json.dumps(body), timeout=30
            )
            if r.status_code not in (200, 201):
                raise RuntimeError(f"Failed to post evidence {os.path.basename(p)}: HTTP {r.status_code} - {r.text[:200]}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON in evidence file {os.path.basename(p)}: {e}") from e
        except requests.RequestException as e:
            raise RuntimeError(f"Network error posting evidence {os.path.basename(p)}: {e}") from e

# ================== ORCHESTRATOR ==================
def clean_tables():
    """Clean all seeded data from tables"""
    conn = psycopg2.connect(**DB)
    try:
        with conn:
            with conn.cursor() as cur:
                print("Cleaning existing seeded data...")
                # Delete in reverse dependency order
                cur.execute("DELETE FROM public.spdw_vwsfitserviceinstance")
                cur.execute("DELETE FROM public.spdw_vwsfbusinessapplication WHERE correlation_id LIKE 'APM%'")
                cur.execute("DELETE FROM public.spdw_vwsfserviceoffering WHERE correlation_id LIKE 'SOF%'") 
                cur.execute("DELETE FROM public.spdw_vwsfitbusinessservice WHERE service_correlation_id LIKE 'SVC%'")
                print("Tables cleaned.")
    finally:
        conn.close()

def main():
    ap = argparse.ArgumentParser(description="Seed -> create profiles -> md+json -> push -> post")
    ap.add_argument("-n", "--count", type=int, required=True, help="How many apps to seed")
    ap.add_argument("--clean", action="store_true", help="Clean existing seeded data first")
    args = ap.parse_args()

    if args.clean:
        clean_tables()

    app_ids = seed_apps(args.count)
    print("Seeded appIds:", app_ids)

    for a in app_ids:
        create_profile(a)
    generate_and_push(app_ids)
    for a in app_ids:
        post_evidence(a)

    print("Done.")

if __name__ == "__main__":
    main()