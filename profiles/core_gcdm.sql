-- =========================================
-- FRESH START: drop views, function, tables
-- =========================================
DROP VIEW IF EXISTS v_profile_fields CASCADE;
DROP VIEW IF EXISTS v_app_profiles_latest CASCADE;

DROP FUNCTION IF EXISTS set_updated_at() CASCADE;

DROP TABLE IF EXISTS evidence CASCADE;
DROP TABLE IF EXISTS profile_field CASCADE;
DROP TABLE IF EXISTS profile CASCADE;
DROP TABLE IF EXISTS policy_requirement CASCADE;
DROP TABLE IF EXISTS control_claim CASCADE;
DROP TABLE IF EXISTS risk_story CASCADE;
DROP TABLE IF EXISTS release CASCADE;
DROP TABLE IF EXISTS application CASCADE;
DROP TABLE IF EXISTS service_instances CASCADE;

-- Needed for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================
-- APPLICATION  (TEXT IDs)
-- =========================
CREATE TABLE IF NOT EXISTS application (
           app_id                       text PRIMARY KEY,
           scope                        text NOT NULL DEFAULT 'application',
           parent_app_id                text,                         -- correlation id of parent
           parent_app_name              text,                         -- human-readable parent name
           name                         text,                         -- application display name
           business_service_name        text,
           app_criticality_assessment   text,
           security_rating              text,
           confidentiality_rating       text,
           integrity_rating             text,
           availability_rating          text,
           resilience_rating            text,
           business_application_sys_id  text,                         -- source sys id (child_app)
           architecture_hosting         text,
           jira_backlog_id              text,
           lean_control_service_id      text,
           repo_id                      text,
           operational_status           text,
           transaction_cycle            text,
           transaction_cycle_id         text,                         -- from "owning_transaction_cycle>id"
           application_type             text,
           application_tier             text,
           architecture_type            text,
           install_type                 text,
           house_position               text,
           product_owner                text,
           product_owner_brid           text,
           system_architect             text,
           system_architect_brid        text,
           onboarding_status            text NOT NULL DEFAULT 'pending',
           owner_id                     text,
           created_at                   timestamptz NOT NULL DEFAULT now(),
           updated_at                   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_application_scope  ON application(scope);
CREATE INDEX IF NOT EXISTS idx_application_parent ON application(parent_app_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_application_name ON application(lower(name));

-- =========================
-- SERVICE_INSTANCES (TEXT IDs)
-- =========================
CREATE TABLE IF NOT EXISTS service_instances (
         it_service_instance_sysid  text PRIMARY KEY,                   -- source PK
         app_id                     text NOT NULL REFERENCES application(app_id) ON DELETE CASCADE,

         environment                text,                               -- e.g., Prod/UAT/QA (non-null & not 'Dev' enforced at ingest)
         it_business_service_sysid  text,
         business_application_sysid text,
         service_offering_join      text,
         service_instance           text,                               -- display/name
         install_type               text,
         service_classification     text,

         created_at                 timestamptz NOT NULL DEFAULT now(),
         updated_at                 timestamptz NOT NULL DEFAULT now()
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_si_app_id        ON service_instances(app_id);
CREATE INDEX IF NOT EXISTS idx_si_environment   ON service_instances(environment);
CREATE INDEX IF NOT EXISTS idx_si_ba_sysid      ON service_instances(business_application_sysid);
CREATE INDEX IF NOT EXISTS idx_si_svc_off_join  ON service_instances(service_offering_join);



-- =========================
-- RELEASE  (TEXT IDs)
-- =========================
CREATE TABLE release (
                         release_id   text PRIMARY KEY,
                         scope_type   text NOT NULL,
                         scope_id     text NOT NULL,
                         version      text NOT NULL,
                         window_start timestamptz,
                         window_end   timestamptz,
                         created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_release_scope  ON release(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_release_window ON release(window_start, window_end);

-- =========================
-- PROFILE  (TEXT IDs)
-- =========================
CREATE TABLE profile (
                         profile_id  text PRIMARY KEY,
                         scope_type  text NOT NULL,
                         scope_id    text NOT NULL,
                         version     integer NOT NULL,
                         snapshot_at timestamptz NOT NULL DEFAULT now(),
                         created_at  timestamptz NOT NULL DEFAULT now(),
                         updated_at  timestamptz NOT NULL DEFAULT now(),
                         CONSTRAINT uq_profile UNIQUE (scope_type, scope_id, version)
);

CREATE INDEX IF NOT EXISTS idx_profile_scope   ON profile(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_profile_version ON profile(version);

-- =========================
-- PROFILE_FIELD  (TEXT IDs)
-- =========================
CREATE TABLE profile_field (
                               id            text PRIMARY KEY,
                               profile_id    text NOT NULL REFERENCES profile(profile_id) ON DELETE CASCADE,
                               field_key     text NOT NULL,
                               derived_from  text NOT NULL,
                               value         jsonb,
                               confidence    text,
                               source_system text,
                               source_ref    text,
                               collected_at  timestamptz,
                               created_at    timestamptz NOT NULL DEFAULT now(),
                               updated_at    timestamptz NOT NULL DEFAULT now(),
                               CONSTRAINT uq_profile_field UNIQUE (profile_id, field_key)
);

CREATE INDEX IF NOT EXISTS idx_profile_field_profile ON profile_field(profile_id);
CREATE INDEX IF NOT EXISTS idx_profile_field_key     ON profile_field(field_key);

-- =========================
-- EVIDENCE  (TEXT IDs) with lifecycle fields and SHA dedup
-- =========================
CREATE TABLE evidence (
                          evidence_id       text PRIMARY KEY DEFAULT concat('ev_', md5(gen_random_uuid()::text)),
                          profile_field_id  text NOT NULL REFERENCES profile_field(id) ON DELETE CASCADE,
                          uri               text NOT NULL,
                          type              text,               -- e.g. link | file | attestation
                          sha256            text,               -- fingerprint for reuse/dedupe
                          source_system     text,               -- where it came from (e.g. MANUAL, GITLAB, SNOW)
                          submitted_by      text,               -- user/system that submitted it

                          valid_from        timestamptz DEFAULT now(),
                          valid_until       timestamptz,

                          status            text NOT NULL DEFAULT 'active',
    -- lifecycle: active (approved), superseded (auto on newer approval), revoked (rejected/withdrawn)

                          revoked_at        timestamptz,        -- when evidence was rejected/withdrawn
                          reviewed_by       text,               -- reviewer/approver ID (SME/system)
                          reviewed_at       timestamptz,        -- when review decision was made

                          tags              text,               -- optional categorization
                          added_at          timestamptz NOT NULL DEFAULT now(),   -- legacy compat
                          created_at        timestamptz NOT NULL DEFAULT now(),
                          updated_at        timestamptz NOT NULL DEFAULT now(),

                          CONSTRAINT uq_evidence_pf_uri UNIQUE (profile_field_id, uri),
                          CONSTRAINT chk_evidence_status CHECK (status IN ('active','superseded','revoked'))
);

-- Indices / dedup for evidence
CREATE INDEX IF NOT EXISTS idx_evidence_pf            ON evidence(profile_field_id);
CREATE INDEX IF NOT EXISTS idx_evidence_status        ON evidence(status);
CREATE INDEX IF NOT EXISTS idx_evidence_valid_window  ON evidence(valid_from, valid_until);
CREATE INDEX IF NOT EXISTS idx_evidence_sha256        ON evidence(sha256);

-- SHA-based dedup per profile field (multiple NULLs allowed by default)
DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes WHERE indexname = 'ux_evidence_pf_sha'
        ) THEN
            CREATE UNIQUE INDEX ux_evidence_pf_sha ON evidence(profile_field_id, sha256);
        END IF;
    END $$;

-- =========================
-- POLICY_REQUIREMENT  (TEXT IDs)
-- =========================
CREATE TABLE policy_requirement (
                                    requirement_id          text PRIMARY KEY,
                                    domain                  text NOT NULL,
                                    policy_id               text NOT NULL,
                                    scope_type              text NOT NULL,
                                    scope_id                text NOT NULL,
                                    release_id              text REFERENCES release(release_id) ON DELETE CASCADE,
                                    required_evidence_types text,
                                    severity                text,
                                    due_date                timestamptz,
                                    policy_version_applied  text,
                                    created_at              timestamptz NOT NULL DEFAULT now(),
                                    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_requirement_scope   ON policy_requirement(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_requirement_release ON policy_requirement(release_id);
CREATE INDEX IF NOT EXISTS idx_requirement_domain  ON policy_requirement(domain);

-- =========================
-- CONTROL_CLAIM  (TEXT IDs) with decision_json
-- =========================
CREATE TABLE control_claim (
                               claim_id       text PRIMARY KEY,
                               requirement_id text REFERENCES policy_requirement(requirement_id) ON DELETE CASCADE,
                               scope_type     text NOT NULL,
                               scope_id       text NOT NULL,
                               release_id     text REFERENCES release(release_id) ON DELETE SET NULL,
                               method         text,
                               status         text,
                               submitted_at   timestamptz,
                               reviewed_at    timestamptz,
                               assigned_at    timestamptz,
                               comment        text,
                               decision_json  jsonb,   -- OPA or reviewer decision details
                               created_at     timestamptz NOT NULL DEFAULT now(),
                               updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_claim_requirement ON control_claim(requirement_id);
CREATE INDEX IF NOT EXISTS idx_claim_scope       ON control_claim(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_claim_release     ON control_claim(release_id);

-- =========================
-- RISK_STORY  (TEXT IDs)
-- =========================
CREATE TABLE risk_story (
                            risk_key    text PRIMARY KEY,
                            domain      text NOT NULL,
                            status      text NOT NULL,
                            scope_type  text NOT NULL,
                            scope_id    text NOT NULL,
                            release_id  text REFERENCES release(release_id) ON DELETE SET NULL,
                            sla_due     timestamptz,
                            created_at  timestamptz NOT NULL DEFAULT now(),
                            updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_risk_scope   ON risk_story(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_risk_release ON risk_story(release_id);

-- =========================
-- Triggers: keep updated_at fresh
-- =========================
CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END$$;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_application_updated_at') THEN
            CREATE TRIGGER trg_application_updated_at BEFORE UPDATE ON application
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_profile_updated_at') THEN
            CREATE TRIGGER trg_profile_updated_at BEFORE UPDATE ON profile
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_profile_field_updated_at') THEN
            CREATE TRIGGER trg_profile_field_updated_at BEFORE UPDATE ON profile_field
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_policy_requirement_updated_at') THEN
            CREATE TRIGGER trg_policy_requirement_updated_at BEFORE UPDATE ON policy_requirement
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_control_claim_updated_at') THEN
            CREATE TRIGGER trg_control_claim_updated_at BEFORE UPDATE ON control_claim
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_risk_story_updated_at') THEN
            CREATE TRIGGER trg_risk_story_updated_at BEFORE UPDATE ON risk_story
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_evidence_updated_at') THEN
            CREATE TRIGGER trg_evidence_updated_at BEFORE UPDATE ON evidence
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_service_instances_updated_at') THEN
            CREATE TRIGGER trg_service_instances_updated_at BEFORE UPDATE ON service_instances
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;


    END$$;

-- =========================
-- Helpful views
-- =========================
CREATE OR REPLACE VIEW v_app_profiles_latest AS
SELECT p.*
FROM profile p
         JOIN (
    SELECT scope_type, scope_id, MAX(version) AS max_version
    FROM profile
    WHERE scope_type = 'application'
    GROUP BY scope_type, scope_id
) lv
              ON p.scope_type = lv.scope_type
                  AND p.scope_id   = lv.scope_id
                  AND p.version    = lv.max_version;

CREATE OR REPLACE VIEW v_profile_fields AS
SELECT p.scope_type,
       p.scope_id,
       p.version,
       f.field_key,
       f.value,
       f.source_system,
       f.source_ref,
       f.collected_at
FROM profile p
         JOIN profile_field f ON f.profile_id = p.profile_id;
