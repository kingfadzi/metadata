-- =========================================
-- FRESH START: drop views, function, tables
-- =========================================
DROP VIEW  IF EXISTS v_profile_fields CASCADE;
DROP VIEW  IF EXISTS v_app_profiles_latest CASCADE;

DROP FUNCTION IF EXISTS set_updated_at() CASCADE;

DROP TABLE IF EXISTS evidence CASCADE;
DROP TABLE IF EXISTS profile_field CASCADE;
DROP TABLE IF EXISTS profile CASCADE;
DROP TABLE IF EXISTS policy_requirement CASCADE;
DROP TABLE IF EXISTS control_claim CASCADE;
DROP TABLE IF EXISTS risk_story CASCADE;
DROP TABLE IF EXISTS release CASCADE;
DROP TABLE IF EXISTS application CASCADE;

-- =========================
-- APPLICATION  (TEXT IDs)
-- =========================
CREATE TABLE application (
                             app_id                   text PRIMARY KEY,
                             scope                    text NOT NULL DEFAULT 'application',
                             parent_app_id            text,
                             name                     text,
                             business_service_name    text,
                             app_criticality_assessment text,
                             jira_backlog_id          text,
                             lean_control_service_id  text,
                             repo_id                  text,
                             operational_status       text,
                             transaction_cycle        text,
                             application_type         text,
                             application_tier         text,
                             architecture_type        text,
                             install_type             text,
                             house_position           text,
                             product_owner            text,
                             product_owner_brid       text,
                             onboarding_status        text NOT NULL DEFAULT 'pending',
                             owner_id                 text,
                             created_at               timestamptz NOT NULL DEFAULT now(),
                             updated_at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_application_scope   ON application(scope);
CREATE INDEX IF NOT EXISTS idx_application_parent  ON application(parent_app_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_application_name ON application(lower(name));

-- =========================
-- RELEASE  (TEXT IDs)
-- =========================
CREATE TABLE release (
                         release_id   text PRIMARY KEY,
                         scope_type   text NOT NULL,         -- e.g., 'application'
                         scope_id     text NOT NULL,         -- target row id (e.g., application.app_id)
                         version      text NOT NULL,         -- semantic or numeric string
                         window_start timestamptz,
                         window_end   timestamptz,
                         created_at   timestamptz NOT NULL DEFAULT now()
    -- polymorphic scope; no FK on (scope_type, scope_id)
);

CREATE INDEX IF NOT EXISTS idx_release_scope   ON release(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_release_window  ON release(window_start, window_end);

-- =========================
-- PROFILE  (TEXT IDs)
-- =========================
CREATE TABLE profile (
                         profile_id  text PRIMARY KEY,
                         scope_type  text NOT NULL,          -- e.g., 'application'
                         scope_id    text NOT NULL,          -- e.g., application.app_id
                         version     integer NOT NULL,       -- profile schema version
                         snapshot_at timestamptz NOT NULL DEFAULT now(),
                         created_at  timestamptz NOT NULL DEFAULT now(),
                         updated_at  timestamptz NOT NULL DEFAULT now(),
                         CONSTRAINT uq_profile UNIQUE (scope_type, scope_id, version)
);

CREATE INDEX IF NOT EXISTS idx_profile_scope    ON profile(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_profile_version  ON profile(version);

-- =========================
-- PROFILE_FIELD  (TEXT IDs)
-- =========================
CREATE TABLE profile_field (
                               id            text PRIMARY KEY,
                               profile_id    text NOT NULL REFERENCES profile(profile_id) ON DELETE CASCADE,
                               field_key     text NOT NULL,     -- namespaced key, e.g., 'app.criticality'
                               value         jsonb,             -- flexible & indexable
                               confidence    text,              -- e.g., 'derived','owner','system'
                               source_system text,              -- provenance (e.g., 'ODS')
                               source_ref    text,              -- upstream identifier
                               collected_at  timestamptz,
                               created_at    timestamptz NOT NULL DEFAULT now(),
                               updated_at    timestamptz NOT NULL DEFAULT now(),
                               CONSTRAINT uq_profile_field UNIQUE (profile_id, field_key)
);

CREATE INDEX IF NOT EXISTS idx_profile_field_profile ON profile_field(profile_id);
CREATE INDEX IF NOT EXISTS idx_profile_field_key     ON profile_field(field_key);
-- Optional JSONB GIN index if needed:
-- CREATE INDEX IF NOT EXISTS idx_profile_field_value_gin ON profile_field USING gin (value jsonb_path_ops);

-- =========================
-- EVIDENCE  (TEXT IDs)  — with lifecycle & quality
-- =========================
CREATE TABLE evidence (
                          evidence_id      text PRIMARY KEY,
                          profile_field_id text NOT NULL REFERENCES profile_field(id) ON DELETE CASCADE,
                          uri              text NOT NULL,          -- link or storage URI
                          "type"           text,                   -- e.g., 'diagram','report','contract' (quoted: keyword)
    -- lifecycle
                          valid_from       timestamptz,
                          valid_until      timestamptz,
                          revoked_at       timestamptz,
    -- quality / provenance
                          confidence       text,                   -- 'system_verified' | 'human_verified' | 'self_attested'
                          method           text,                   -- 'auto' | 'manual' | 'attested' (or other)
                          sha256           text,
                          source_system    text,
                          evidence_status  text,                   -- optional: 'active' | 'superseded' | 'revoked'
    -- audit
                          added_at         timestamptz NOT NULL DEFAULT now(),
                          created_at       timestamptz NOT NULL DEFAULT now(),
                          updated_at       timestamptz NOT NULL DEFAULT now(),
                          CONSTRAINT uq_evidence_pf_uri UNIQUE (profile_field_id, uri),
                          CONSTRAINT ck_evidence_valid_window
                              CHECK (valid_until IS NULL OR valid_from IS NULL OR valid_until >= valid_from),
                          CONSTRAINT ck_evidence_confidence
                              CHECK (confidence IS NULL OR confidence IN ('system_verified','human_verified','self_attested')),
                          CONSTRAINT ck_evidence_method
                              CHECK (method IS NULL OR method IN ('auto','manual','attested'))
);


CREATE INDEX IF NOT EXISTS idx_evidence_pf           ON evidence(profile_field_id);
CREATE INDEX IF NOT EXISTS idx_evidence_revoked_at   ON evidence(revoked_at);
CREATE INDEX IF NOT EXISTS idx_evidence_valid_window ON evidence(valid_from, valid_until);
CREATE INDEX IF NOT EXISTS idx_evidence_confidence   ON evidence(confidence);
CREATE INDEX IF NOT EXISTS idx_evidence_method       ON evidence(method);
CREATE INDEX IF NOT EXISTS idx_evidence_sha256       ON evidence(sha256);
-- prevent duplicate uploads of the same blob per field
CREATE UNIQUE INDEX IF NOT EXISTS uq_evidence_pf_sha256
    ON evidence(profile_field_id, sha256)
    WHERE sha256 IS NOT NULL;

-- =========================
-- POLICY_REQUIREMENT  (TEXT IDs)
-- =========================
CREATE TABLE policy_requirement (
                                    requirement_id          text PRIMARY KEY,
                                    domain                  text NOT NULL,     -- e.g., 'security','resilience'
                                    policy_id               text NOT NULL,     -- logical policy identifier
                                    scope_type              text NOT NULL,     -- e.g., 'application'
                                    scope_id                text NOT NULL,     -- target entity id (polymorphic)
                                    release_id              text REFERENCES release(release_id) ON DELETE CASCADE,
                                    required_evidence_types text,              -- list/JSON of evidence types
                                    severity                text,              -- 'blocker','high','medium','low'
                                    due_date                timestamptz,
                                    policy_version_applied  text,
                                    created_at              timestamptz NOT NULL DEFAULT now(),
                                    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_requirement_scope    ON policy_requirement(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_requirement_release  ON policy_requirement(release_id);
CREATE INDEX IF NOT EXISTS idx_requirement_domain   ON policy_requirement(domain);

-- =========================
-- CONTROL_CLAIM  (TEXT IDs)
-- =========================
CREATE TABLE control_claim (
                               claim_id       text PRIMARY KEY,
                               requirement_id text REFERENCES policy_requirement(requirement_id) ON DELETE CASCADE,
                               scope_type     text NOT NULL,
                               scope_id       text NOT NULL,
                               release_id     text REFERENCES release(release_id) ON DELETE SET NULL,
                               method         text,               -- attestation/automation/test/etc.
                               status         text,               -- submitted/approved/rejected/in_review
                               submitted_at   timestamptz,
                               reviewed_at    timestamptz,
                               assigned_at    timestamptz,
                               comment        text,
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
                            domain      text NOT NULL,    -- e.g., 'security','data'
                            status      text NOT NULL,    -- e.g., 'open','mitigated','accepted','closed'
                            scope_type  text NOT NULL,
                            scope_id    text NOT NULL,
                            release_id  text REFERENCES release(release_id) ON DELETE SET NULL,
                            sla_due     timestamptz,
                            created_at  timestamptz NOT NULL DEFAULT now(),
                            updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_risk_scope    ON risk_story(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_risk_release  ON risk_story(release_id);

-- =========================
-- Trigger function: keep updated_at fresh
-- =========================
CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END$$;

-- Attach triggers (idempotent)
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

        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_evidence_updated_at') THEN
            CREATE TRIGGER trg_evidence_updated_at BEFORE UPDATE ON evidence
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
    END$$;

-- =========================
-- Helpful views
-- =========================

-- Latest profile per application
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

-- Flattened profile fields for quick querying
CREATE OR REPLACE VIEW v_profile_fields AS
SELECT
    p.scope_type,
    p.scope_id,
    p.version,
    f.field_key,
    f.value,
    f.source_system,
    f.source_ref,
    f.collected_at
FROM profile p
         JOIN profile_field f ON f.profile_id = p.profile_id;
