ALTER TABLE public.component_mapping ADD COLUMN repo_id text;

CREATE INDEX idx_component_mapping_repo_id
    ON public.component_mapping (repo_id);
