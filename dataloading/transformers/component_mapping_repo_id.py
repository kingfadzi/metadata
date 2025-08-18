import re
import logging

# Setup standard logger
logger = logging.getLogger("GitCoordExtractor")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s in %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Patterns with optional `.git` and optional trailing slash
PATTERNS = [
    # Bitbucket Browse (e.g., http://host:7990/projects/PRJ/repos/my-repo/browse)
    re.compile(r'^https?://[^/]+/projects/(?P<project>[^/]+)/repos/(?P<slug>[^/]+)/browse/?$'),

    # Bitbucket SCM (e.g., http://host/scm/PRJ/my-repo.git)
    re.compile(r'^https?://[^/]+/scm/(?P<project>[^/]+)/(?P<slug>[^/]+?)(?:\.git)?/?$'),

    # GitHub/GitLab Flat (e.g., http://host/org/repo.git)
    re.compile(r'^https?://[^/]+/(?P<project>[^/]+)/(?P<slug>[^/]+?)(?:\.git)?/?$'),

    # GitLab Nested (e.g., http://gitlab/org/group1/group2/slug/-/settings)
    re.compile(r'^https?://[^/]+/(?P<group_repo_path>.+?)(?=(?:\.git|/-/|$))')
]


def extract_git_coordinates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["repo_id"] = None

        if row.get("mapping_type") != "version_control":
            logger.debug(f"Skipping row (not version_control): {row.get('mapping_type')}")
            continue

        url = (row.get("web_url") or "").strip()
        logger.debug(f"Processing URL: {url}")

        for pattern in PATTERNS:
            match = pattern.match(url)
            if not match:
                continue

            groups = match.groupdict()

            if "project" in groups and "slug" in groups:
                row["repo_id"] = f"{groups['project']}/{groups['slug']}"
                logger.debug(f"Matched project/slug → repo_id: {row['repo_id']}")
            elif "group_repo_path" in groups:
                row["repo_id"] = groups["group_repo_path"]
                logger.debug(f"Matched group_repo_path → repo_id: {row['repo_id']}")
            break
        else:
            logger.warning(f"No pattern matched for URL: {url}")

    return rows
