import asyncio
import yaml
import aioodbc
import asyncpg
import re
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Union, Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from filelock import FileLock
import importlib
import hashlib

CHUNK_SIZE = 500
CONFIG_PATH = "metadata/table_mapping.yaml"
OFFSET_FILE = Path("migration_checkpoints.json")

# === Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")

# === Configuration Handling ===
def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def build_sqlserver_conn_str(dsn_or_conn: str, extra: Optional[str]) -> str:
    """Append extra ODBC options safely to a DSN or full conn string."""
    if not extra:
        return dsn_or_conn
    sep = "" if dsn_or_conn.endswith(";") else ";"
    return f"{dsn_or_conn}{sep}{extra}"

def redact_conn_str(conn: str) -> str:
    """Redact secrets in a connection string for logging."""
    s = conn
    s = re.sub(r"(?i)(Password|Pwd)\s*=\s*[^;]*", r"\1=****", s)
    s = re.sub(r"(?i)(UID|User\s*ID)\s*=\s*[^;]*", r"\1=****", s)
    s = re.sub(r"(?i)(AccessToken)\s*=\s*[^;]*", r"\1=****", s)
    return s

# === Checkpoint Management (KEYED) ===
def _ckpt_key(src_conn: str, dst_conn: str, src_table: str, dst_table: str) -> str:
    h = hashlib.sha1((src_conn + "→" + dst_conn).encode()).hexdigest()[:8]
    return f"{src_table}->{dst_table}#{h}"

def _read_ckpt_file() -> Dict[str, Any]:
    with FileLock(str(OFFSET_FILE) + ".lock"):
        if not OFFSET_FILE.exists():
            return {}
        try:
            return json.loads(OFFSET_FILE.read_text())
        except json.JSONDecodeError:
            return {}

def _write_ckpt_file(data: Dict[str, Any]) -> None:
    with FileLock(str(OFFSET_FILE) + ".lock"):
        temp = OFFSET_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(data))
        temp.replace(OFFSET_FILE)

def read_checkpoint_keyed(src_conn: str, dst_conn: str, src_table: str, dst_table: str) -> int:
    key = _ckpt_key(src_conn, dst_conn, src_table, dst_table)
    data = _read_ckpt_file()
    return int(data.get(key, 0))

def write_checkpoint_keyed(src_conn: str, dst_conn: str, src_table: str, dst_table: str, offset: int) -> None:
    key = _ckpt_key(src_conn, dst_conn, src_table, dst_table)
    data = _read_ckpt_file()
    data[key] = int(offset)
    _write_ckpt_file(data)

# === Helpers ===
def normalize_col(name: str) -> str:
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name.strip()).lower()
    return re.sub(r'_+', '_', name).strip('_')[:63]

def sqlserver_to_postgres_type(sql_type: str, max_len: Union[int, None]) -> str:
    t = sql_type.lower()
    if t in ("varchar", "nvarchar", "char", "nchar"):
        return f"varchar({max_len})" if max_len and max_len > 0 else "text"
    return {
        "int": "integer", "bigint": "bigint", "smallint": "smallint", "bit": "boolean",
        "decimal": "numeric", "numeric": "numeric", "float": "double precision",
        "real": "real", "uniqueidentifier": "uuid", "varbinary": "bytea",
        "datetime": "timestamptz", "datetime2": "timestamptz",
        "date": "date", "time": "time", "smalldatetime": "timestamptz"
    }.get(t, "text")

def _esc(idn: str) -> str:
    # SQL Server identifier escaping: [name], escape ']' as ']]'
    return f"[{idn.replace(']', ']]')}]"

def _build_base_sql(
        table: str,
        sort_columns: Optional[List[str]],
        where: Optional[str],
        selected_columns: Optional[List[str]],
        group_by: Optional[List[str]],
        row_limit: Optional[int],
        uses_paging_out: List[bool],
) -> str:
    """
    Build the base SQL (parameterized when paging). Mutates uses_paging_out[0] to True/False.
    Rules:
      - SELECT * only when selected_columns is not provided.
      - GROUP BY requires explicit columns (no SELECT *).
      - If sort_columns is missing/empty -> no ORDER BY/OFFSET; if row_limit is set, inject TOP (row_limit).
      - If sort_columns present -> ORDER BY and OFFSET ? FETCH NEXT ? (limit is provided at runtime).
    """
    schema, name = table.split(".", 1)
    esc_schema = _esc(schema)
    esc_table  = _esc(name)

    # SELECT list (with optional TOP when not paging)
    top_clause = ""
    uses_paging = bool(sort_columns and len(sort_columns) > 0)

    if not uses_paging and row_limit and row_limit > 0:
        top_clause = f"TOP ({int(row_limit)}) "

    if selected_columns and len(selected_columns) > 0:
        select_list = ", ".join([_esc(c) for c in selected_columns])
    else:
        select_list = "*"

    # GROUP BY
    group_clause = ""
    if group_by and len(group_by) > 0:
        if not selected_columns:
            raise ValueError("GROUP BY requires explicit 'columns' in config; SELECT * is invalid with GROUP BY.")
        esc_group = ", ".join([_esc(c) for c in group_by])
        group_clause = f" GROUP BY {esc_group}"

    # ORDER BY + paging
    order_clause = ""
    paging_clause = ""
    if uses_paging:
        esc_order = ", ".join([_esc(c) for c in sort_columns])
        order_clause = f" ORDER BY {esc_order}"
        paging_clause = " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    else:
        logger.warning(
            "No sort_columns defined for %s — running without ORDER BY and without paging. "
            "Result set order is non-deterministic; checkpoint will be ignored.",
            table,
        )

    sql = f"""
        SELECT {top_clause}{select_list}
        FROM {esc_schema}.{esc_table}
        {f'WHERE {where}' if where else ''}
        {group_clause}
        {order_clause}
        {paging_clause}
    """
    uses_paging_out[0] = uses_paging
    return re.sub(r"\s+", " ", sql).strip()

# === Schema Handling ===
async def extract_sqlserver_schema(conn_str: str, table: str, selected_columns: Optional[List[str]] = None) -> str:
    schema, name = table.split(".")
    conn = await aioodbc.connect(dsn=conn_str, autocommit=True)
    cursor = await conn.cursor()
    try:
        await cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (schema, name))
        rows = await cursor.fetchall()

        if selected_columns and len(selected_columns) > 0:
            want = [c.strip() for c in selected_columns]
            want_lc_index = {c.lower(): i for i, c in enumerate(want)}
            rows = [r for r in rows if r[0].lower() in want_lc_index]
            rows.sort(key=lambda r: want_lc_index[r[0].lower()])

        ddl_lines = []
        for col_name, col_type, nullable, char_max, _pos in rows:
            norm_col = normalize_col(col_name)
            pg_type = sqlserver_to_postgres_type(col_type, char_max)
            line = f'"{norm_col}" {pg_type}' + (" NOT NULL" if nullable == "NO" else "")
            ddl_lines.append(line)

        target_schema = normalize_col(schema)
        target_name = normalize_col(name)
        return f'CREATE TABLE "{target_schema}"."{target_name}" (\n  ' + ",\n  ".join(ddl_lines) + "\n);"
    finally:
        await cursor.close()
        await conn.close()

async def recreate_pg_table(conn_str: str, ddl: str, target_table: str):
    target_schema, target_name = target_table.split(".", 1)
    norm_schema = normalize_col(target_schema)
    norm_name = normalize_col(target_name)
    fqn = f'"{norm_schema}"."{norm_name}"'
    m = re.search(r"\(.*\)", ddl, flags=re.DOTALL)
    if not m:
        raise ValueError("Could not find column list in DDL:\n" + ddl[:200])
    fixed_ddl = f"CREATE TABLE {fqn} {m.group(0)}"
    logger.info("Recreate DDL:\n%s", fixed_ddl)
    conn = await asyncpg.connect(conn_str)
    try:
        async with conn.transaction():
            await conn.execute(f"DROP TABLE IF EXISTS {fqn} CASCADE")
            await conn.execute(fixed_ddl)
    finally:
        await conn.close()

# === Data Fetch/Insert ===
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=15, min=15, max=120),
    retry=retry_if_exception_type(Exception)
)
async def fetch_chunk_sqlserver(conn_str: str,
                                sql_base: str,
                                params: Optional[tuple],
                                dsn_extra: Optional[str] = None,
                                inline_paging: bool = False,
                                offset: Optional[int] = None,
                                limit: Optional[int] = None,
                                sql_timeout: Optional[int] = None) -> List[dict]:
    """
    Execute the SQL. If inline_paging is True, replace 'OFFSET ? ROWS FETCH NEXT ? ROWS ONLY'
    with literal integers (avoids some ODBC quirks). When inlined, params must be None.
    Applies per-statement timeout if provided (cursor.timeout).
    """
    sql_to_run = sql_base
    exec_params = params

    if inline_paging:
        if offset is None or limit is None:
            raise ValueError("inline_paging=True requires offset and limit")
        sql_to_run = sql_base.replace(
            "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            f"OFFSET {int(offset)} ROWS FETCH NEXT {int(limit)} ROWS ONLY"
        )
        exec_params = None

    full_conn = build_sqlserver_conn_str(conn_str, dsn_extra)

    async with aioodbc.connect(dsn=full_conn, autocommit=True) as conn:
        async with conn.cursor() as cur:
            if sql_timeout and sql_timeout > 0:
                try:
                    # aioodbc exposes pyodbc cursor underneath; set per-statement timeout (seconds)
                    cur.timeout = int(sql_timeout)
                except Exception:
                    logger.warning("Could not set cursor timeout to %s seconds; driver may not support it.", sql_timeout)
            if exec_params:
                await cur.execute(sql_to_run, exec_params)
            else:
                await cur.execute(sql_to_run)
            if not cur.description:
                return []
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) async for row in cur]

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=10, min=10, max=60),
    retry=retry_if_exception_type(Exception)
)
async def insert_chunk_pg(conn_str: str, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    schema, name = table.split(".", 1)
    raw_cols = list(rows[0].keys())
    norm_cols = [normalize_col(c) for c in raw_cols]
    conn = await asyncpg.connect(conn_str)
    try:
        await conn.copy_records_to_table(
            table_name=normalize_col(name),
            schema_name=normalize_col(schema),
            records=[tuple(row[col] for col in raw_cols) for row in rows],
            columns=norm_cols
        )
        return len(rows)
    finally:
        await conn.close()

def apply_plugin_chain(rows: List[dict], plugin_paths: List[str]) -> List[dict]:
    for plugin_path in plugin_paths:
        mod_name, func_name = plugin_path.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        rows = func(rows)
    return rows

# === Orchestration ===
async def migrate_table(src_conn: str, dst_conn: str, src_table: str, dst_table: str,
                        table_cfg: dict, where: str = None):
    logger.info(f"Starting migration: {src_table} → {dst_table}")

    selected_cols = table_cfg.get("columns")              # optional explicit select
    group_by      = table_cfg.get("group_by")             # optional
    sort_columns  = table_cfg.get("sort_columns")         # optional; controls paging
    odbc_extra    = table_cfg.get("odbc_extra")           # optional; ODBC 17 options
    inline_paging = bool(table_cfg.get("inline_paging", False))
    sql_timeout   = table_cfg.get("sql_timeout")          # optional; seconds (e.g., 300)
    row_limit     = table_cfg.get("row_limit")            # optional; cap total rows (e.g., 1000000)

    # Build & log base SQL once
    uses_paging_box = [False]
    sql_base = _build_base_sql(
        table=src_table,
        sort_columns=sort_columns,
        where=where,
        selected_columns=selected_cols,
        group_by=group_by,
        row_limit=row_limit if row_limit and row_limit > 0 else None,
        uses_paging_out=uses_paging_box,
    )
    uses_paging = uses_paging_box[0]

    effective_conn = build_sqlserver_conn_str(src_conn, odbc_extra)
    logger.info("ODBC extras for %s: %s", src_table, odbc_extra if odbc_extra else "<none>")
    logger.info("inline_paging for %s: %s", src_table, inline_paging)
    logger.info("sql_timeout for %s: %s", src_table, sql_timeout if sql_timeout else "<none>")
    logger.info("row_limit for %s: %s", src_table, row_limit if row_limit else "<none>")
    logger.info("Paging enabled for %s: %s (CHUNK_SIZE=%s)", src_table, uses_paging, CHUNK_SIZE if uses_paging else "n/a")
    logger.info("Effective SQL Server DSN for %s: %s", src_table, redact_conn_str(effective_conn))
    logger.info("SQL Server base query for %s:\n%s", src_table, sql_base)

    # Read keyed checkpoint (only meaningful if paging)
    offset = read_checkpoint_keyed(src_conn, dst_conn, src_table, dst_table)
    recreate = table_cfg.get("recreate", True)

    if not uses_paging and offset > 0:
        logger.warning("Checkpoint offset %s for %s will be ignored because paging is disabled.", offset, src_table)

    # Only (re)create target if starting fresh OR explicitly requested
    if offset == 0 and recreate:
        try:
            ddl = await extract_sqlserver_schema(src_conn, src_table, selected_columns=selected_cols)
            await recreate_pg_table(dst_conn, ddl, dst_table)
            post_file = table_cfg.get("post_ddl_file")
            if post_file:
                post_sql = Path(post_file).read_text()
                conn = await asyncpg.connect(dst_conn)
                try:
                    await conn.execute(post_sql)
                finally:
                    await conn.close()
        except Exception as e:
            logger.error(f"DDL error: {e}")
            raise
    else:
        logger.info(f"Resuming {src_table} at offset {offset} without dropping {dst_table}")

    total = 0

    if not uses_paging:
        # Single-shot fetch (no ORDER BY / no OFFSET-FETCH); TOP(row_limit) applied in base SQL if provided
        try:
            rows = await fetch_chunk_sqlserver(
                src_conn, sql_base, params=None, dsn_extra=odbc_extra, sql_timeout=sql_timeout
            )
            if rows:
                plugins = table_cfg.get("transforms", {}).get("plugins", [])
                if plugins:
                    rows = apply_plugin_chain(rows, plugins)
                inserted = await insert_chunk_pg(dst_conn, dst_table, rows)
                total += inserted
                logger.info("Copied %s rows (no paging).", inserted)
        except Exception as e:
            logger.error("Error in non-paged fetch for %s: %s", src_table, e)
            raise
    else:
        # Paged loop honoring row_limit
        while True:
            # Respect row_limit by shrinking the requested page
            page_size = CHUNK_SIZE
            if row_limit and row_limit > 0:
                remaining = max(0, int(row_limit) - offset)
                if remaining <= 0:
                    logger.info("Reached row_limit (%s); stopping.", row_limit)
                    break
                page_size = min(page_size, remaining)

            try:
                rows = await fetch_chunk_sqlserver(
                    src_conn,
                    sql_base,
                    params=(offset, page_size) if not inline_paging else None,
                    dsn_extra=odbc_extra,
                    inline_paging=inline_paging,
                    offset=offset,
                    limit=page_size,
                    sql_timeout=sql_timeout,
                )
                if not rows:
                    break

                plugins = table_cfg.get("transforms", {}).get("plugins", [])
                if plugins:
                    rows = apply_plugin_chain(rows, plugins)

                inserted = await insert_chunk_pg(dst_conn, dst_table, rows)
                total += inserted

                offset += inserted
                write_checkpoint_keyed(src_conn, dst_conn, src_table, dst_table, offset)

                logger.info("Copied %s rows (offset now %s, total %s)", inserted, offset, total)

            except Exception as e:
                logger.error("Error at offset %s for %s: %s", offset, src_table, e)
                raise

    # Completed
    write_checkpoint_keyed(src_conn, dst_conn, src_table, dst_table, 0)
    logger.info(f"Finished migration: {total} rows")

async def main_migration():
    config = load_config()
    for db_group in config.get("databases", []):
        if not db_group.get("enabled", True):
            continue
        tasks = []
        for table_cfg in db_group.get("tables", []):
            if not table_cfg.get("enabled", True):
                continue
            tasks.append(
                migrate_table(
                    db_group["source_db"],
                    db_group["target_db"],
                    table_cfg["source"],
                    table_cfg["dest"],
                    table_cfg,
                    where=table_cfg.get("where")
                )
            )
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main_migration())
