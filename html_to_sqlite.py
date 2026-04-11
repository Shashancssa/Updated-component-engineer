import re
import sqlite3
from pathlib import Path

from bs4 import BeautifulSoup  # pip install beautifulsoup4 lxml

BASE_DIR = Path(__file__).resolve().parent
HTML_DIR = BASE_DIR / "output_html"
DB_PATH = BASE_DIR / "components.db"


# =========================
#  DB SETUP
# =========================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Rebuild fresh each time
    cur.execute("DROP TABLE IF EXISTS cells;")
    cur.execute("DROP TABLE IF EXISTS tables;")
    cur.execute("DROP TABLE IF EXISTS sections;")

    cur.execute("""
        CREATE TABLE sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mpn TEXT NOT NULL,
            section_name TEXT NOT NULL,
            section_order INTEGER NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mpn TEXT NOT NULL,
            section_name TEXT NOT NULL,
            title TEXT NOT NULL,
            table_index INTEGER NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE cells (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id INTEGER NOT NULL,
            row_index INTEGER NOT NULL,
            col_index INTEGER NOT NULL,
            header TEXT,
            value TEXT,
            FOREIGN KEY(table_id) REFERENCES tables(id) ON DELETE CASCADE
        );
    """)

    conn.commit()
    return conn


# =========================
#  HTML HELPERS
# =========================

def extract_mpn(soup: BeautifulSoup) -> str | None:
    """
    Read MPN from the <h1> line like:
        PART: 39-30-1080
    """
    h1 = soup.find("h1")
    if not h1:
        return None

    text = h1.get_text(strip=True)
    m = re.search(r"PART:\s*(.+)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return text or None


def collect_section_tables(section_h2):
    """
    Collect ALL <table> tags that belong to this <h2> section.

    Walk depth-first from this <h2> until the next <h1>/<h2>
    and grab every <table> encountered, even if nested in <div>s.
    """
    next_section = section_h2.find_next(
        lambda tag: tag.name in ("h1", "h2") and tag is not section_h2
    )

    tables = []
    el = section_h2.next_element
    while el and el is not next_section:
        if getattr(el, "name", None) == "table":
            tables.append(el)
        el = el.next_element
    return tables


def infer_table_title(table_tag, section_name: str) -> str:
    """
    Try to give each table a meaningful title, so we can distinguish:
      - Lifecycle summary vs. a small text table
      - Parametric main grid vs. other info tables, etc.
    """
    # 1) If <caption> exists, use it
    if table_tag.caption:
        cap = table_tag.caption.get_text(strip=True)
        if cap:
            return cap

    # 2) Look at first row and its <th> cells (header row)
    first_row = None
    thead = table_tag.find("thead")
    if thead:
        trs = thead.find_all("tr", recursive=False)
        if trs:
            first_row = trs[0]
    if not first_row:
        # fall back to first <tr> anywhere
        trs = table_tag.find_all("tr", recursive=False)
        if trs:
            first_row = trs[0]

    if first_row:
        ths = first_row.find_all("th", recursive=False)
        header_texts = [th.get_text(strip=True) for th in ths if th.get_text(strip=True)]
        if header_texts:
            # Example: "Part Number | Company | Lifecycle | Source"
            joined = " | ".join(header_texts[:4])
            return joined

        # No <th>; maybe first cell is key (e.g. "MSL", "Lead Finish")
        tds = first_row.find_all("td", recursive=False)
        if tds:
            first_key = tds[0].get_text(strip=True)
            if first_key:
                return f"{section_name} — {first_key[:40]}"

    # 3) Fallback
    return section_name


def write_table_cells(cur, table_tag, table_id: int):
    """
    Normalize an HTML <table> into rows in 'cells'.

    Handles:

    1) Normal header tables:
         first row (or thead) has <th>
         -> store each cell with header taken from that row.

    2) Key–Value(/Link) tables (Manufacturing, Part Options, etc.):
         no header row, but rows have >= 2 cells
         -> col0 => header (key), col1 => value, rest ignored.
    """
    rows = []

    thead = table_tag.find("thead")
    if thead:
        rows.extend(thead.find_all("tr", recursive=False))

    tbody = table_tag.find("tbody")
    if tbody:
        rows.extend(tbody.find_all("tr", recursive=False))
    else:
        rows.extend(table_tag.find_all("tr", recursive=False))

    if not rows:
        return

    first_row = rows[0]
    first_cells = first_row.find_all(["th", "td"], recursive=False)
    header_row_used = bool(first_row.find("th"))

    headers = []
    data_start_idx = 0

    if header_row_used:
        for c in first_cells:
            headers.append(c.get_text(strip=True))
        data_start_idx = 1

    row_index = 0
    for r in rows[data_start_idx:]:
        cells = r.find_all(["td", "th"], recursive=False)

        # ---- Case 2: Key–Value(/Link) table ----
        if not header_row_used and len(cells) >= 2:
            key = cells[0].get_text(strip=True)
            val = cells[1].get_text(strip=True)

            if key or val:
                cur.execute(
                    """
                    INSERT INTO cells (table_id, row_index, col_index, header, value)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (table_id, row_index, 0, key, val),
                )

        # ---- Case 1: Header table ----
        else:
            for col_index, c in enumerate(cells):
                header = headers[col_index] if col_index < len(headers) else None
                val = c.get_text(strip=True)
                if not (header or val):
                    continue

                cur.execute(
                    """
                    INSERT INTO cells (table_id, row_index, col_index, header, value)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (table_id, row_index, col_index, header, val),
                )

        row_index += 1


def parse_html_content(html_text: str, conn: sqlite3.Connection, source_name: str = "<memory>"):
    soup = BeautifulSoup(html_text, "lxml")

    mpn = extract_mpn(soup)
    if not mpn:
        print(f"[WARN] {source_name}: could not find MPN, skipping.")
        return

    cur = conn.cursor()

    section_order = 0
    for h2 in soup.find_all("h2"):
        section_name = h2.get_text(strip=True)

        cur.execute(
            """
            INSERT INTO sections (mpn, section_name, section_order)
            VALUES (?, ?, ?)
            """,
            (mpn, section_name, section_order),
        )
        section_order += 1

        section_tables = collect_section_tables(h2)

        table_index = 0
        for tbl in section_tables:
            title = infer_table_title(tbl, section_name)

            cur.execute(
                """
                INSERT INTO tables (mpn, section_name, title, table_index)
                VALUES (?, ?, ?, ?)
                """,
                (mpn, section_name, title, table_index),
            )
            table_id = cur.lastrowid
            table_index += 1

            write_table_cells(cur, tbl, table_id)

    conn.commit()
    print(f"[OK] Parsed {source_name} for MPN {mpn}")


def parse_html_file(path: Path, conn: sqlite3.Connection):
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        parse_html_content(f.read(), conn, source_name=path.name)


# =========================
#  MAIN
# =========================

def main():
    print(f"HTML directory: {HTML_DIR}")
    print(f"Database path : {DB_PATH}")

    conn = init_db()
    html_files = sorted(HTML_DIR.glob("*.html"))

    if not html_files:
        print("[ERROR] No .html files found in output_html")
        conn.close()
        return

    for html_path in html_files:
        parse_html_file(html_path, conn)

    conn.close()
    print("✅ Done. components.db rebuilt.")


if __name__ == "__main__":
    main()
