import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "components.db"

st.set_page_config(layout="wide", page_title="Component Engineer Dashboard")


# --- DB helpers ---

def get_db_connection():
    if not DB_PATH.exists():
        st.error(
            f"Database file not found: {DB_PATH}\n\n"
            "Run html_to_sqlite.py in the SAME folder as this script."
        )
        st.stop()
    return sqlite3.connect(DB_PATH)


def run_query(query, params=None):
    conn = get_db_connection()
    try:
        if params is None:
            df = pd.read_sql_query(query, conn)
        else:
            df = pd.read_sql_query(query, conn, params=params)
        return df
    finally:
        conn.close()


def get_part_numbers():
    df = run_query("SELECT DISTINCT mpn FROM tables ORDER BY mpn;")
    return df["mpn"].tolist() if not df.empty else []


def get_sections(mpn):
    q = """
        SELECT DISTINCT section_name
        FROM sections
        WHERE mpn = ?
        ORDER BY section_order;
    """
    df = run_query(q, (mpn,))
    return df["section_name"].tolist() if not df.empty else []


def get_tables(mpn, section_name):
    q = """
        SELECT id, title, table_index
        FROM tables
        WHERE mpn = ? AND section_name = ?
        ORDER BY table_index;
    """
    df = run_query(q, (mpn, section_name))
    return df if not df.empty else pd.DataFrame(columns=["id", "title", "table_index"])


def get_cell_data(table_id):
    q = """
        SELECT row_index, col_index, header, value
        FROM cells
        WHERE table_id = ?
        ORDER BY row_index, col_index;
    """
    return run_query(q, (table_id,))


def pivot_data(df_cells: pd.DataFrame) -> pd.DataFrame:
    if df_cells.empty:
        return df_cells

    kv_df = df_cells.copy()

    # If each row_index is unique and header is mostly filled,
    # treat as simple key/value list
    if kv_df["row_index"].nunique() == len(kv_df):
        try:
            out = (
                kv_df
                .set_index("header")["value"]
                .to_frame(name="Value")
            )
            out.index.name = "Attribute"
            return out.reset_index()
        except Exception:
            pass

    # General pivot
    try:
        pt = kv_df.pivot_table(
            index="row_index",
            columns="header",
            values="value",
            aggfunc="first",
        )
        pt.index.name = None
        pt = pt.reset_index(drop=True)

        # If single column but many rows, transpose for nicer view
        if pt.shape[1] == 1 and pt.shape[0] > 1:
            pt = pt.T
            pt.columns.name = None
        return pt
    except Exception:
        # Fallback: raw-ish view
        return (
            df_cells[["header", "value", "row_index"]]
            .rename(columns={
                "header": "Attribute",
                "value": "Value",
                "row_index": "Row ID",
            })
        )


# --- UI ---

st.title("Z2Data Component Dashboard")
st.markdown("---")

st.sidebar.header("Data Selection")
st.sidebar.caption(f"Using DB: `{DB_PATH}`")

mpns = get_part_numbers()
st.sidebar.write(f"📦 Parts found in DB: {len(mpns)}")

if not mpns:
    st.sidebar.error(
        "No parts found in the database.\n\n"
        "1. Run SS.PY to generate HTML files into `output_html/`\n"
        "2. Run html_to_sqlite.py in this folder\n"
        "3. Refresh this page."
    )
    st.stop()

selected_mpn = st.sidebar.selectbox("Select Part (MPN)", mpns)

if selected_mpn:
    sections = get_sections(selected_mpn)
    st.sidebar.write(f"📑 Sections for this MPN: {len(sections)}")
    selected_section = st.sidebar.selectbox("Section", sections)

    if selected_section:
        table_df = get_tables(selected_mpn, selected_section)
        st.sidebar.write(f"📊 Tables in this section: {len(table_df)}")

        if not table_df.empty:
            # Use DF index as the option, to avoid title-duplication problems
            def format_table_option(idx):
                row = table_df.loc[idx]
                # Example: "1. Part Number | Company | Lifecycle | Source | Last Check Date"
                return f"{int(row['table_index']) + 1}. {row['title']}"

            selected_idx = st.sidebar.selectbox(
                "Select Table",
                options=table_df.index.tolist(),
                format_func=format_table_option,
            )

            selected_table_id = int(table_df.loc[selected_idx, "id"])
            selected_title = table_df.loc[selected_idx, "title"]

            # --- Main content ---
            st.header(f"Data for: {selected_section}")
            st.subheader(selected_title)

            df_cells = get_cell_data(selected_table_id)

            if df_cells.empty:
                st.info(
                    "No cell data found for this table.\n"
                    "Try selecting a different table in the dropdown."
                )
            else:
                # 1) Raw
                st.markdown("### 1. Raw Extracted Data (Cell View)")
                st.markdown(
                    """
                    <div style="background-color:#f0f2f6;padding:8px;border-radius:5px;">
                    This shows the raw Key (Header) and Value pairs extracted by the parser.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                st.dataframe(
                    df_cells[["header", "value", "row_index"]].reset_index(drop=True),
                    use_container_width=True,
                )

                # 2) Pivoted
                st.markdown("### 2. Final Pivoted Table View")
                st.markdown(
                    """
                    <div style="background-color:#e6f7ff;padding:8px;border-radius:5px;">
                    This is the clean table created by pivoting the raw data.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                df_pivoted = pivot_data(df_cells)
                st.dataframe(df_pivoted, use_container_width=True)
        else:
            st.info(f"No tables found for section '{selected_section}'.")
