import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "components.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mpn TEXT NOT NULL,
            section_name TEXT NOT NULL,
            section_order INTEGER NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mpn TEXT NOT NULL,
            section_name TEXT NOT NULL,
            title TEXT NOT NULL,
            table_index INTEGER NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cells (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id INTEGER NOT NULL,
            row_index INTEGER NOT NULL,
            col_index INTEGER NOT NULL,
            header TEXT,
            value TEXT,
            FOREIGN KEY(table_id) REFERENCES tables(id) ON DELETE CASCADE
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS unified_part_cache (
            mpn TEXT PRIMARY KEY,
            manufacturer TEXT,
            manufacturer_part_number TEXT,
            supplier_part_number TEXT,
            description TEXT,
            category TEXT,
            lifecycle_status TEXT,
            rohs TEXT,
            stock TEXT,
            datasheet_url TEXT,
            product_url TEXT,
            msd_level TEXT,
            reflow_soldering_temperature TEXT,
            thermal_cycle TEXT,
            wave_soldering_temperature TEXT,
            lsl_details TEXT,
            package_details TEXT,
            price_details TEXT,
            operating_temperature TEXT,
            component_thickness TEXT,
            reach TEXT,
            reflow_soldering_time TEXT,
            wave_soldering_time TEXT,
            body_mark TEXT,
            source_trace TEXT,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS live_part_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mpn TEXT NOT NULL,
            selected_source TEXT NOT NULL,
            fetched_at_utc TEXT NOT NULL,
            lifecycle_status TEXT,
            stock_details TEXT,
            manufacturer TEXT,
            description TEXT,
            data_json TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scrub_queue (
            mpn TEXT PRIMARY KEY,
            manufacturer TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            source TEXT,
            last_error TEXT,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scrub_queue_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_mpn TEXT,
            last_status TEXT,
            processed_count INTEGER NOT NULL DEFAULT 0,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        INSERT OR IGNORE INTO scrub_queue_state (id, last_mpn, last_status, processed_count, updated_at_utc)
        VALUES (1, '', '', 0, datetime('now'));
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS z2_spec_cache (
            mpn TEXT PRIMARY KEY,
            description TEXT,
            msl TEXT,
            reflow_temt TEXT,
            thermal_cycle TEXT,
            wave TEXT,
            package_details TEXT,
            operating_temperature TEXT,
            lifecycle TEXT,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS z2_parametric_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mpn TEXT NOT NULL,
            section_name TEXT,
            table_title TEXT,
            row_index INTEGER,
            header TEXT,
            value TEXT,
            updated_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )

    conn.commit()
    conn.close()
    print(f"Database initialized at: {DB_PATH}")


if __name__ == "__main__":
    main()
