# Component-Engineer

## Excel to DB import format

## Initialize local database

Run this once in the project folder to create `components.db` with the required tables:

```bash
python init_components_db.py
```

`components.db` is intentionally not committed as a binary file; generate it locally with the command above.

Use the following template header in Excel/CSV before loading to the database (`excel_import_template.csv`):

1. `mpn`
2. `manufacturer`
3. `manufacturer_part_number`
4. `supplier_part_number`
5. `rohs`
6. `description`
7. `category`
8. `lifecycle_status`
9. `stock`
10. `datasheet_url`
11. `product_url`
12. `msd_level`
13. `reflow_soldering_temperature`
14. `thermal_cycle`
15. `wave_soldering_temperature`
16. `lsl_details`
17. `package_details`
18. `price_details`
19. `operating_temperature`
20. `component_thickness`
21. `reach`
22. `reflow_soldering_time` (Reflow solder time)
23. `wave_soldering_time` (Wave solder time)
24. `body_mark`

Reference files:
- `excel_import_template.csv` → header row to paste into Excel.
- `excel_import_schema.sql` → SQL table structure for storing the imported data.

## Large file processing (50k+ MPNs)

- Use **Large File Background Queue (Resume Supported)** in the Scraper tab.
- Upload full file (`col1=MPN`, `col2=Manufacturer/Make` optional), then click **Add file to queue**.
- Click **Run queue now (process all)** to drain all pending rows in the queue.  
- The app stores queue status and checkpoint (`last processed MPN`) in DB, so after restart you can continue from remaining pending rows.
- Use **Live Queue Viewer** to monitor current progress and latest processed rows.
- Use **Process History (step-wise)** to review every queue event (`queue_add`, `process_start`, `fetch_warning`, `process_done`, `process_error`) and export it as CSV for debugging.

## Manual data entry

- Use **Manual Data Entry (Direct to Unified DB)** in Advanced Master Export tab to save values directly for one MPN.
- Includes all export fields plus `OPERATING TEMPERATURE`.

## Build Windows EXE + startup task

1. Install build dependencies:
   - `pip install -r packages.txt pyinstaller pillow`
2. Create app icon from `logo.png`:
   - `python make_icon.py`
3. Build EXE:
   - `build_windows_exe.bat`
   - Output: `dist\ComponentEngineer.exe`
4. Register OS startup task (run as admin command prompt):
   - `create_startup_task.bat "C:\full\path\to\dist\ComponentEngineer.exe"`

Notes:
- `build_windows_exe.bat` bundles `logo.png` and `components.db` (generate `components.db` first with `python init_components_db.py`).
- Startup task is created with name `ComponentEngineerAutoStart` and trigger `ONLOGON`.
