# Component-Engineer

## Excel to DB import format

Use the following column order in Excel/CSV before loading to the database:

1. `mpn`
2. `manufacture_part_number`
3. `manufacture`
4. `lifecycle`
5. `rohs`
6. `description`
7. `msd_level`
8. `datasheet_link`
9. `reflow_soldering_temperature`
10. `thermal_cycle`
11. `wave_soldering_temperature`
12. `lsl_details`
13. `package`
14. `price_details`

Reference files:
- `excel_import_template.csv` → header row to paste into Excel.
- `excel_import_schema.sql` → SQL table structure for storing the imported data.

## Large file processing (50k+ MPNs)

- Use **Large File Background Queue (Resume Supported)** in the Scraper tab.
- Upload full file (`col1=MPN`, `col2=Manufacturer/Make` optional), then click **Add file to queue**.
- Click **Run next batch** to process in chunks (e.g., 200/500/1000).  
- The app stores queue status and checkpoint (`last processed MPN`) in DB, so after restart you can continue from remaining pending rows.
- Use **Live Queue Viewer** to monitor current progress and latest processed rows.

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
- `build_windows_exe.bat` bundles `logo.png` and `components.db`.
- Startup task is created with name `ComponentEngineerAutoStart` and trigger `ONLOGON`.
