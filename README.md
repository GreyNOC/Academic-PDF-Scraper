# GreyNOC PDF Scraper

Windows GUI app for collecting higher-quality open-access PDFs from public academic sources.

Default output folder is configured inside the app and can be changed from the GUI.

## New in v2

- **PubMed Central source** — adds NCBI's PMC open-access subset alongside OpenAlex and arXiv.
- **Statistics & History tab** — see total downloads, today's count, breakdowns by source / keyword / date, and a sortable list of recent downloads. Export the full history to CSV.
- **Duplicate scanner** — hash every PDF in the output folder (SHA-256), find byte-identical duplicates, and remove extras while keeping the oldest copy.
- Per-source enable/disable checkboxes (OpenAlex, arXiv, PubMed Central).
- Notebook UI: Scraper / Statistics & History / Duplicate Scanner tabs.

## Existing features

- GUI with keyword/subject input.
- Supports one or multiple comma-separated keywords.
- Continuous 24/7 mode or one-shot run.
- Max 100 PDF downloads per day; max 100 MB per day.
- Skips files already in output folder or in the database.
- Validates PDFs using `%PDF-` magic bytes.
- Quality scoring before download; filters low-value PDFs.
- Auto-rotation across keyword profiles.
- Create / edit / delete keyword profiles in the GUI; persisted to `keyword_profiles.json`.

## Run from source

Double-click `run_greynoc_scraper.bat`.

## Stop the scraper

Double-click `stop_greynoc_scraper.bat`.

## Build EXE

Double-click `compile_exe.bat`. Output: `dist\GreyNOCPDFScraper\GreyNOCPDFScraper.exe`.

## Build Windows installer

1. Install Inno Setup 6.
2. Run `compile_exe.bat`.
3. Run `build_installer.bat`.

Output: `installer\output\GreyNOCPDFScraperSetup.exe`.

## Notes

This tool only downloads open-access PDFs from public APIs (OpenAlex, arXiv, PubMed Central). It does not bypass paywalls, logins, robots.txt restrictions, copyright protections, or website access controls.
