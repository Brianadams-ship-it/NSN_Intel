# NSN Intelligence Suite (Deploy Package)

This package wires together:
- Excel cockpit (`NSN_Filter2.xlsx`) with Command Center + NoBid scoring
- Separated-phase crawler package (Phase 1 index -> Phase 2 probe -> Phase 3 focused download/extract)
- Evidence Viewer (HTML)
- Engineering Fingerprint generator
- FLIS reference number importer
- Decision Recommendation Engine v3 (adds Pricing signals + BID_NOW action)

## Folder map
- `01_Workbook/NSN_Filter2.xlsx`  -> primary operator workbook
- `02_Inputs/`                   -> drop in input datasets (NoQuote, Pricing, seed manuals list)
- `03_Pipelines/`                -> Python tools (and packaged zips) that generate evidence + features
- `04_Run/`                      -> one-click bat files

## Quick Start (ROI path)
1) Copy this whole folder somewhere (e.g., `C:\NSN_Intel_Suite\`)
2) Run: `04_Run\run_00_all.bat`
   - imports Pricing signals into workbook
   - runs Decision Engine v3 (profit-aware)
3) Open: `01_Workbook\NSN_Filter2.xlsx` -> `NSN_Command_Center`
   - sort by `Action` (BID_NOW / SAR_TARGET / COTS_VALIDATE)

## Running the crawler
Use the crawler package zip:
- `03_Pipelines\A_Crawl_Phases\NSN_Crawler_SeparatedPhases_v1_7_Package.zip`

Unzip it into a working folder (e.g., `C:\NSN_Crawler\`), then follow its README to:
- Phase 1: index URLs (no download)
- Phase 2: probe titles/snippets
- Phase 3: focused download/extract + images

Then load outputs back into the workbook (Evidence_Links / Coverage / etc.).

## Decision Engine Configuration
Decision Engine v3 reads tunable weights and thresholds from an optional `Decision_Config` sheet
in the workbook (key-value format, same as other config sheets). If the sheet is absent, built-in
defaults are used. Available keys include:

- **Opportunity weights**: `W_EVIDENCE`, `W_FINGERPRINT`, `W_DOCS`, `W_NOBID`, `W_PROFIT`, `W_PN_DUP`, `W_HAS_URL`
- **Action thresholds**: `BID_NOW_PROFIT`, `BID_NOW_EVIDENCE`, `BID_NOW_FP`, `SAR_NOBID`, `SAR_EVIDENCE`, `SAR_FP`, `COTS_FP`, `COTS_EVIDENCE`, `CRAWL_MAX_EVIDENCE`, `CRAWL_MAX_DOCS`
- **Sub-score columns**: Add `Evidence Score`, `FP Score`, `Docs Score`, `Profit Score` columns to `NSN_Command_Center` row 6 to receive per-signal breakdowns.

## Notes
- NoBid signals are already integrated into the workbook (`NoBid_Signals` + XLOOKUP columns).
- Pricing signals are imported via `run_02_import_pricing.bat` and written to `Pricing_Signals`.
- Decision Engine v3 uses evidence + fingerprint + no-bid + pricing to produce BID_NOW/SAR_TARGET/COTS_VALIDATE.
- All pipeline scripts create a timestamped `.backup_*.xlsx` before saving.
- Log files are written alongside the workbook for each pipeline run.
- Dependencies are installed once per venv; delete `.venv\.deps_installed` to force reinstall.
