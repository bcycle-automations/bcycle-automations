---
name: payroll-validation-onedrive
description: How (and status of) moving the Make "AI PDF to Excel" payroll validation scenario (ID 6571143) from Google Drive to OneDrive — pull the newest Desjardins payroll PDF from a OneDrive folder and write the validation CSV back to a OneDrive folder. Read this when the user picks the OneDrive work back up, or before editing that scenario.
---

# Payroll validation scenario → OneDrive

Status (2026-10-27): DONE in Make, not yet test-run (both folders were empty).

## Current OneDrive setup

- Connection **11986923** "OneDrive - Jonathan (payroll validation)" (azure, Files/Sites ReadWrite.All), user jonathan@bcyclespin.com.
- Folder **Payroll validations** is SHARED WITH JONATHAN by Bijan (temporary). Item id `0146ZSOL4SCJDNKYJH6VEY3QEPOBWSBAT3`.
  - Import subfolder **Validation document**: `0146ZSOL3PIA2REYOYSNFK4BA7FCWG4E2O`
  - Export subfolder **Validation export**: `0146ZSOLZTBDNJRP2HDBH2BN332ZWYPP5R`
- Drive id is looked up at run time from `GET /v1.0/me/drive/sharedWithMe` (item named "Payroll validations" → `remoteItem.parentReference.driveId`). Make's listFolders RPC cannot browse inside shared items, so use Graph via `onedrive:makeApiCall`.
- New first modules: 30 makeApiCall sharedWithMe → 31 makeApiCall `/drives/{drive}/items/{import}/children` → 32 `onedrive:downloadAFile` (enter=id, select=my, select1=yes, drive, file = newest `application/pdf` by `lastModifiedDateTime`; filter stops the run if no PDF). Module 2 now reads `{{32.data}}`/`{{32.filename}}`. Last module 33 `onedrive:uploadAFile` to the export folder, conflictBehavior=rename.
- If Bijan removes sharing, modules 30–33 break: move to a folder Jonathan owns or a SharePoint site drive and swap ids.

## History (original plan below kept for reference)

## The scenario today

- Make scenario **6571143 "AI PDF to Excel"** (team 608569, org 3291331), on-demand.
- Flow: `google-drive:getAFile` (hard-coded file ID) → `make-ai-extractors:extractADocument` → `util:TextAggregator` (joins pages with `=====PAGEBREAK=====`) → `builtin:BasicRepeater` (one pass per page) → `ai-tools:Ask` (per-page prompt, previous page as context) → `json:ParseJSON` (data structure **529636**) → `csv:CreateAdvancedAggregator` → `builtin:BasicAggregator` → `util:ComposeTransformer` (adds header) → `google-drive:uploadAFile` (folder `0APd22MS3kYvpUk9PVA`).
- Google Drive connection **11596727** is signed in as `jonathanify6@gmail.com`.
- CSV columns: `employee_name, total_hours, total_pay, employee_matrix, total_deductions, g305_amount, employee_matrix_raw`. The user pastes A:F into the **Validation** tab of the NEW PAYROLL FILE (Google Sheet `1iy5z5iLqTQxrsOXCz6995sQxtkTflBnd8VZsQYnIQ9Q`).
- `total_pay` = printed Sous-total "Paiement" Montant (includes G305). `total_deductions` = the "Ajust/Déduc." subtotal (D40 barter). `g305_amount` = sum of G305 lines, still included in total_pay.

## What is blocking the move

No Make Microsoft connection has file access. The existing `azure` connections (3285449 Automations, 2203808 b.healthy, 4269233 Jon bcycle, 2203666 No reply, 3798667 SPINCO FRANCHISING) only have Mail/Calendar scopes. The user must add a new **Microsoft 365 / OneDrive** connection in Make (Files.ReadWrite scope) for the account that owns the folders, or Claude can create a Make credential request link for them. Claude cannot do the OAuth sign-in.

Also needed from the user:
1. The OneDrive folder the payroll PDFs land in.
2. The OneDrive folder the CSV should be written to.
3. Whether it is My files, Shared with me, or a SharePoint/Teams site drive.

## Planned change (once unblocked)

App `onedrive` version 2, connection type `azure`. Modules to use:
1. `onedrive:searchFilesFolders` — `select` = `my` (or `site`/`group`), `itemType` = `file`, folder = the PDF folder, `limit` high enough; then pick the newest PDF (sort by `lastModifiedDateTime`, filter name ending `.pdf` so the CSVs are skipped if both folders are the same). An array aggregator + `sort()` / `last()` or a `builtin:BasicFeeder`-free approach both work; keep it to one bundle going forward.
2. `onedrive:downloadAFile` — by the item ID from step 1. Map its data/name into module 2 (`extractADocument`) in place of `{{1.data}}` / `{{1.name}}`.
3. Leave modules 2–15 unchanged.
4. Replace the final `google-drive:uploadAFile` with `onedrive:uploadAFile` to the output folder; keep the filename expression `{{replace(substring(get(split(19.text; "Date comptable: "); 2); 0; 10); "/"; "-")}} - payroll totals.csv`.
5. Fetch the live blueprint with `scenarios_get` first and send the whole blueprint back with `scenarios_update` (it replaces wholesale).

## Known behaviour to remember

- Original Desjardins PDFs (real text layer) run in ~2–2.5 min and read cleanly.
- A PDF that was redacted/re-exported so the text became vector shapes runs ~10–15 min (~500 credits) and misses or invents rows near the black boxes. Tell the user to run on the unredacted original.
