# Airtable "Instructor Portal / Portail d'instructeur" — Audit (Stage 1, PARTIAL)

Base: `appx3zb8X7DCtY8sO`. Audited 2026-11-12 via the Airtable MCP API only.

**Status: partial.** This cloud session has no browser or computer-use tools, so the Airtable web UI could not be walked. The API sees one interface; the user says there are more (the main instructor interface and its ratings, events and subs pages). Those are still unaudited.

## 1. What the API can see (confirmed)

`list_pages_for_base` (including drafts and record-detail pages) returns exactly one interface. There are no draft pages, no standalone or embedded forms, and no record-detail pages.

### Interface: Instructor Info - Admin (`pbdYgYb0iW8F0AgUP`)

| Page | ID | Type | Source table |
|---|---|---|---|
| Active Employees | `pagTI7NleKlB9HvAr` | list (record review) | Active Employees `tblQ8EgCutGt3QwzK` |

Fields on the page. All come straight from `Active Employees`, with no lookups or links, and all are editable.

| Role | Field | Field ID | Type |
|---|---|---|---|
| Title | Name | fldwEGaq5UwRn4c6J | multilineText |
| Subtitle | Status | fldRtQxNL8k7Sq0wJ | singleSelect (Active, Inactive, Onboarding, Mentorship, Offboarding, Sub-only, On Hold) |
| Thumbnail | Contract Amendment | fldoNTGsIHGjTcMvv | attachments |
| Detail | Manager | fld90tdl13pwqOuK0 | singleLineText |
| Detail | Program | fld5OjeD6sbJAKKxp | multiSelect (Spin, Barre, Strength) |
| Detail | Start Date | fldSKV3tEcGtR0Cis | date |
| Detail | Next Eval | fldQUVhOyNDxgLXIV | date |
| Detail | Next Due | fldA335m6Sqez25Kh | singleSelect (Sit-down, Self-eval) |
| Detail | Eval Due | fldmGWh3fjYTAMBHq | checkbox |
| Detail | Email | fldKbLJbuGGrmIHj0 | email |
| Detail | Barre Bonus Rate | fldaDBkQuG3hx7to5 | currency |

## 2. Base tables (from `list_tables_for_base`)

Several fields in these tables are text-typed copies of lookups, such as "Email (from Manager)" and "(from Employee Info)".

| Table | ID | Notes |
|---|---|---|
| Active Employees | tblQ8EgCutGt3QwzK | ~150 fields: contact, rates, evals, sanctions, offboarding, CPR, newsletter stats, 90-day ratings, attendance, subs |
| Grid view (ratings) | tblu3XNEP2SRfLVIF | Rating name, Email (from Instructor), RATING, COMMENT, CAL_NAME, CLASSTYPE, SITE, Instructor, New, Directed to, Type, DATE OF RATING |
| Reminder/Correction count | tblAJANIshoeau7cn | Newsletter reminder and did-not-read counts and % per instructor |
| Event Registrations | tblE8qkVMYldSmf1U | Event signups for staff and instructors, with waitlist and event lookups |
| EVENTS | tblk2RdlcI75AuThD | Same shape as Event Registrations, plus dietary restrictions, FINAL EMAIL and FINAL NAME |
| Payroll Admin | tblmgKnHD4UrQNFYa | Evaluations, Mentorship and CPR reimbursement amounts and dates |
| Subs Offered | tbl7FFLiqzD2m9a4k | Sub points and scoring, plus "I cannot sub this class!" button and Cancel Sub URL |
| Active Employees - Subs | tblnUR1Jm4gvxIshh | Contribution scores, 365-day sub and class counts, late sub requests |
| Active Employees - Ratings | tbliIGQY1j9Tcv4pC | Ratings and averages per instructor, plus eval dates |
| Attendance Comparison | tblFVUrY5pPbUOW1V | Average attendance against team average, percentile |
| Conversion - Weekly | tblpO3JhhZLfSaaI3 | First-timer conversion per instructor per week |
| Table 1 | tblMTk7Y7WY4aOIO5 | Scratch table (Name, Notes, Assignee, Status). Ignore. |

## 3. Not audited (needs the Airtable web UI)

- The main instructor interface and every page in the sidebar.
- Page types, layouts and fields for the ratings, events, subs and other pages.
- Buttons and links on the main instructor interface and where each leads.

Options to close this gap:
1. The user connects the cloud session to their computer (browser or computer-use), or runs this audit from a local Claude Code session with Chrome.
2. The user lists the interface and page names, or shares screenshots or page URLs. The API accepts `pageIds` if the IDs are known.

## 4. Existing Softr pages likely to overlap (no duplicates; app `7aac4d59-d2a3-47a3-90a7-f08772c0567d`)

- Instructor Info - Staff (`cc639c2b-…`, one List block)
- Instructor Info - Marketing (`4d2a8ae8-…`)
- Active Employees - Instructors (`295578c1-…`, tabs plus two vibe-coding blocks)
- Active Employees HR (`f09a78d5-…`)
- Ratings-per instructor (`d8f08f36-…`)
- Instructor ratings details (`644a3a9f-…`)
- Ratings - Team (`45a5041a-…`)
- Events details (`32eed0dd-…`)
- Events - MGMT (`838ead97-…`)
- Overview - Events (`43feadfe-…`)
- Subs - Admin (Cycle / Body) (`8444b73a-…`)
- Evaluations Submitted - Instructors (`e757c057-…`)
- Evals Due - Instructors (`ccf3862e-…`)
- Evals To do - Instructors (`3cca0bfa-…`)
- Payroll admin - b.cycle (`fbb263f8-…`)

## 5. Preliminary mapping (to be confirmed after the full audit)

| Airtable page | Softr target | Action |
|---|---|---|
| Instructor Info - Admin / Active Employees | "Instructor Info - Staff" or "Active Employees - Instructors" | Likely exists. Check that the 11 fields above are present, then change only if missing. No new page yet. |
| Main instructor interface, ratings, events, subs, other pages | Unknown | Blocked on the UI audit. |
