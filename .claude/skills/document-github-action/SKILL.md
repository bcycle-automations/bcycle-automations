---
name: document-github-action
description: Document a GitHub Action from this repo and register it in the "GitHub Actions" table of the Airtable Tickets base — writes a docs/*.docx in the house style, commits it, creates the Airtable record, and attaches the doc so Airtable's AI fields generate. Use whenever a new workflow is added to bcycle-automations, or an existing one needs its documentation record created or refreshed.
---

# Document a GitHub Action → Airtable

End-to-end procedure for taking a workflow in `bcycle-automations` and producing (a) a Word
document in `docs/` matching the existing house style and (b) a record in the Airtable
**GitHub Actions** table with that document attached.

Do **one record per workflow file**, not one per feature. That is the granularity the table
already uses.

## 1. Where things live

| Thing | ID |
|---|---|
| Airtable base | `appUlQkgoSmkNhaGx` ("Airtable Tickets") |
| Table | `tbl5bbwpZLlv9eez0` ("GitHub Actions") |
| Docs folder | `docs/` in the repo root |
| Repo | `bcycle-automations/bcycle-automations`, default branch `main` |

### Field map

Writable:

| Field | ID | Notes |
|---|---|---|
| Action Name | `fldqs1lcs7Uqez2Uk` | Primary. Format: `Human Name (filename.yml)` |
| Action Type | `fld4qrAPHWP3vGEDr` | `Workflow` / `Automation` / `Custom Action` / `Third-party Action`. Existing sync jobs use **Automation** |
| Status | `fldBQqsZKUmSc64KS` | `Active` / `Inactive` / `Deprecated` |
| Last Updated | `fldHNamrr1pkdt1j4` | date |
| Related Documentation | `fldcwkwlMfEee4cbD` | URL to the workflow file on `main` |
| Notes | `fldhsOdQOVsRQq4dx` | long text — the dense summary, see §4 |
| Documentation File | `fldJbUTpaI2tfKnW9` | attachment — the `.docx` |
| GitHub Action Updates | `fld41eqNj6mfFLnjK` | link to change-log table `tblmVo9E6A5zgdmqe`; leave empty on creation |

**NOT writable — these are `aiText` and Airtable computes them.** Do not try to set them and
do not apologise for them being empty at first:
`Description` (`fldQpaSejZ2LNfVmP`), `Workflow Steps` (`fldzTk4DMExqfdErD`),
`Triggers` (`fldAjNHpeHWceix3h`), `TOols connected to/impacted` (`fldoYVcwAuzsVXWi4`),
`Answer` (`fldjYBsmkeChLyTop`).

`Description` is generated **from the Documentation File attachment**. So attaching the
`.docx` is what makes it populate — a record without an attachment has a permanently empty
Description. After attaching, the field reads `{"state":"loading"}` for a while; that means
it worked.

## 2. Security rule — the repo is PUBLIC

Verify before writing anything: `gh repo view --json visibility`. As of 2026-09 it is
**public**.

Never put into `docs/`, the README, or a commit: webhook URLs (e.g. `hook.*.make.com/...`),
API tokens, PATs, or anything else that is a credential *by possession*. A Make webhook URL
is an unauthenticated trigger endpoint — treat it as a secret.

Refer to such things by **ID instead of value**: "Make scenario 6166754, webhook 2777677,
authenticating with keychain key 86508". Say where to retrieve the real value.

Check before committing:

```bash
grep -rn "hook.us2.make.com\|ghp_\|github_pat_" --exclude-dir=.git --exclude-dir=node_modules .
```

## 3. Document structure (house style)

Match `docs/Payroll_Classes_Main_Documentation.docx`. Filename convention:
`Title_Case_With_Underscores_Documentation.docx`.

- **Heading 1**: `Human Name (filename.yml)`
- **Heading 2**: numbered sections
- **Body**: plain paragraphs. Bullets are written as literal `- ` prefixed paragraphs, not
  real list numbering — that is what the existing docs do.

Baseline sections, in order:

1. System Purpose
2. High-Level Execution Summary
3. API Usage
4. Environment Variables & Secrets
5. Script Behavior (Node.js) — name the script file
6. GitHub Actions Behavior — name the workflow file, triggers, steps
7. Troubleshooting Guide
8. Summary

Add a **section 0 notice** when there is a genuine trap — e.g. "runs against the HR base, NOT
HR - Instructors", or the overlap notice on the payroll-classes docs. Add extra numbered
sections when the action has real substance worth recording: Trigger Chain, Dedupe Behavior,
Status Field Lifecycle, Airtable Schema Touched, Known Data Issues, Verification Record.

**Write down what went wrong, not just what it does.** The valuable parts of the existing docs
are the gotchas: an API parameter that is silently ignored, a dedup key that dropped rows, a
YAML quirk that made a workflow undispatchable. If a bug was found while building, record it.
Include a Verification Record section with real numbers when the action has been test-run.

## 4. The Notes field

Notes duplicates the doc in condensed form, because it is what shows in the table view.
Dense prose, no fluff. Cover: what it does and which base/tables it touches, how it is
triggered, the top two or three gotchas, and any known data issues. See the
`mtek-sales-to-bigquery.yml` record for the reference voice.

## 5. Generating the .docx

`pandoc` is **not installed** on this machine and the docx skill's
`scripts/office/soffice.py` **fails** here (a `TemporaryDirectory` kwarg incompatibility), so
neither `pandoc -t markdown` nor the PDF render/verify loop works. Use these instead.

Read an existing doc (to match style):

```bash
python3 - <<'PY'
import zipfile, re
z = zipfile.ZipFile('docs/Payroll_Classes_Main_Documentation.docx')
x = z.read('word/document.xml').decode('utf8')
for p in re.findall(r'<w:p[ >].*?</w:p>', x, re.S):
    s = re.search(r'w:pStyle w:val="([^"]+)"', p)
    t = ''.join(re.findall(r'<w:t[^>]*>(.*?)</w:t>', p, re.S))
    if t.strip(): print(f"[{s.group(1) if s else 'Normal'}] {t}")
PY
```

Generate: write a Node script in the scratchpad using `docx` (npm). It is **not** preinstalled
despite what the docx skill says — `npm install docx` inside the scratchpad first, and run the
script from there so it resolves the module.

```js
const { Document, Packer, Paragraph, HeadingLevel } = require('docx');
// blocks: [['h1'|'h2'|'p', text], ...]
// page size US Letter: properties: { page: { size: { width: 12240, height: 15840 } } }
// never use \n — one Paragraph per line
```

Verify with the same XML extraction above: confirm the Heading1/Heading2 styles landed and the
body paragraph count looks right. That is sufficient; do not chase the PDF render.

## 6. Procedure

1. Confirm repo visibility (§2) and read an existing doc for style.
2. Write the `.docx` into `docs/`, verify it via XML extraction.
3. Commit and push it to `main` — the attachment step needs the file reachable at a raw URL.
   Note this repo is often hundreds of commits behind (automated `[skip ci]` state commits);
   `git pull --rebase origin main` before pushing.
4. Confirm the raw URL resolves:
   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" \
     "https://raw.githubusercontent.com/bcycle-automations/bcycle-automations/main/docs/<FILE>.docx"
   ```
5. Create the Airtable record with the writable fields from §1.
6. Attach the doc by URL — Airtable fetches it and copies it into its own storage, so it
   survives the repo later going private:
   ```json
   {"fldJbUTpaI2tfKnW9": [{"url": "https://raw.githubusercontent.com/.../docs/<FILE>.docx",
                           "filename": "<FILE>.docx"}]}
   ```
   This only works because the repo is public. If it ever goes private, generate the file and
   hand it to the user to attach manually — there is no `upload_attachment` tool on the
   b.cycle Airtable connector (only on the Personal-Consulting and Waterpleasures ones).
7. Read the record back and confirm the attachment ingested (its `url` should now be an
   `airtableusercontent.com` link) and that `Description` shows `state: loading`.
8. Send the `.docx` files to the user with SendUserFile.

## 7. Worked example

Created 2026-09-05, both `Action Type: Automation`, `Status: Active`:

- `HR Payroll Time Punches (hr-payroll-time-punches.yml)` → `recOAzRFKaOMe275y`,
  doc `docs/HR_Payroll_Time_Punches_Documentation.docx` (14 sections — added Trigger Chain,
  Dedupe Behavior, Status Field Lifecycle, Airtable Schema Touched, Known Data Issues,
  Verification Record).
- `HR Create Budget week (hr-create-budget-week.yml)` → `recJR0X3f3AQdvM0Y`,
  doc `docs/HR_Create_Budget_Week_Documentation.docx` (10 sections — added Date Logic and
  "Why This Is Not an Airtable Automation").
