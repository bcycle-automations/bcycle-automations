# bcycle-automations

## b.cycle PAYROLL Classes GitHub Action

Workflow file: `.github/workflows/mtek-classes-sync.yml`

### Triggers
- `workflow_dispatch` with required input `record_id`
- `repository_dispatch` with event type `airtable-mtek-classes-sync` and payload field `record_id`

### Required secrets
- `AIRTABLE_TOKEN`
- `MTEK_API_TOKEN`

### Config values (editable directly in workflow/script)
- Airtable base/table IDs
- MarianaTek base URL and API paths

### Script
- `scripts/sync-mtek-classes-workflow.mjs`
