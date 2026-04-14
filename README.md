# bcycle-automations

## b.cycle PAYROLL Classes GitHub Action

Workflow file: `.github/workflows/bcycle-payroll-classes-action.yml`

### Triggers
- `workflow_dispatch` with required input `record_id`
- `repository_dispatch` with event type `airtable-bcycle-payroll-classes` and payload field `record_id`

### Required secrets
- `AIRTABLE_TOKEN`
- `MTEK_API_TOKEN`

### Config values (editable directly in workflow/script)
- Airtable base/table IDs
- MarianaTek base URL and API paths

### Script
- `scripts/bcycle-payroll-classes-workflow.mjs`
