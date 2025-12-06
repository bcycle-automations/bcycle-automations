// scripts/process-mtek-class.js
import fs from 'node:fs';
import process from 'node:process';

function getClassRecordIdFromEvent() {
  const eventPath = process.env.GITHUB_EVENT_PATH;
  if (!eventPath) {
    throw new Error('GITHUB_EVENT_PATH not set');
  }

  const raw = fs.readFileSync(eventPath, 'utf8');
  const event = JSON.parse(raw);

  console.log('Full event payload:', JSON.stringify(event, null, 2));

  const recordId =
    event.client_payload?.airtable_record_id ||
    event.client_payload?.recordId ||
    null;

  if (!recordId) {
    throw new Error('No airtable_record_id found in repository_dispatch payload');
  }

  return recordId;
}

async function main() {
  const recordId = getClassRecordIdFromEvent();
  console.log('✓ Got Airtable class record id from dispatch:', recordId);
}

main().catch(err => {
  console.error('Error in process-mtek-class.js:', err);
  process.exit(1);
});
