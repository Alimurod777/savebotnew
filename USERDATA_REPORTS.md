# Owner-controlled inventory exports

Only the configured owner can use `/userdata <user_id>` in a private conversation
with the bot. The target user's existing logged-in session is looked up by the
explicit numeric ID and its Telegram identity is verified before collecting
metadata. Progress and both reports go to OWNER_ID, using the original command's
bot delivery path. The target account does not receive the reports. Captions and
both files identify the target account.

`/userdata_status`, `/userdata_resume` and `/userdata_cancel` are also owner-only.
Ordinary users cannot initiate personal exports or access any inventory job.
Owner task registration is preserved, including /stop cancellation behavior.
This restores the original owner-controlled command after the user explicitly
corrected the earlier self-service change. It preserves HTML/XLSX reporting.

## Implementation and baseline

Discovery found `main` at `eb1b894` and an existing uncommitted dual-format
implementation. This work completes and hardens those changes without resetting
them. The pre-existing `local_db/bot_storage.db` modification is outside this work.

`TechVJ/userdata_command.py` retains the existing command registration, target
session lookup, task manager, checkpoint schema, progress reporter, resume and
cancellation commands. The inventory enumerates dialogs once, deduplicates chat
IDs, and inspects their permitted metadata. It retains a whitelist of dialog
metadata, not message contents, phone numbers or raw dialog objects.

Channel/group inspection uses raw full-chat metadata because installed Pyrofork
2.3.69's high-level `get_chat()` can implicitly fetch pinned messages. Topic
enumeration remains the existing `messages.GetForumTopics` operation; it handles
short pages, API-reported counts, pagination order, duplicate topic IDs and
partial/unavailable results. No history traversal or invite creation is added.
Telegram dialog/topic RPC responses themselves can contain related messages and
users; their contents are neither inventoried nor exported. Only topic pagination
dates from those responses are consulted.

`core/userdata_reports.py` normalizes records into one immutable snapshot and has
two independent renderers with no Telegram/database access. The same snapshot,
timestamp, classifications and totals feed both formats. Existing TXT generation
is retained for compatibility but is no longer the command's delivery path.

## Reports

HTML embeds all styles/scripts. It provides a dark responsive grid, private-category
shortcuts, category totals, Unicode-insensitive search across names/IDs/usernames,
category/privacy filters, clear/reset, result counts, empty results, independent
details sections and copy-ID buttons with a local-file fallback. Long collapsed
names are clamped; the full name remains in details. Usernames link to Telegram
only on explicit navigation. There are no automatic remote requests or assets.

Every metadata value is escaped. Dynamic controls use textContent and existing
DOM nodes. CSP permits only the hash-matched embedded script, inline styles, and
blocks network connections, objects, forms and external resources.

XLSX worksheets are ordered exactly:
Summary, Private Channels, Private Groups, Private Chats, Public Channels,
Public Groups, Bots, Unclassified. Each inventory sheet uses the same nine
columns, frozen header, filter, alternating fills, wrapped top-aligned text,
bounded row heights and practical widths. Summary has counts, limitations and
internal worksheet links. IDs are explicit strings; all untrusted cells are
explicit strings, preventing formulas. The saved workbook is reopened for
structural validation; tests compare its values/counts to the snapshot.

Both formats preserve unknown classifications. Missing usernames alone never
establish channel/supergroup privacy, so many such chats can legitimately appear
under Unclassified. Basic group and private-dialog types are positive evidence
for their respective private categories. Privacy evidence is displayed. Unsupported,
missing and unverified forum data are distinguished. Topic count is API-reported,
and a confirmed empty list differs from inaccessible or incomplete metadata.

## Reliability and resource limits

Normalization and both synchronous renderers run off the event loop. A cancelled
job joins its worker, including repeated cancellation, before removing temporary
files and releasing active-job state. Each request uses its own secure temporary
directory. Format generation and delivery outcomes are independent. Partial
success, upload/generation limits and cleanup failures are reflected in completion
status. Logs record operation/error classes rather than report contents or exception
strings. No shared client is disconnected.

Progress is throttled with an approximately five-second heartbeat; stage transitions
can update immediately. Progress edits respect FloodWait cooldowns. API/upload retries
are bounded. Revoked authorization and long/exhausted collection FloodWait stop further
collection calls. Resume uses the existing checkpoints. Geometric checkpoint batches
avoid repeatedly serializing a growing inventory every 25 records; an abrupt process
exit can therefore require re-inspecting more of the last unfinished batch.

Optional environment settings (no `.env` edits are required):

| Setting | Default | Behavior |
| --- | --- | --- |
| USERDATA_MAX_CHATS | 50000 | Stop collection and disclose a partial scan; discovered total becomes a lower bound |
| USERDATA_MAX_TOPICS | 10000 per forum | Keep accessible topics and disclose partial topic enumeration |
| USERDATA_MAX_REPORT_BYTES | 1900 MiB | Fail only the oversize format, capped at a conservative 2000 MiB for non-premium MTProto delivery |
| USERDATA_UPLOAD_TIMEOUT | 1800 seconds per attempt | Bound a stalled document upload |

Use positive values. Excel's 32767-character cell limit is checked before openpyxl
can truncate it. Oversize cells fail XLSX explicitly while HTML can still be
delivered. XML-incompatible control characters are omitted from Excel and disclosed
in report limitations. Topic lists and inventory are observations during a scan,
not an atomic Telegram server snapshot.

## Dependencies and validation

Runtime addition: `openpyxl>=3.1.5,<4.0` (requires Python >=3.8, compatible with the
project's Python 3.10–3.13 target). Verified here using Python 3.11.17,
Pyrofork 2.3.69 and openpyxl 3.1.5. Pytest was installed only as a local test runner.

Executed checks:

- `venv/bin/python -m unittest test_userdata test_userdata_reports -q`: 72 unittest cases passed after restoring the owner-only workflow.
- `timeout 60 venv/bin/python -m pytest -q -k 'not test_role_manager_synchronizes_role_file_local_db_and_legacy_ban'`: 132 passed, 1 deselected, 15 subtests passed (16.97 seconds). This also executes the repository's standalone regression test functions.
- Full `pytest -q` stalled at the existing role-manager/SQLite synchronization test and was interrupted. Running that test alone with a 25-second timeout also timed out (exit 124); its trace waits in aiosqlite's worker and the asyncio selector. It is not reported as passing.
- Python compilation and `git diff --check` passed.
- Actual generated JavaScript executed in Node with offline DOM fixtures to verify name/ID/username searches, filters, reset and empty states.
- A synthetic 3000-record export generated HTML in 0.073 seconds (1962875 bytes) and XLSX in 3.361 seconds (136589 bytes), including workbook reopening. Timings are observations of this host, not guarantees.

Tests use mocked Telegram clients and temporary report directories. They cover
owner-only access, owner-selected target lookup, target identity mismatch,
nonowner job denial, one collection/shared snapshot, classification/order/totals, HTML injection,
Unicode and long names, CSP hash, formula injection, exact IDs, all sheets, empty
data, progress/FloodWait, topic pagination, independent failures, cancellation and
cleanup. No real credentials or Telegram accounts were used.

Known validation limits: no physical Android/browser layout test or live Telegram
delivery was performed; JavaScript behavior and mobile CSS were checked separately.
Only Python 3.11 was executed here. The unrelated SQLite test remains unresolved.
No commit, push, deployment, credential or production database change is part of
this implementation.

## Files and diff scope

The combined working-tree change (including the dual-report work already present
at discovery) covers these nine text/source files:

| File | Purpose |
| --- | --- |
| TechVJ/userdata_command.py | Owner-controlled orchestration, collection, progress, retry, cancellation and independent delivery |
| core/userdata_reports.py | Shared snapshot, classification and offline HTML/XLSX renderers |
| TechVJ/owner_commands.py | Document restored /userdata <uid> owner command and dual-format reports |
| requirements.txt | Declare the workbook dependency |
| test_userdata.py | Correct missing-username classification expectations; retain legacy regressions |
| test_userdata_reports.py | Isolated report, security, browser-script and workflow regressions |
| data/BUGS.md | Record corrected export defects and the unrelated SQLite test timeout |
| data/PROMPTS.md | Preserve task history and project direction |
| USERDATA_REPORTS.md | Engineering analysis, configuration and validation evidence |

The pre-existing binary database diff remains outside the implementation. Startup,
session management, save/download/upload routing and unrelated commands retain
their existing code.
