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

The current correction started from `main` at `07f4904`, preserving the pending
FloodWait fixes and the pre-existing `local_db/bot_storage.db` modification. Earlier
dual-format work started at `eb1b894`. The database diff remains outside this work.

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

One ordered category mapping uses `private_channels`, `private_groups`, `bots`,
`private_chats`, `public_channels`, `public_groups`, `unclassified` throughout
classification, snapshots, HTML controls/records, counts and worksheet selection.
Display labels are separate from these identifiers.

HTML embeds all styles/scripts. It provides a dark responsive grid, private-category
shortcuts, category totals, Unicode-insensitive search across names/IDs/usernames,
category/privacy filters, clear/reset, result counts, empty results, independent
details sections and copy-ID buttons with a local-file fallback. Clear search
preserves category/privacy selections. Private categories use cards; lower-priority
categories use compact rows. Long collapsed
names are clamped; the full name remains in details. Usernames link to Telegram
only on explicit navigation. There are no automatic remote requests or assets.

Every metadata value is escaped. Dynamic controls use textContent and existing
DOM nodes. CSP permits only the hash-matched embedded script, inline styles, and
blocks network connections, objects, forms and external resources.

XLSX worksheets are ordered exactly:
Summary, Private Channels, Private Groups, Bots, Private Chats, Public Channels,
Public Groups, Unclassified. Private and Unclassified sheets retain the existing
nine-column detailed schema, including all collected discussion/inspection fields
and every supported topic field. Bots, Private Chats, Public Channels and Public
Groups use exactly Name, Chat ID, Username, Chat Type. All inventory sheets use
a frozen header, filter, alternating fills, wrapped top-aligned text,
bounded row heights and practical widths. Summary has counts, limitations and
internal worksheet links. IDs are explicit strings; all untrusted cells are
explicit strings, preventing formulas. The saved workbook is reopened for
validation of row counts, category totals, IDs and cell values against the same
snapshot. HTML identities/category counts are also parsed and checked before
delivery. The validators reject mismatches rather than sending a successful report.

Both formats preserve unknown classifications. Missing usernames alone never
establish channel/supergroup privacy. Existing full metadata RPC results are used:
only a native, full non-min Channel constructor with complete active-address state,
no active primary/collectible username, and joined/non-geo/non-monoforum flags
establishes private status. Reduced, unsupported, left, geo or monoforum constructors
without positive public evidence remain unknown. Provenance and verification are
retained through normalization and persisted in the existing JSON record column;
no SQL schema change is needed. See Telegram's [Channel constructor](https://core.telegram.org/constructor/channel),
[min constructors](https://core.telegram.org/api/min) and [channel types](https://telegram.org/faq_channels).
Basic group and private-dialog types are positive evidence
for their respective private categories. Privacy evidence is displayed. Unsupported,
missing and unverified forum data are distinguished. Topic count is API-reported,
and a confirmed empty list differs from inaccessible or incomplete metadata.
Pyrofork FORUM/MONOFORUM enums and enum strings map to groups. Public channels/groups
use existing dialog metadata and skip supplementary full-chat RPCs. Public groups
never enumerate topics, including when an active alias is discovered in a full
response already needed to resolve unknown privacy.

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

Use positive values. Long detailed values are split losslessly into labeled
continuation columns on the same chat row, bounded to 32767 UTF-16 units without
splitting surrogate pairs or stripping whitespace. Compact field or worksheet
limits fail XLSX explicitly while HTML can still be delivered. XML-incompatible
control characters are omitted from Excel and disclosed
in report limitations. Topic lists and inventory are observations during a scan,
not an atomic Telegram server snapshot.

## Dependencies and validation

Existing runtime dependency: `openpyxl>=3.1.5,<4.0` (requires Python >=3.8, compatible with the
project's Python 3.10–3.13 target). Verified here using Python 3.11.17,
Pyrofork 2.3.69 and openpyxl 3.1.5. No runtime dependency was added by this correction.
Playwright/Chromium were downloaded only under /tmp for offline synthetic-data
browser verification; requirements.txt is unchanged.

Executed checks:

- `venv/bin/python -m unittest test_userdata test_userdata_reports test_userdata_integrity -q`: 93 unittest cases passed.
- `timeout 60 venv/bin/python -m pytest -q -k 'not test_role_manager_synchronizes_role_file_local_db_and_legacy_ban'`: 153 passed, 1 deselected, 22 subtests passed (17.87 seconds).
- Earlier sandboxed `pytest -q` stalled at the SQLite synchronization test. On 2026-10-29, a worker-thread socket probe confirmed sandbox PermissionError (errno 1) blocking asyncio selector wakeup. The unchanged isolated test outside the sandbox passed in 2.86 seconds.
- `timeout 90 venv/bin/python -m pytest -q`, outside the restricted execution sandbox: **154 passed, 22 subtests passed in 15.97 seconds**, no exclusions. No test or production code workaround was added for the sandbox limitation.
- Python compilation and `git diff --check` passed.
- Actual generated JavaScript executed in Node with offline DOM fixtures to verify name/ID/username searches, filters, reset and empty states.
- A synthetic 3000-record export normalized in 0.054 seconds, generated and validated HTML in 0.624 seconds (3042693 bytes), and generated/reopened/validated XLSX in 2.945 seconds (105715 bytes). Timings are observations of this host, not guarantees.
- Offline Chromium tested all seven categories, combined Unicode/partial-ID search, clear-search state, copy-ID, details expansion, no horizontal overflow and no remote requests at 320×740, 390×844, 768×1024 and 1440×900. The two smallest viewports used mobile/touch emulation. The browser harness is /tmp/check_userdata_browser.py and uses only synthetic data.

Tests use mocked Telegram clients and temporary report directories. They cover
owner-only access, owner-selected target lookup, target identity mismatch,
nonowner job denial, one collection/shared snapshot, classification/order/totals, HTML injection,
Unicode and long names, CSP hash, formula injection, exact IDs, all sheets, empty
data, progress/FloodWait, topic pagination, independent failures, cancellation and
cleanup. No real credentials or Telegram accounts were used.

Known validation limits: mobile Chromium emulation was executed, but no physical
Android device, live Telegram delivery, or manual Excel/Google Sheets import was tested.
Only Python 3.11 was executed here. The SQLite test requires an environment allowing
standard thread-to-event-loop wakeup sockets; BUG-061 documents the resolved cause.
No commit, push, deployment, credential or production database change is part of
this implementation.

## Files and diff scope

This correction, including the pending FloodWait fix preserved at discovery,
covers these seven text/source files:

| File | Purpose |
| --- | --- |
| TechVJ/userdata_command.py | Owner-controlled orchestration, collection, progress, retry, cancellation and independent delivery |
| core/userdata_reports.py | Shared snapshot, classification and offline HTML/XLSX renderers |
| test_userdata_reports.py | Isolated report, security, browser-script and workflow regressions |
| test_userdata_integrity.py | Verified private data, FORUM mapping, detailed metadata, duplicate merging, continuation cells and artifact consistency regressions |
| data/BUGS.md | Record corrected export defects and the unrelated SQLite test timeout |
| data/PROMPTS.md | Preserve task history and project direction |
| USERDATA_REPORTS.md | Engineering analysis, configuration and validation evidence |

The pre-existing binary database diff remains outside the implementation. Startup,
session management, save/download/upload routing and unrelated commands retain
their existing code.

## Evidence from the original reports supplied in test/

Both original files were inspected read-only. HTML and XLSX contain exactly 306
records and identical multisets of 306 unique IDs. No record was lost between
these two formats. Their original categories were:

| Category | Original count |
| --- | ---: |
| Private Channels | 0 |
| Private Groups | 2 |
| Bots | 18 |
| Private Chats | 28 |
| Public Channels | 89 |
| Public Groups | 14 |
| Unclassified | 155 |

Unclassified contains 140 channels, 14 supergroups and one forum. 154 have complete
supplementary inspections; the forum has a partial inspection. The application
failed to retain sufficient private evidence/type mapping, rather than losing
these rows in the XLSX loop. Original HTML also confirms Clear resets category
and privacy. Original XLSX uses nine columns everywhere.

Root causes corrected in code: FORUM/MONOFORUM missing from type mapping; privacy
provenance discarded; dialog top-message IDs discarded with raw message objects;
basic-group counts read from the wrong full-chat field; missing supplemental values
overwriting known values; normalization retaining only description/member count/
inspection and dropping discussion/topic detail fields; first-wins duplicate removal;
Clear resetting filter state; and validation checking only workbook sheet names.

Original files do not contain the missing source evidence or discarded fields, so
they were not guessed or rewritten. After loading the corrected code, a fresh
owner-authorized `/userdata USER_ID` run obtains the available metadata through
the existing pipeline. Unknown classifications remain unknown when evidence is
insufficient. No alternate account-access route or export command was added.
