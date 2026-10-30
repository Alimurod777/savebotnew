# Wzgram migration

This branch uses [Wzgram](https://github.com/rjriajul/wzgram) 3.1.6 at
[`016ce19`](https://github.com/rjriajul/wzgram/commit/016ce19ef7136c3d8423bae26e5b3d4cd972bb44).
Python 3.11 was used for validation; upstream requires Python 3.10+.

## Installation

```bash
git clone --branch wzgram https://github.com/Alimurod777/savebotnew.git
cd savebotnew
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python verify_migration.py
python -m pip check
cp .env.example .env
# Fill in your configuration, then:
python bot.py
```

Use a fresh virtual environment when switching from Pyrofork. Wzgram supplies
both the `wzgram` and legacy `pyrogram` namespaces as the same module. Installing
another Pyrogram fork into that environment would overwrite shared files.
Wzgram depends on WarpCrypto; the old TgCrypto dependency is removed.
Git is required to install the pinned upstream commit (also installed by Docker).
The Colab/Kaggle runners select this repository's `wzgram` branch.

## Preserved behavior

Media goes through user sessions/workers; text/status/overflow goes through the
bot. Pool deliveries still use bot copy followed by deletion only after success.
Own-session delivery, premium caption/entity limits, splitting, roles, queues,
owner commands and the download-session override retain their existing behavior.
Client fingerprints remain unchanged to preserve existing authorizations.

All application imports use `wzgram`. The legacy filename
`core/pyrofork_compat.py` and its public helper names remain for callers; they
no longer imply a Pyrofork installation. The old reply monkey patch skips
Wzgram's native ReplyParameters API. Bounded opt-in upload-part FloodWait
handling remains active for the upload worker. Other clients retain Wzgram's
native uploader. Boolean reply helper flags are not passed as quoted text.

## Session and parser compatibility

Wzgram reads legacy Pyrofork session strings. Exports use Wzgram's new format;
do not assume that newly exported strings work in the old Pyrofork project.

Wzgram's native file-database migration cannot directly read Pyrofork's v4
username table and pre-existing update_state table. Before opening such a file,
`core/wzgram_compat.py` creates a private `*.session.pyrofork-backup`, converts
the username cache while preserving auth/peers/update state, then lets Wzgram
complete its native DC-address/port migration. Native files are left alone.
Backups stay local and have mode 0600. Stop all processes using that copied
session before starting this branch; never share one session file between forks.

The pinned release also iterates some absent TL vectors as if they were lists.
Version-scoped parser wrappers normalize only absent optional usernames,
restrictions and message entities. Populated values are preserved. Recheck these
workarounds before upgrading Wzgram; they do not apply to other versions.

## Local copy and validation

`/home/telegramdata3/savebotnew-wzgram` contains the source project's local
configuration, runtime data and sessions. Its `venv` was rebuilt with Wzgram.
The original `/home/telegramdata3/savebotnew` was not edited. GitHub receives
code/docs/tests only; `.env`, databases, sessions and reports remain ignored.

Offline validation uses a clean temporary source copy with no real environment,
sessions or databases. It covers imported API symbols, module identity, native
replies, parser vectors, old/new session-string round trips, v4 file migration,
upload-part handling and existing application regressions. Bot construction and
all plugin imports were also checked with outbound socket connects disabled.
No real Telegram login, download/upload, live bot start or Docker build was run.

Validation result: 168 tests and 22 subtests passed; 612 imported API symbols
verified; `pip check` passed. Docker CLI was unavailable.
