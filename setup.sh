#!/bin/sh
# One-command install/update without root. Safe to re-run; works from any directory.
#   sh setup.sh
set -eu

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PROJECT_DIR"

say() { printf '\n==> %s\n' "$1"; }
die() { printf '\nXATO: %s\n' "$1" >&2; exit 1; }

[ -f requirements.txt ] || die "requirements.txt topilmadi: $PROJECT_DIR
Loyiha to'liq yuklanmagan. Qayta yuklang:
  git clone https://github.com/Alimurod777/savebotnew.git"

if [ ! -x venv/bin/python ]; then
    # alwaysdata exposes the version chosen in Environment > Python as `python`.
    PY=""
    for candidate in ${PYTHON:-} python python3; do
        if command -v "$candidate" >/dev/null 2>&1 &&
            "$candidate" -c 'import sys; raise SystemExit(sys.version_info < (3, 10))' 2>/dev/null; then
            PY=$candidate
            break
        fi
    done
    [ -n "$PY" ] || die "Python 3.10 yoki yangiroq kerak.
alwaysdata: panel > Environment > Python da 3.12 ni tanlang, keyin qayta urinib ko'ring,
yoki:  PYTHON_VERSION=3.12 sh setup.sh"

    say "venv yaratilmoqda ($("$PY" --version 2>&1))"
    if ! "$PY" -m venv venv 2>/dev/null; then
        rm -rf venv
        "$PY" -m pip install --user --quiet virtualenv
        "$PY" -m virtualenv venv
    fi
fi

VPY="$PROJECT_DIR/venv/bin/python"

say "Kutubxonalar o'rnatilmoqda (bir necha daqiqa)"
"$VPY" -m pip install --no-cache-dir --quiet --upgrade pip
if command -v git >/dev/null 2>&1; then
    "$VPY" -m pip install --no-cache-dir -r requirements.txt
else
    # No git on the host: install the same pinned Wzgram commit from its archive.
    commit=$(sed -n 's/^wzgram @ git+.*@\([0-9a-f]\{40\}\).*$/\1/p' requirements.txt)
    [ -n "$commit" ] || die "git yo'q va requirements.txt dan Wzgram commit o'qilmadi."
    "$VPY" -m pip install --no-cache-dir \
        "wzgram @ https://github.com/rjriajul/wzgram/archive/$commit.zip"
    grep -v '^wzgram @' requirements.txt >venv/requirements-nogit.txt
    "$VPY" -m pip install --no-cache-dir -r venv/requirements-nogit.txt
fi

if [ ! -f .env ]; then
    cp .env.example .env
    chmod 600 .env
    say ".env yaratildi (namunadan)"
fi

say "Tekshiruv"
"$VPY" -m pip check
"$VPY" verify_migration.py

cat <<EOF

Tayyor. Keyingi qadamlar:

1) .env faylini to'ldiring (API_ID, API_HASH, BOT_TOKEN, OWNER_ID):
     nano "$PROJECT_DIR/.env"

2) Sinab ko'rish (to'xtatish: Ctrl+C):
     /bin/sh "$PROJECT_DIR/start-service.sh"

3) alwaysdata panelida doimiy ishga tushirish uchun shu buyruqni kiriting:
     /bin/sh $PROJECT_DIR/start-service.sh
   (batafsil: ALWAYSDATA.md)
EOF
