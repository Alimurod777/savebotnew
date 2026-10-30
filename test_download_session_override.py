"""
Regression test: download-only session override (/dlsession).

Verifies, without Telegram/wzgram:
  * no override        -> download client is built from the user's own DB session
  * override set       -> download client is built from the override session
  * invalid override   -> DownloadSessionError (NOT SessionInvalidError, so the DB
                          upload session is never wiped by _handle_session_invalid)
  * clear              -> back to the own session
  * account id         -> used ONLY for the download client (name/fingerprint)
  * album/location upload client -> the user's OWN session when an override is active,
                          the download client itself otherwise (behaviour unchanged)
  * legacy store format ({"uid": "session"}) is still readable

Run: python -m pytest test_download_session_override.py  (or python test_download_session_override.py)
"""
import asyncio
import importlib.util
import logging
import os
import sys
import tempfile
import types
from contextlib import asynccontextmanager
from typing import Optional

REPO = os.path.dirname(os.path.abspath(__file__))


class _FakeSessionInvalidError(Exception):
    def __init__(self, msg="", error_type="AuthKeyUnregistered"):
        super().__init__(msg)
        self.error_type = error_type


def _load_store(tmp_cwd: str):
    os.makedirs(os.path.join(tmp_cwd, "data"), exist_ok=True)
    os.chdir(tmp_cwd)
    spec = importlib.util.spec_from_file_location(
        "download_session_store", os.path.join(REPO, "core", "download_session_store.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # save.py imports it as core.download_session_store; avoid importing the heavy `core` package.
    pkg = types.ModuleType("core")
    pkg.__path__ = []
    sys.modules["core"] = pkg
    sys.modules["core.download_session_store"] = mod
    return mod


def _load_wrapper(used: list, invalid: set):
    src = open(os.path.join(REPO, "TechVJ", "save.py"), encoding="utf-8").read()
    block = src[src.index("class DownloadSessionError(RuntimeError):"):src.index("class PoolDeliveryError(RuntimeError):")]

    @asynccontextmanager
    async def create_user_session(session_string, user_id, **kw):
        used.append((session_string, user_id))
        if session_string in invalid:
            raise _FakeSessionInvalidError("bad")
        yield f"client[{session_string}]"

    ns = dict(
        Optional=Optional,
        logger=logging.getLogger("test"),
        create_user_session=create_user_session,
        SessionInvalidError=_FakeSessionInvalidError,
        _safe_session_fp=lambda s: (s or "-")[:4],
        asynccontextmanager=asynccontextmanager,
    )
    exec(block, ns)
    return ns["_open_download_session"], ns["DownloadSessionError"], ns["_album_upload_session"]


def test_download_override_flow():
    old_cwd = os.getcwd()
    old_core = sys.modules.get("core"), sys.modules.get("core.download_session_store")
    try:
        store = _load_store(tempfile.mkdtemp())
        used, invalid = [], set()
        open_dl, DownloadSessionError, album_up = _load_wrapper(used, invalid)

        async def run():
            # no override -> own DB session
            async with open_dl("OWN", 1, peers_to_resolve=[5]) as acc:
                assert acc == "client[OWN]"

            # override -> download uses override, user id unchanged
            store.set_download_session(1, "OTHER")
            async with open_dl("OWN", 1) as acc:
                assert acc == "client[OTHER]"
            assert used[-1] == ("OTHER", 1)

            # override is per-user
            async with open_dl("OWN2", 2) as acc:
                assert acc == "client[OWN2]"

            # invalid override -> DownloadSessionError, never SessionInvalidError
            invalid.add("OTHER")
            try:
                async with open_dl("OWN", 1):
                    raise AssertionError("must not enter with invalid override")
            except DownloadSessionError as err:
                assert not isinstance(err, _FakeSessionInvalidError)

            # exceptions from the body are not translated
            invalid.clear()
            try:
                async with open_dl("OWN", 1):
                    raise ValueError("body")
            except ValueError:
                pass

            # account id is used only for the download client; upload/routing id unchanged
            store.set_download_session(1, "OTHER", account_id=777)
            assert store.get_download_account_id(1) == 777
            async with open_dl("OWN", 1) as acc:
                assert acc == "client[OTHER]"
            assert used[-1] == ("OTHER", 777), used[-1]

            # upload client for albums / location / venue
            dl_client = object()
            async with album_up("OWN", 1, dl_client) as up:
                assert up == "client[OWN]" and up is not dl_client   # owner's own session
            assert used[-1] == ("OWN", 1)
            async with album_up("OWN2", 2, dl_client) as up:
                assert up is dl_client                                # no override -> unchanged
            try:
                async with album_up(None, 1, dl_client):
                    raise AssertionError("must not fall back to the download client")
            except DownloadSessionError:
                pass

            # legacy store format still readable
            import json
            with open(os.path.join("data", "download_sessions.json"), "w", encoding="utf-8") as f:
                json.dump({"5": "LEGACY"}, f)
            assert store.get_download_session(5) == "LEGACY"
            assert store.get_download_account_id(5) is None
            async with open_dl("OWN5", 5) as acc:
                assert acc == "client[LEGACY]"
            assert used[-1] == ("LEGACY", 5)
            store.set_download_session(1, "OTHER")  # restore for the clear check below

            # clear -> own session again
            assert store.clear_download_session(1) is True
            assert store.clear_download_session(1) is False
            async with open_dl("OWN", 1) as acc:
                assert acc == "client[OWN]"

        asyncio.run(run())
    finally:
        os.chdir(old_cwd)
        for name, mod in zip(("core", "core.download_session_store"), old_core):
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod


if __name__ == "__main__":
    test_download_override_flow()
    print("OK")
