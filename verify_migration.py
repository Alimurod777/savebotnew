"""Offline Wzgram dependency/API verification. Never connects to Telegram."""
from __future__ import annotations

import ast
import importlib
from importlib import metadata
from pathlib import Path
import sys


def main() -> int:
    failures = []
    import wzgram
    import pyrogram
    import warpcrypto
    if metadata.version("wzgram") != "3.1.6":
        failures.append("Expected the reviewed Wzgram 3.1.6 dependency")
    if wzgram is not pyrogram:
        failures.append("Wzgram/Pyrogram aliases have different identities")
    if "wzgram" not in getattr(wzgram, "__fork__", "").lower():
        failures.append("Imported client is not Wzgram")
    for name in ("Pyrogram", "Pyrofork", "Kurigram", "tgcrypto", "tgcrypto-pyrofork"):
        try:
            metadata.version(name)
        except metadata.PackageNotFoundError:
            continue
        failures.append(f"Conflicting distribution installed: {name}")
    # Check every direct client import, including imports inside rarely used handlers.
    root = Path(__file__).resolve().parent
    paths = list(root.glob("*.py"))
    for directory in ("core", "TechVJ", "database"):
        paths.extend((root / directory).rglob("*.py"))
    count = 0
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("wzgram"):
                try:
                    module = importlib.import_module(node.module)
                    for alias in node.names:
                        if alias.name != "*":
                            getattr(module, alias.name)
                        count += 1
                except (ImportError, AttributeError) as exc:
                    failures.append(f"{path.relative_to(root)}:{node.lineno}: {exc}")
    data = bytes(range(32))
    key, iv = bytes(range(32)), bytes(range(32))
    if warpcrypto.ige256_decrypt(warpcrypto.ige256_encrypt(data, key, iv), key, iv) != data:
        failures.append("WarpCrypto IGE round-trip failed")
    print(f"Wzgram {metadata.version('wzgram')}; WarpCrypto {metadata.version('warpcrypto')}; {count} import symbols checked")
    for failure in failures:
        print(f"FAIL: {failure}")
    print(f"Verification: {len(failures)} failures")
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
