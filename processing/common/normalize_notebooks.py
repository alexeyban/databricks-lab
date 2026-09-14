#!/usr/bin/env python3
import json
import re
import sys
from pathlib import Path


BROKEN_SOURCE_COMMENT_RE = re.compile(r'^(\s*)#(.*\\n",?)$')


def repair_text(text: str) -> str:
    repaired_lines = []
    for line in text.splitlines():
        match = BROKEN_SOURCE_COMMENT_RE.match(line)
        if match:
            repaired_lines.append(f'{match.group(1)}"#{match.group(2)}')
        else:
            repaired_lines.append(line)
    return "\n".join(repaired_lines) + ("\n" if text.endswith("\n") else "")


def load_notebook(path: Path) -> dict:
    text = path.read_text()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        repaired = repair_text(text)
        data = json.loads(repaired)
        return data


def unwrap_raw_wrapped_notebook(data: dict) -> dict:
    cells = data.get("cells", [])
    if not cells:
        return data

    first = cells[0]
    if first.get("cell_type") != "raw":
        return data

    source = first.get("source", [])
    raw_text = "".join(source) if isinstance(source, list) else str(source)
    raw_text = raw_text.strip()
    if not raw_text.startswith("{") or '"cells"' not in raw_text:
        return data

    try:
        inner = json.loads(raw_text)
    except json.JSONDecodeError:
        inner = json.loads(repair_text(raw_text))
    return inner


def normalize(path: Path) -> bool:
    data = load_notebook(path)
    data = unwrap_raw_wrapped_notebook(data)
    normalized = json.dumps(data, indent=1, ensure_ascii=False) + "\n"
    original = path.read_text()
    if original != normalized:
        path.write_text(normalized)
        return True
    return False


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: normalize_notebooks.py <notebook> [<notebook> ...]")
        return 2

    changed = []
    for arg in argv[1:]:
        path = Path(arg)
        if normalize(path):
            changed.append(str(path))

    print(json.dumps({"changed": changed}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
