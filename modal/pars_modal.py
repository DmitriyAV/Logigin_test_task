from __future__ import annotations
import ast
import json
import re
from pathlib import Path
from typing import Iterator, Any, Iterable, List, Dict, Optional

__all__ = [
    "parse_log_lines",
    "extract_events",
    "parse_line",
    "parse_file",
]

def contains_special_chars(text: str) -> bool:
    return any(sym in text for sym in ["{", "[", "(", ":"])

def contains_brackets(text: str) -> bool:
    return any(sym in text for sym in ["{", "[", "("])

def transform_to_json(fragment: str) -> str:
    account_id = _EVENT_ACCAUNt_ID_RE.match(fragment)
    try:
        parsed_dict = ast.literal_eval(fragment)
        if account_id:
            parsed_dict['account_id'] = account_id
        return json.dumps(parsed_dict, default=str)
    except (ValueError, SyntaxError):
        return fragment

def find_json_like_structure(text: str) -> Iterable[str]:
    stack: List[int] = []
    start = -1
    for i, ch in enumerate(text):
        if ch == "{":
            if not stack:
                start = i
            stack.append(i)
        elif ch == "}":
            if stack:
                stack.pop()
                if not stack and start >= 0:
                    yield text[start: i + 1]

_TUPLE_RE = re.compile(r"([A-Za-z0-9_]+)\((.*?)\)")

def _transform_enum(value: Any) -> Any:
    return value[1] if isinstance(value, tuple) and len(value) == 2 else value

def _tuple_to_dict(event_data: str) -> dict | str:
    try:
        parsed = ast.literal_eval(f"{{{event_data}}}")
        if isinstance(parsed, dict):
            return {k: _transform_enum(v) for k, v in parsed.items()}
        return parsed
    except (ValueError, SyntaxError):
        return event_data

def extract_events_tuple(message: str):
    if "(" not in message:
        return []
    events = _TUPLE_RE.findall(message)
    return [{"type": t, "data": _tuple_to_dict(d)} for t, d in events]

def extract_events(message: str):
    return extract_events_tuple(message)

_EVENT_HEAD_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T[^Z]+Z)\t.*?event:\s*(?P<body>.*)$")
_EVENT_PERFOMED_RE = re.compile(r'\bperformed\s+(\w+)\b')
_EVENT_GENERIC_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T[^Z]+Z)\t(?P<body>.*)$")
_EVENT_ACCAUNt_ID_RE = re.compile(r"account_id\s*[:=]\s*['\"]?([\w-]+)['\"]?")

def _try_load_json(fragment: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(fragment)
    except json.JSONDecodeError:
        try:
            return json.loads(transform_to_json(fragment))
        except json.JSONDecodeError:
            return None

def _normalize_ip(event_dict: Dict[str, Any]):
    if "callerIpAddress" in event_dict and ":" in str(event_dict["callerIpAddress"]):
        event_dict["callerIpAddress"] = str(event_dict["callerIpAddress"]).split(":")[0]

def parse_line(line: str) -> Optional[Dict[str, Any]]:
    m = _EVENT_HEAD_RE.match(line)
    if not m:
        m = _EVENT_GENERIC_RE.match(line)
        if not m:
            return None
    ts_raw, body = m.group("ts"), m.group("body")
    event_dict: Optional[Dict[str, Any]] = None
    if body.lstrip().startswith("{"):
        event_dict = _try_load_json(body)
    if not event_dict:
        for frag in find_json_like_structure(body):
            event_dict = _try_load_json(frag)
            if event_dict:
                break
    if not event_dict:
        tuples = extract_events_tuple(body)
        if tuples:
            tup = tuples[0]
            if isinstance(tup, dict) and isinstance(tup.get("data"), dict):
                event_dict = tup["data"]
                event_dict.setdefault("eventType", tup.get("type"))
    if not event_dict:
        return None
    event_dict.setdefault("time_global", ts_raw)
    account_match = _EVENT_ACCAUNt_ID_RE.search(line) if line else None
    account_id = account_match.group(1) if account_match else None
    if account_id:
        event_dict.setdefault("account_id", account_id)
    perfomed_match = _EVENT_PERFOMED_RE.search(line) if line else None
    perfomed_event = perfomed_match.group(1) if perfomed_match else None
    if perfomed_event:
        event_dict.setdefault("eventTypePerformed", perfomed_event)
    _normalize_ip(event_dict)
    return event_dict


def parse_file(path: str | Path) -> Iterator[Dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if (ev := parse_line(line.rstrip("\n"))):
                yield ev

if __name__ == "__main__":
    samples = [
        "2025-04-08T12:37:33.451Z\tThe event getblobproperties is not mapped, ignoring. event: {\"time\": \"2025-04-08T12:36:27Z\", \"operationName\": \"GetBlobProperties\", \"statusCode\": 200, \"durationMs\": 5, \"callerIpAddress\": \"10.1.1.1:123\"}",
        "2025-04-08T12:35:22.269Z\tscanning 1 automatic evend: [AutomaticEvent(resource_id='snap-06ff02120fb312fe9', resource_type='snapshot', action=<ActionType.DELETE: 'delete'>, account_id='769409937191', region='us-east-1', additional_scans={}, additional_deletes=[])]",
    ]
    for s in samples:
        parse_line(s)
