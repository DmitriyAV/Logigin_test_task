import json
import re
import ast
from pathlib import Path

def contains_special_chars(text: str) -> bool:
    return any(sym in text for sym in ['{', '[', '(', ':'])

def contains_brackets(text: str) -> bool:
    return any(sym in text for sym in ['{', '[', '('])

def transform_to_json(fragment: str) -> str:
    try:
        parsed_dict = ast.literal_eval(fragment)
        return json.dumps(parsed_dict, default=str)
    except (ValueError, SyntaxError):
        return fragment

def find_json_like_structure(text: str):
    stack = []
    start = -1
    for i, char in enumerate(text):
        if char == '{':
            if not stack:
                start = i
            stack.append(char)
        elif char == '}':
            if stack:
                stack.pop()
                if not stack:
                    yield text[start:i + 1]
    if stack:
        yield text[start:]

def fix_bracket_balance(text: str) -> str:
    stack = []
    result = []
    for char in text:
        if char == '{':
            stack.append(char)
            result.append(char)
        elif char == '}':
            if stack:
                stack.pop()
                result.append(char)
            else:
                result.append('{')
                result.append(char)
        else:
            result.append(char)
    result.extend('}' * len(stack))
    return ''.join(result)

def parse_log_lines(filepath):
    without_special_chars = []
    with_special_chars = []
    with_brackets = []
    without_brackets = []
    empty_dict_lines = []
    with open(filepath, encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            if '\t' in line:
                timestamp, message = line.split('\t', 1)
            else:
                timestamp, message = None, line
            entry = {"timestamp": timestamp, "message": message}
            if contains_special_chars(message):
                with_special_chars.append(entry)
                if contains_brackets(message):
                    with_brackets.append(entry)
                else:
                    without_brackets.append(entry)
            else:
                without_special_chars.append(entry)
    valid_dict_lines = []
    for entry in with_brackets:
        try:
            json_fragments = list(find_json_like_structure(entry["message"]))
            remaining_message = entry["message"]
            for fragment in json_fragments:
                transformed_fragment = transform_to_json(fragment)
                try:
                    log_dict = json.loads(transformed_fragment)
                    valid_dict_lines.append({
                        "timestamp": entry["timestamp"],
                        "message": remaining_message.replace(fragment, "", 1),
                        "parsed_data": log_dict
                    })
                    remaining_message = remaining_message.replace(fragment, "", 1)
                except json.JSONDecodeError:
                    pass
            if not valid_dict_lines or remaining_message.strip():
                empty_dict_lines.append({"timestamp": entry["timestamp"], "message": remaining_message.strip()})
        except Exception:
            empty_dict_lines.append(entry)
    return without_special_chars, with_special_chars, valid_dict_lines, empty_dict_lines, without_brackets

def parse_log_lines(filepath: str | Path):
    filepath = Path(filepath)
    without_special_chars, with_special_chars = [], []
    with_brackets, without_brackets = [], []
    empty_dict_lines, valid_dict_lines = [], []
    with filepath.open(encoding="utf-8", errors="ignore") as file:
        for line in file:
            line = line.rstrip("\n")
            if not line:
                continue
            timestamp, message = line.split("\t", 1) if "\t" in line else (None, line)
            entry = {"timestamp": timestamp, "message": message}
            if contains_special_chars(message):
                with_special_chars.append(entry)
                if contains_brackets(message):
                    with_brackets.append(entry)
                else:
                    without_brackets.append(entry)
            else:
                without_special_chars.append(entry)
            for frag in find_json_like_structure(message):
                if (ev := _try_load_json(frag)):
                    valid_dict_lines.append({
                        "timestamp": timestamp,
                        "message": message.replace(frag, "", 1),
                        "parsed_data": ev,
                    })
                    break
            else:
                tuples = extract_events_tuple(message)
                if tuples:
                    valid_dict_lines.append({
                        "timestamp": timestamp,
                        "message": message,
                        "parsed_data": tuples[0]["data"],
                    })
                else:
                    empty_dict_lines.append(entry)
    return (
        without_special_chars,
        with_special_chars,
        valid_dict_lines,
        empty_dict_lines,
        without_brackets,
    )