import re
import ast
import json


def contains_brackets_tuple(text: str) -> bool:
    """Checks if the text contains any of the following brackets: '[', '('. """
    return any(sym in text for sym in ['[', '('])


def transform_enum(value: any) -> any:
    """Transforms Enum-like objects into their string representation."""
    if isinstance(value, tuple) and len(value) == 2:
        return value[1]  # Example: 'delete' from ActionType.DELETE
    return value


def transform_tuple_to_dict(event: str) -> dict:
    """Converts a Python-like tuple string into a dictionary, including Enum transformations."""
    try:
        parsed_dict = ast.literal_eval(event)
        # If the object contains tuples or Enums, we convert them to string values
        if isinstance(parsed_dict, dict):
            return {key: transform_enum(value) for key, value in parsed_dict.items()}
        return parsed_dict
    except (ValueError, SyntaxError):
        # If the string can't be interpreted as a dictionary or tuple, return the original string
        return event


def extract_event_data(message: str):
    """Extracts event types and their data from the message using regular expressions."""
    pattern = r"([A-Za-z0-9_]+)\((.*?)\)"
    events = re.findall(pattern, message)
    return events


def transform_event(event_data: str):
    """Transforms event data from a string format into a dictionary."""
    try:
        transformed_event = transform_tuple_to_dict(f"{{{event_data}}}")
        return transformed_event
    except Exception as e:
        print(f"Error parsing event: {e}")
        return None


def extract_events(entry, valid_dict_lines, empty_dict_lines):
    """Extracts all events from the message and converts them into dictionaries."""
    try:
        events = extract_event_data(entry["message"])  # Extract events using the regex

        remaining_message = entry["message"]
        for event_type, event_data in events:
            transformed_event = transform_event(event_data)  # Transform event data into a dictionary
            if transformed_event:
                valid_dict_lines.append({
                    "timestamp": entry["timestamp"],
                    "message": remaining_message,
                    "event_type": event_type,
                    "data": transformed_event
                })
                remaining_message = remaining_message.replace(f"{event_type}({event_data})", "", 1)

        # Check if there are still remaining messages that couldn't be parsed into events
        if not valid_dict_lines or remaining_message.strip():
            empty_dict_lines.append({"timestamp": entry["timestamp"], "message": remaining_message.strip()})

    except Exception as e:
        print(f"Error extracting events: {e}")
        empty_dict_lines.append(entry)
