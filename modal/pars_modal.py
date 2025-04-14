import logging
import json

from dto.data_parser_json import (
    parse_log_line,
    find_json_like_structure,
    transform_to_json,
    contains_special_chars,
    contains_brackets,
)
from dto.data_parser_taple import (
    contains_brackets_tuple,
)

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_log_lines(filepath):
    """Processes all lines in the log file and classifies them into different categories."""
    without_special_chars = []
    with_special_chars = []
    with_brackets = []
    without_brackets = []
    empty_dict_lines = []  # For empty dictionaries
    with_brackets_taple = []  # For lines with brackets but not a valid JSON object

    logging.info(f"Opening file: {filepath}")
    with open(filepath, encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            # Parse the log line
            entry = parse_log_line(line)
            logging.debug(f"Parsed entry: {entry}")

            # Classify lines based on the presence of special characters
            if contains_special_chars(entry["message"]):
                with_special_chars.append(entry)

                if contains_brackets_tuple(entry["message"]):
                    with_brackets_taple.append(entry)
                elif contains_brackets(entry["message"]):
                    with_brackets.append(entry)
                else:
                    without_brackets.append(entry)
            else:
                without_special_chars.append(entry)

    valid_dict_lines = []
    for entry in with_brackets:
        try:
            # Find JSON-like fragments in the message
            json_fragments = list(find_json_like_structure(entry["message"]))
            remaining_message = entry["message"]
            for fragment in json_fragments:
                # Transform the fragment into a JSON-compatible format
                transformed_fragment = transform_to_json(fragment)
                try:
                    # Attempt to load the transformed JSON data
                    log_dict = json.loads(transformed_fragment)
                    valid_dict_lines.append({
                        "timestamp": entry["timestamp"],
                        "message": remaining_message.replace(fragment, "", 1),
                        "parsed_data": log_dict
                    })
                    remaining_message = remaining_message.replace(fragment, "", 1)
                except json.JSONDecodeError:
                    logging.error(f"Error decoding JSON for fragment: {fragment}")
                    continue

            # Check if there are any remaining messages that couldn't be parsed into JSON
            if not valid_dict_lines or remaining_message.strip():
                empty_dict_lines.append({"timestamp": entry["timestamp"], "message": remaining_message.strip()})
                logging.debug(f"Remaining unprocessed message: {remaining_message.strip()}")

        except Exception as e:
            logging.error(f"Error processing log entry: {e}")
            empty_dict_lines.append(entry)

    # Return all categorized log lines
    logging.info("Log parsing complete.")
    return (without_special_chars, with_special_chars, valid_dict_lines, empty_dict_lines, without_brackets,
            with_brackets_taple)
