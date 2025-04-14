from modal.pars_modal import parse_log_lines
from pathlib import Path
import json

def main():
    log_path = Path(r"D:\Program Files (x86)\PyProject\QA_TestAssignment\testLogs\changes_output.txt")
    without_special_chars, with_special_chars, valid_dict_lines, empty_dict_lines, without_brackets, with_brackets_tuple = parse_log_lines(log_path)

    print("\n=== Lines with valid dictionaries (JSON parsing) ===")
    for entry in valid_dict_lines:
        print(f"Timestamp: {entry['timestamp']}")
        print(f"Message: {entry['message']}")
        print("Parsed Data:")
        for key, value in entry['parsed_data'].items():
            print(f"  {key}: {json.dumps(value, indent=4)}")
        print("-" * 50)

if __name__ == "__main__":
    main()
