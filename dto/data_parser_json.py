import json
import re
from pathlib import Path


def contains_special_chars(text: str) -> bool:
    """Проверяет наличие хотя бы одного символа: { [ ( :"""
    return any(sym in text for sym in ['{', '[', '(', ':'])


def contains_brackets(text: str) -> bool:
    """Проверяет наличие скобок: { [ ("""
    return any(sym in text for sym in ['{', '[', '('])


import ast


def transform_to_json(log_entry: str) -> str:
    """Преобразует строку с Python-подобным словарем в JSON-совместимую строку"""
    try:
        # Пытаемся интерпретировать строку как Python-словарь
        parsed_dict = ast.literal_eval(log_entry)
        # Преобразуем Python-словарь в JSON-строку
        return json.dumps(parsed_dict)
    except (ValueError, SyntaxError):
        # Если не удалось интерпретировать как словарь, возвращаем исходную строку
        return log_entry


def find_json_like_structure(text: str):
    """Ищет все JSON-подобные структуры в строке (включая вложенные)"""
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
        yield text[start:]  # Возвращаем неполную структуру, если остались открытые скобки


def fix_bracket_balance(text: str) -> str:
    """Проверяет сбалансированность скобок и добавляет недостающие скобки, если они не сбалансированы"""
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

    # Если в стеке остались открывающие скобки, добавляем закрывающие в конец строки
    result.extend('}' * len(stack))

    return ''.join(result)  # Возвращаем строку с добавленными скобками


def parse_log_lines(filepath):
    without_special_chars = []
    with_special_chars = []
    with_brackets = []
    without_brackets = []
    empty_dict_lines = []  # Для пустых словарей

    with open(filepath, encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            if '\t' in line:
                timestamp, message = line.split('\t', 1)
            else:
                timestamp, message = None, line

            entry = {
                "timestamp": timestamp,
                "message": message
            }

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
            print(f"Processing entry: {entry['message'][:100]}...")  # Печатаем начало сообщения
            # Ищем все JSON-подобные структуры между фигурными скобками
            json_fragments = list(find_json_like_structure(entry["message"]))
            print(f"Found {len(json_fragments)} JSON-like structures")

            remaining_message = entry["message"]
            for fragment in json_fragments:
                print(f"Processing fragment: {fragment[:100]}...")  # Печатаем начало фрагмента
                # Преобразуем строку в правильный формат JSON
                transformed_fragment = transform_to_json(fragment)
                print(
                    f"Transformed fragment: {transformed_fragment[:100]}...")  # Печатаем начало преобразованного фрагмента

                # Пытаемся распарсить текущий фрагмент как JSON
                try:
                    log_dict = json.loads(transformed_fragment)
                    valid_dict_lines.append({
                        "timestamp": entry["timestamp"],
                        "message": remaining_message.replace(fragment, "", 1),  # Удаляем обработанный фрагмент
                        "parsed_data": log_dict
                    })
                    remaining_message = remaining_message.replace(fragment, "", 1)  # Обновляем оставшееся сообщение
                    print("Successfully parsed JSON")
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                    # Не добавляем в empty_dict_lines, так как мы обрабатываем все фрагменты

            if not valid_dict_lines or remaining_message.strip():
                empty_dict_lines.append({"timestamp": entry["timestamp"], "message": remaining_message.strip()})

        except Exception as e:
            print(f"Exception occurred: {e}")
            empty_dict_lines.append(entry)

    print(f"Total valid_dict_lines: {len(valid_dict_lines)}")
    print(f"Total empty_dict_lines: {len(empty_dict_lines)}")

    return without_special_chars, with_special_chars, valid_dict_lines, empty_dict_lines, without_brackets

# Пример использования
log_path = Path(r"D:\Program Files (x86)\PyProject\QA_TestAssignment\testLogs\changes_output.txt")
without_special_chars, with_special_chars, valid_dict_lines, empty_dict_lines, without_brackets = parse_log_lines(
    log_path)

# Проверка: выводим первые 20 строк с валидными словарями
print("\n=== Строки с валидными словарями (парсинг JSON) ===")
for entry in valid_dict_lines:

    print(f"Timestamp: {entry['timestamp']}")
    print(f"Message: {entry['message']}")
    print("Parsed Data:")

    # Печать каждого найденного ключа и его значения в Parsed Data
    parsed_data = entry['parsed_data']
    for key, value in parsed_data.items():
        print(f"  {key}: {json.dumps(value, indent=4)}")

    print("-" * 50)
