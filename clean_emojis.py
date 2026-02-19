import os
import re

def remove_non_ascii(text):
    # Mapping of common symbols/emojis to text markers
    replacements = {
        '[OK]': '[OK]',
        '[WARN]': '[WARN]',
        '[PATH]': '[PATH]',
        '[DOC]': '[DOC]',
        '[MATCH]': '[MATCH]',
        '[DOC]': '[FILE]',
        '[START]': '[START]',
        '[SEARCH]': '[SEARCH]',
        '[INFO]': '[INFO]',
        '[STATS]': '[DATA]',
        '[DATE]': '[DATE]',
        '[INFO]': '[INFO]',
        '[INFO]': '[INFO]',
        '[STATS]': '[STATS]',
        '[STATS]': '[STATS]',
        '[SAVE]': '[SAVE]',
        '[CUT]': '[CUT]',
        '[INFO]': '[INFO]',
        '[INFO]': '[INFO]',
        '[ERR]': '[ERR]',
        '[ERR]': '[ERR]',
        '[OK]': '[OK]',
        '[WARN]': '[WARN]',
        '\U0001f4c2': '[PATH]',
        '\u2705': '[OK]',
        '\u274c': '[ERR]',
        '\u2713': '[OK]',
        '\u2717': '[ERR]',
        '\U0001f680': '[START]',
        '\U0001f50d': '[SEARCH]',
        '\U0001f4c4': '[DOC]',
        '\U0001f4ca': '[STATS]',
        '\U0001f4d3': '[DOC]',
        '\U0001f5d2': '[DOC]',
        '\U0001f4ab': '[INFO]',
        '\U0001f504': '[MATCH]',
        '\U0001f4da': '[INFO]',
        '\xa0': ' ', # Replace non-breaking space with space
    }
    
    for char, rep in replacements.items():
        text = text.replace(char, rep)
    
    # Remove any other non-ASCII characters
    return re.sub(r'[^\x00-\x7F]+', ' ', text)

def process_directory(directory):
    for root, dirs, files in os.walk(directory):
        if 'venv' in root or '.git' in root or '__pycache__' in root:
            continue
            
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    cleaned_content = remove_non_ascii(content)
                    
                    if cleaned_content != content:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(cleaned_content)
                        print(f"Cleaned: {file_path}")
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    process_directory(current_dir)
