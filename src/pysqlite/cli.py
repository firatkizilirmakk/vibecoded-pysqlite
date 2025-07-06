import argparse
import os

try:
    import readline
except ImportError:
    print("Module 'readline' not available. Command history will not be saved.")
    readline = None

from .core.storage_engine import StorageEngine
from .core.parser import Parser
from .core.execution_engine import ExecutionEngine

def print_table(data):
    """
    Prints a list of dictionaries in a well-formatted table.
    """
    if not data:
        print("(no rows)")
        return

    headers = data[0].keys()
    widths = {h: len(h) for h in headers}
    for row in data:
        for h in headers:
            widths[h] = max(widths[h], len(str(row.get(h, ''))))

    header_line = " | ".join(f"{h:<{widths[h]}}" for h in headers)
    separator_line = "-+-".join("-" * widths[h] for h in headers)

    print(header_line)
    print(separator_line)

    for row in data:
        row_line = " | ".join(f"{str(row.get(h, '')):<{widths[h]}}" for h in headers)
        print(row_line)

def main():
    """
    The main function for the command-line interface.
    """
    arg_parser = argparse.ArgumentParser(description="A simple SQLite-like database in Python.")
    arg_parser.add_argument(
        'db_dir', 
        type=str, 
        help="The directory to store database files."
    )
    args = arg_parser.parse_args()

    db_dir = args.db_dir
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"Database directory '{db_dir}' created.")

    history_file = os.path.join(db_dir, '.pysqlite_history')
    if readline:
        try:
            readline.read_history_file(history_file)
        except FileNotFoundError:
            pass

    storage = StorageEngine(database_path=db_dir)
    parser = Parser()
    engine = ExecutionEngine(storage)

    print("pysqlite version 1.2.0")
    print(f"Connected to database at '{os.path.abspath(db_dir)}'.")
    print("Enter '.exit' to quit or '.tables' to list tables.")

    while True:
        try:
            query = input("pysqlite> ").strip()

            if not query:
                continue

            if query.lower() == '.exit':
                break
            
            if query.lower() == '.tables':
                table_files = [f for f in os.listdir(db_dir) if f.endswith('.db')]
                tables = [os.path.splitext(f)[0] for f in table_files]
                if tables:
                    for table_name in sorted(tables):
                        print(table_name)
                else:
                    print("(no tables found)")
                continue
            
            parsed_command = parser.parse(query)
            result = engine.execute(parsed_command, data_context={})

            if result is not None:
                if isinstance(result, list):
                    print_table(result)
                else:
                    print(result)

        except (ValueError, FileNotFoundError, NotImplementedError, FileExistsError) as e:
            print(f"Error: {e}")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    if readline:
        readline.write_history_file(history_file)
    print("\nExiting pysqlite. Goodbye!")

if __name__ == '__main__':
    main()
