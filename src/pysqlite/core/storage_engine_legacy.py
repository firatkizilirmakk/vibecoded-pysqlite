import os
import struct
import json

# Define a constant for the page size. 4KB is a common size.
PAGE_SIZE = 4096

class StorageEngine:
    """
    The StorageEngine is responsible for all physical data storage.
    It translates logical requests (e.g., insert a row) into
    physical operations on files (e.g., write bytes to a page).
    """

    def __init__(self, database_path='.'):
        """
        Initializes the storage engine.

        Args:
            database_path (str): The path to the directory where database files are stored.
        """
        if not os.path.exists(database_path):
            os.makedirs(database_path)
        self.database_path = database_path

    def _get_table_path(self, table_name):
        """Constructs the full file path for a given table name."""
        return os.path.join(self.database_path, f"{table_name}.db")

    def create_table(self, table_name, columns):
        """
        Creates a new file for a table and writes the schema to the first page.
        """
        table_path = self._get_table_path(table_name)
        if os.path.exists(table_path):
            raise FileExistsError(f"Table '{table_name}' already exists.")

        with open(table_path, 'wb') as f:
            schema_data = json.dumps(columns).encode('utf-8')
            if len(schema_data) > PAGE_SIZE:
                raise ValueError("Schema is too large for a single page.")
            page_data = schema_data.ljust(PAGE_SIZE, b'\x00')
            f.write(page_data)

    def get_schema(self, table_name):
        """
        Retrieves the schema for a given table from its file.

        Args:
            table_name (str): The name of the table.

        Returns:
            dict: The schema of the table.
        """
        table_path = self._get_table_path(table_name)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name}' does not exist.")

        with open(table_path, 'rb') as f:
            schema_page = f.read(PAGE_SIZE)
            # Find the end of the JSON string (it's null-padded)
            schema_json_str = schema_page.split(b'\x00', 1)[0].decode('utf-8')
            return json.loads(schema_json_str)

    def insert_record(self, table_name, record):
        """
        Inserts a record into a table.
        """
        table_path = self._get_table_path(table_name)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name}' does not exist.")

        record_bytes = json.dumps(record).encode('utf-8')
        record_len = len(record_bytes)

        with open(table_path, 'r+b') as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()

            if file_size == PAGE_SIZE: # Only schema page exists
                num_records = 1
                free_space_ptr = 8 + 4 + record_len
                header = struct.pack('!II', num_records, free_space_ptr)
                data = struct.pack('!I', record_len) + record_bytes
                page_data = (header + data).ljust(PAGE_SIZE, b'\x00')
                f.write(page_data)
            else:
                # Simplification: append a new page for each record.
                num_records = 1
                free_space_ptr = 8 + 4 + record_len
                header = struct.pack('!II', num_records, free_space_ptr)
                data = struct.pack('!I', record_len) + record_bytes
                page_data = (header + data).ljust(PAGE_SIZE, b'\x00')
                f.write(page_data)

    def get_all_records(self, table_name):
        """
        Retrieves all records from a table.
        """
        table_path = self._get_table_path(table_name)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name}' does not exist.")

        with open(table_path, 'rb') as f:
            f.seek(PAGE_SIZE) # Skip schema page
            while True:
                page_data = f.read(PAGE_SIZE)
                if not page_data:
                    break
                
                header = page_data[:8]
                if not header or len(header) < 8:
                    continue

                num_records, _ = struct.unpack('!II', header)

                if num_records > 0:
                    record_len_bytes = page_data[8:12]
                    record_len = struct.unpack('!I', record_len_bytes)[0]
                    record_data = page_data[12:12 + record_len]
                    record = json.loads(record_data.decode('utf-8'))
                    yield record