import os
import pickle
import struct
from .locking import Locker

PAGE_SIZE = 4096
BTREE_ORDER = 16 

class BTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.values = []
        self.children = []

class StorageEngine:
    def __init__(self, database_path='.'):
        if not os.path.exists(database_path):
            os.makedirs(database_path)
        self.database_path = database_path
        self.transaction_active = False
        self.journaled_pages = {}
        self.db_locker = Locker(os.path.join(self.database_path, ".db_lock"))
        self._recover()

    def _get_table_path(self, table_name):
        return os.path.join(self.database_path, f"{table_name}.db")

    def _get_index_path(self, index_name):
        return os.path.join(self.database_path, f"{index_name}.idx")

    def _get_journal_path(self, db_path):
        return db_path + "-journal"

    def _recover(self):
        self.db_locker.lock(exclusive=True)
        try:
            for filename in os.listdir(self.database_path):
                if filename.endswith("-journal"):
                    db_path = os.path.join(self.database_path, filename.replace("-journal", ""))
                    journal_path = os.path.join(self.database_path, filename)
                    print(f"Recovery needed: Found journal file {journal_path}. Rolling back changes.")
                    self._perform_rollback(db_path, journal_path)
        finally:
            self.db_locker.unlock()

    def begin_transaction(self):
        if self.transaction_active:
            raise Exception("Transaction already in progress.")
        self.db_locker.lock(exclusive=True)
        self.transaction_active = True
        self.journaled_pages = {}

    def commit_transaction(self):
        if not self.transaction_active:
            return
        for db_path in self.journaled_pages.keys():
            journal_path = self._get_journal_path(db_path)
            if os.path.exists(journal_path):
                os.remove(journal_path)
        self.transaction_active = False
        self.journaled_pages = {}
        self.db_locker.unlock()

    def rollback_transaction(self):
        if not self.transaction_active:
            return
        for db_path in self.journaled_pages.keys():
            journal_path = self._get_journal_path(db_path)
            self._perform_rollback(db_path, journal_path)
        self.transaction_active = False
        self.journaled_pages = {}
        self.db_locker.unlock()

    def _perform_rollback(self, db_path, journal_path):
        if not os.path.exists(journal_path):
            return
        try:
            with open(journal_path, 'rb') as jf, open(db_path, 'r+b') as dbf:
                while True:
                    header_bytes = jf.read(4)
                    if not header_bytes:
                        break
                    page_num = struct.unpack('!I', header_bytes)[0]
                    page_data = jf.read(PAGE_SIZE)
                    offset = page_num * PAGE_SIZE
                    dbf.seek(offset)
                    dbf.write(page_data)
        finally:
            if os.path.exists(journal_path):
                os.remove(journal_path)

    def _write_page(self, file_path, page_num, node):
        if self.transaction_active:
            if file_path not in self.journaled_pages:
                self.journaled_pages[file_path] = set()
            
            if page_num not in self.journaled_pages[file_path]:
                journal_path = self._get_journal_path(file_path)
                with open(file_path, 'rb') as dbf, open(journal_path, 'ab') as jf:
                    dbf.seek(page_num * PAGE_SIZE)
                    original_data = dbf.read(PAGE_SIZE)
                    jf.write(struct.pack('!I', page_num))
                    jf.write(original_data)
                self.journaled_pages[file_path].add(page_num)

        with open(file_path, 'r+b') as f:
            offset = page_num * PAGE_SIZE
            f.seek(offset)
            data = pickle.dumps(node)
            f.write(data.ljust(PAGE_SIZE, b'\x00'))
    
    def create_table(self, table_name, columns, primary_key):
        table_path = self._get_table_path(table_name)
        if os.path.exists(table_path):
            raise FileExistsError(f"Table '{table_name}' already exists.")
        metadata = {'schema': columns, 'primary_key': primary_key, 'indexes': {}, 'root_page': 1, 'next_page': 2}
        with open(table_path, 'wb') as f:
            f.write(b'\x00' * PAGE_SIZE * 2)
        self._write_page(table_path, 0, metadata)
        root = BTreeNode(is_leaf=True)
        self._write_page(table_path, 1, root)
    
    def insert_record(self, table_name, record):
        metadata = self.get_table_metadata(table_name)
        primary_key_col = metadata['primary_key']
        pk_value = record.get(primary_key_col)
        if pk_value is None:
            raise ValueError(f"Record must have a value for the primary key column '{primary_key_col}'.")
        table_path = self._get_table_path(table_name)
        self._btree_insert(table_path, pk_value, record)
        for index_name, column_name in metadata['indexes'].items():
            index_path = self._get_index_path(index_name)
            key = record.get(column_name)
            if key is not None:
                self._btree_insert(index_path, key, pk_value)
    
    def _read_page(self, file, page_num):
        offset = page_num * PAGE_SIZE
        file.seek(offset)
        data = file.read(PAGE_SIZE)
        if not data.strip(b'\x00'):
            return None
        return pickle.loads(data)

    def get_table_metadata(self, table_name):
        if not self.transaction_active:
            self.db_locker.lock(exclusive=False)
        try:
            table_path = self._get_table_path(table_name)
            if not os.path.exists(table_path):
                raise FileNotFoundError(f"Table '{table_name}' does not exist.")
            with open(table_path, 'rb') as f:
                return self._read_page(f, 0)
        finally:
            if not self.transaction_active:
                self.db_locker.unlock()
    
    def _btree_insert(self, file_path, key, value):
        with open(file_path, 'rb') as f:
            metadata = self._read_page(f, 0)
            root_page_num = metadata['root_page']
            root = self._read_page(f, root_page_num)
        if len(root.keys) == (2 * BTREE_ORDER - 1):
            with open(file_path, 'r+b') as f:
                metadata = self._read_page(f, 0)
                new_root_page_num = metadata['next_page']
                metadata['next_page'] += 1
                old_root, new_root = root, BTreeNode()
                new_root.children.append(root_page_num)
                self._split_child(file_path, new_root, 0, old_root, metadata)
                self._write_page(file_path, new_root_page_num, new_root)
                metadata['root_page'] = new_root_page_num
                self._write_page(file_path, 0, metadata)
                root = new_root
        self._insert_non_full(file_path, root, key, value, metadata)

    def _insert_non_full(self, file_path, node, key, value, metadata):
        i = len(node.keys) - 1
        if node.is_leaf:
            node.keys.append(None)
            node.values.append(None)
            while i >= 0 and key < node.keys[i]:
                node.keys[i+1] = node.keys[i]
                node.values[i+1] = node.values[i]
                i -= 1
            node.keys[i+1] = key
            node.values[i+1] = value
            self._write_page(file_path, self._find_page_of_node(file_path, metadata['root_page'], node.keys[0]), node)
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            with open(file_path, 'rb') as f:
                child_node = self._read_page(f, node.children[i])
            if len(child_node.keys) == (2 * BTREE_ORDER - 1):
                self._split_child(file_path, node, i, child_node, metadata)
                if key > node.keys[i]:
                    i += 1
            with open(file_path, 'rb') as f:
                self._insert_non_full(file_path, self._read_page(f, node.children[i]), key, value, metadata)

    def _split_child(self, file_path, parent_node, child_index, child_node, metadata):
        new_node = BTreeNode(is_leaf=child_node.is_leaf)
        new_node_page = metadata['next_page']
        metadata['next_page'] += 1
        mid_index = BTREE_ORDER - 1
        parent_node.keys.insert(child_index, child_node.keys[mid_index])
        parent_node.children.insert(child_index + 1, new_node_page)
        new_node.keys = child_node.keys[mid_index+1:]
        child_node.keys = child_node.keys[:mid_index]
        if not child_node.is_leaf:
            new_node.children = child_node.children[mid_index+1:]
            child_node.children = child_node.children[:mid_index+1]
        else:
            new_node.values = child_node.values[mid_index+1:]
            child_node.values = child_node.values[:mid_index]
        self._write_page(file_path, self._find_page_of_node(file_path, metadata['root_page'], parent_node.keys[0]), parent_node)
        self._write_page(file_path, self._find_page_of_node(file_path, metadata['root_page'], child_node.keys[0] if child_node.keys else -1, is_child=True), child_node)
        self._write_page(file_path, new_node_page, new_node)
    
    def _find_page_of_node(self, file_path, start_page, key, is_delete=False, is_child=False):
        with open(file_path, 'rb') as f:
            q = [start_page]
            while q:
                curr_page_num = q.pop(0)
                curr_node = self._read_page(f, curr_page_num)
                if curr_node and curr_node.keys:
                    if is_delete:
                        if key in curr_node.keys:
                            return curr_page_num
                    elif curr_node.keys[0] == key:
                        return curr_page_num
                if curr_node and not curr_node.is_leaf:
                    q.extend(curr_node.children)
            if is_child:
                return start_page + 1
            return start_page
    
    def update_record(self, table_name, pk_value, new_data):
        old_record = self.search_pk(table_name, pk_value)
        if not old_record:
            raise ValueError(f"No record found with primary key {pk_value} to update.")
        self.delete_record(table_name, pk_value, old_record)
        updated_record = old_record.copy()
        updated_record.update(new_data)
        self.insert_record(table_name, updated_record)

    def delete_record(self, table_name, pk_value, record_data=None):
        metadata = self.get_table_metadata(table_name)
        if record_data is None:
            record_data = self.search_pk(table_name, pk_value)
        if not record_data:
            return
        for index_name, column_name in metadata['indexes'].items():
            index_path = self._get_index_path(index_name)
            key_to_delete = record_data.get(column_name)
            if key_to_delete is not None:
                self._btree_delete(index_path, key_to_delete)
        table_path = self._get_table_path(table_name)
        self._btree_delete(table_path, pk_value)
    
    def _btree_delete(self, file_path, key):
        with open(file_path, 'r+b') as f:
            metadata = self._read_page(f, 0)
            root_page_num = metadata['root_page']
            root = self._read_page(f, root_page_num)
            self._delete_recursive(f, root, key, metadata, file_path) # Pass file_path
            if len(root.keys) == 0 and not root.is_leaf:
                metadata['root_page'] = root.children[0]
                self._write_page(file_path, 0, metadata)

    def _delete_recursive(self, file, node, key, metadata, file_path): # Accept file_path
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        if i < len(node.keys) and node.keys[i] == key:
            if node.is_leaf:
                node.keys.pop(i)
                node.values.pop(i)
                # THE FIX IS HERE: Use the passed file_path directly
                self._write_page(file_path, self._find_page_of_node(file_path, metadata['root_page'], key, is_delete=True), node)
            else:
                raise NotImplementedError("Deletion from internal B-Tree nodes is not implemented.")
        else:
            if node.is_leaf:
                return
            child_node = self._read_page(file, node.children[i])
            if child_node:
                self._delete_recursive(file, child_node, key, metadata, file_path) # Pass file_path down
    
    def search_pk(self, table_name, pk_value):
        return self._btree_search(self._get_table_path(table_name), pk_value)

    def search_index(self, index_name, key):
        return self._btree_search(self._get_index_path(index_name), key)

    def get_all_records(self, table_name):
        if not self.transaction_active:
            self.db_locker.lock(exclusive=False)
        try:
            yield from self._btree_get_all(table_name)
        finally:
            if not self.transaction_active:
                self.db_locker.unlock()
    
    def _btree_get_all(self, table_name):
        with open(self._get_table_path(table_name), 'rb') as f:
            metadata = self._read_page(f, 0)
            root = self._read_page(f, metadata['root_page'])
            yield from self._traverse_all(f, root)
    
    def _btree_search(self, file_path, key):
        with open(file_path, 'rb') as f:
            metadata = self._read_page(f, 0)
            node = self._read_page(f, metadata['root_page'])
            while node and not node.is_leaf:
                i = 0
                while i < len(node.keys) and key > node.keys[i]:
                    i += 1
                node = self._read_page(f, node.children[i])
            if not node:
                return None
            i = 0
            while i < len(node.keys) and key > node.keys[i]:
                i += 1
            if i < len(node.keys) and node.keys[i] == key:
                return node.values[i]
        return None

    def _traverse_all(self, file, node):
        if node:
            if node.is_leaf:
                for value in node.values:
                    yield value
            else:
                for i in range(len(node.children)):
                    yield from self._traverse_all(file, self._read_page(file, node.children[i]))
