import os
import pickle

PAGE_SIZE = 4096
BTREE_ORDER = 16 

class BTreeNode:
    """Represents a node in the B-Tree."""
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.values = []
        self.children = []

class StorageEngine:
    """
    A StorageEngine that now supports full CRUD operations (Create, Read, Update, Delete).
    """
    def __init__(self, database_path='.'):
        if not os.path.exists(database_path):
            os.makedirs(database_path)
        self.database_path = database_path

    def create_table(self, table_name, columns, primary_key):
        table_path = self._get_table_path(table_name)
        if os.path.exists(table_path):
            raise FileExistsError(f"Table '{table_name}' already exists.")
        metadata = {'schema': columns, 'primary_key': primary_key, 'indexes': {}, 'root_page': 1, 'next_page': 2}
        with open(table_path, 'wb') as f:
            self._write_page(f, 0, metadata)
            root = BTreeNode(is_leaf=True)
            self._write_page(f, 1, root)

    def create_index(self, index_name, table_name, column_name):
        table_path, index_path = self._get_table_path(table_name), self._get_index_path(index_name)
        if os.path.exists(index_path):
            raise FileExistsError(f"Index '{index_name}' already exists.")
        metadata = self.get_table_metadata(table_name)
        if column_name not in metadata['schema']:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table_name}'.")
        index_metadata = {'root_page': 1, 'next_page': 2}
        with open(index_path, 'wb') as f:
            self._write_page(f, 0, index_metadata)
            root = BTreeNode(is_leaf=True)
            self._write_page(f, 1, root)
        primary_key_col = metadata['primary_key']
        for record in self.get_all_records(table_name):
            key, pk_value = record.get(column_name), record.get(primary_key_col)
            if key is not None and pk_value is not None:
                self._btree_insert(index_path, key, pk_value)
        metadata['indexes'][index_name] = column_name
        with open(table_path, 'r+b') as f:
            self._write_page(f, 0, metadata)

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

    def search_pk(self, table_name, pk_value):
        return self._btree_search(self._get_table_path(table_name), pk_value)

    def search_index(self, index_name, key):
        return self._btree_search(self._get_index_path(index_name), key)

    def get_table_metadata(self, table_name):
        table_path = self._get_table_path(table_name)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name}' does not exist.")
        with open(table_path, 'rb') as f:
            return self._read_page(f, 0)

    def get_all_records(self, table_name):
        with open(self._get_table_path(table_name), 'rb') as f:
            metadata = self._read_page(f, 0)
            root = self._read_page(f, metadata['root_page'])
            yield from self._traverse_all(f, root)

    def _get_table_path(self, table_name):
        return os.path.join(self.database_path, f"{table_name}.db")

    def _get_index_path(self, index_name):
        return os.path.join(self.database_path, f"{index_name}.idx")

    def _read_page(self, file, page_num):
        offset = page_num * PAGE_SIZE
        file.seek(offset)
        data = file.read(PAGE_SIZE)
        if not data.strip(b'\x00'):
            return None
        return pickle.loads(data)

    def _write_page(self, file, page_num, node):
        offset = page_num * PAGE_SIZE
        file.seek(offset)
        data = pickle.dumps(node)
        file.write(data.ljust(PAGE_SIZE, b'\x00'))

    def _btree_delete(self, file_path, key):
        with open(file_path, 'r+b') as f:
            metadata = self._read_page(f, 0)
            root_page_num = metadata['root_page']
            root = self._read_page(f, root_page_num)
            
            self._delete_recursive(f, root, key, metadata)
            
            if len(root.keys) == 0 and not root.is_leaf:
                metadata['root_page'] = root.children[0]
                self._write_page(f, 0, metadata)

    def _delete_recursive(self, file, node, key, metadata):
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        if i < len(node.keys) and node.keys[i] == key:
            if node.is_leaf:
                node.keys.pop(i)
                node.values.pop(i)
                self._write_page(file, self._find_page_of_node(file, metadata['root_page'], key, is_delete=True), node)
            else:
                raise NotImplementedError("Deletion from internal B-Tree nodes is not implemented.")
        else:
            if node.is_leaf:
                return
            
            child_node = self._read_page(file, node.children[i])
            if child_node:
                self._delete_recursive(file, child_node, key, metadata)

    def _btree_insert(self, file_path, key, value):
        with open(file_path, 'r+b') as f:
            metadata = self._read_page(f, 0)
            root_page_num = metadata['root_page']
            root = self._read_page(f, root_page_num)
            if len(root.keys) == (2 * BTREE_ORDER - 1):
                new_root_page_num = metadata['next_page']
                metadata['next_page'] += 1
                old_root, new_root = root, BTreeNode()
                new_root.children.append(root_page_num)
                self._split_child(f, new_root, 0, old_root, metadata)
                self._write_page(f, new_root_page_num, new_root)
                metadata['root_page'] = new_root_page_num
                self._write_page(f, 0, metadata)
                root = new_root
            self._insert_non_full(f, root, key, value, metadata)

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

    def _insert_non_full(self, file, node, key, value, metadata):
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
            self._write_page(file, self._find_page_of_node(file, metadata['root_page'], node.keys[0]), node)
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            child_node = self._read_page(file, node.children[i])
            if len(child_node.keys) == (2 * BTREE_ORDER - 1):
                self._split_child(file, node, i, child_node, metadata)
                if key > node.keys[i]:
                    i += 1
            self._insert_non_full(file, self._read_page(file, node.children[i]), key, value, metadata)

    def _split_child(self, file, parent_node, child_index, child_node, metadata):
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
        self._write_page(file, self._find_page_of_node(file, metadata['root_page'], parent_node.keys[0]), parent_node)
        self._write_page(file, self._find_page_of_node(file, metadata['root_page'], child_node.keys[0] if child_node.keys else -1, is_child=True), child_node)
        self._write_page(file, new_node_page, new_node)

    def _traverse_all(self, file, node):
        if node:
            if node.is_leaf:
                for value in node.values:
                    yield value
            else:
                for i in range(len(node.children)):
                    yield from self._traverse_all(file, self._read_page(file, node.children[i]))

    def _find_page_of_node(self, file, start_page, key, is_delete=False, is_child=False):
        q = [start_page]
        while q:
            curr_page_num = q.pop(0)
            curr_node = self._read_page(file, curr_page_num)
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
