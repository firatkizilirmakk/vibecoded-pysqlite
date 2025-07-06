import collections
import operator as op

class ExecutionEngine:
    """
    The ExecutionEngine orchestrates database operations.
    """

    def __init__(self, storage_engine):
        self.storage_engine = storage_engine
        self.in_transaction = False

    def execute(self, parsed_command, data_context=None):
        command_type = parsed_command.get('type')
        data_context = data_context or {}
        if command_type == 'BEGIN':
            return self._execute_begin()
        if command_type == 'COMMIT':
            return self._execute_commit()
        if command_type == 'ROLLBACK':
            return self._execute_rollback()
        
        is_write_operation = command_type in ['UPDATE', 'DELETE', 'CREATE_INDEX', 'CREATE_TABLE', 'INSERT']
        if is_write_operation and not self.in_transaction:
            self._execute_begin()
            try:
                result = self._dispatch_command(command_type, parsed_command, data_context)
                self._execute_commit()
                return result
            except Exception as e:
                self._execute_rollback()
                raise e
        else:
            return self._dispatch_command(command_type, parsed_command, data_context)

    def _dispatch_command(self, command_type, parsed_command, data_context):
        if command_type == 'UPDATE':
            return self._execute_update(parsed_command)
        if command_type == 'DELETE':
            return self._execute_delete(parsed_command)
        if command_type == 'CREATE_INDEX':
            return self._execute_create_index(parsed_command)
        if command_type == 'WITH':
            return self._execute_with(parsed_command, data_context)
        if command_type == 'CREATE_TABLE':
            return self._execute_create_table(parsed_command)
        if command_type == 'INSERT':
            return self._execute_insert(parsed_command)
        if command_type == 'SELECT':
            return self._execute_select(parsed_command, data_context)
        raise ValueError(f"Unsupported command type: {command_type}")

    def _execute_begin(self):
        if self.in_transaction:
            raise ValueError("Transaction already in progress.")
        self.storage_engine.begin_transaction()
        self.in_transaction = True
        return "Transaction started."

    def _execute_commit(self):
        if not self.in_transaction:
            raise ValueError("No transaction to commit.")
        self.storage_engine.commit_transaction()
        self.in_transaction = False
        return "Transaction committed."

    def _execute_rollback(self):
        if not self.in_transaction:
            raise ValueError("No transaction to roll back.")
        self.storage_engine.rollback_transaction()
        self.in_transaction = False
        return "Transaction rolled back."

    def _execute_update(self, command):
        table_name, where_clause, set_values = command['table_name'], command.get('where'), command['set']
        records_to_update = self._find_records_for_modification(table_name, where_clause)
        if not records_to_update:
            return "0 rows updated."
        metadata = self.storage_engine.get_table_metadata(table_name)
        primary_key_col = metadata['primary_key']
        for record in records_to_update:
            pk_value = record.get(primary_key_col)
            self.storage_engine.update_record(table_name, pk_value, set_values)
        return f"{len(records_to_update)} row(s) updated."

    def _execute_delete(self, command):
        table_name, where_clause = command['table_name'], command.get('where')
        if not where_clause:
            raise ValueError("DELETE statement must have a WHERE clause (for safety).")
        records_to_delete = self._find_records_for_modification(table_name, where_clause)
        if not records_to_delete:
            return "0 rows deleted."
        metadata = self.storage_engine.get_table_metadata(table_name)
        primary_key_col = metadata['primary_key']
        for record in records_to_delete:
            pk_value = record.get(primary_key_col)
            self.storage_engine.delete_record(table_name, pk_value, record)
        return f"{len(records_to_delete)} row(s) deleted."

    def _find_records_for_modification(self, table_name, where_clause):
        select_command = {'from': {'type': 'table', 'name': table_name}, 'columns': [{'type': 'wildcard'}], 'where': where_clause}
        return self._execute_select(select_command, data_context={})
    
    def _execute_select(self, command, data_context):
        from_clause = command['from']
        where_clause = command.get('where')
        initial_records = []
        can_use_index = (where_clause and from_clause['type'] == 'table' and where_clause['type'] != 'OR')
        index_used = False
        if can_use_index:
            table_name = from_clause['name']
            index_check_condition = None
            if where_clause['type'] == 'condition':
                index_check_condition = where_clause
            elif where_clause['type'] == 'AND':
                index_check_condition = where_clause['conditions'][0]
            if index_check_condition and index_check_condition['operator'] == '=':
                try:
                    metadata = self.storage_engine.get_table_metadata(table_name)
                    where_col = index_check_condition['column']
                    if where_col == metadata['primary_key']:
                        record = self.storage_engine.search_pk(table_name, index_check_condition['value'])
                        if record:
                            initial_records = [record]
                        index_used = True
                    else:
                        for index_name, column_name in metadata['indexes'].items():
                            if where_col == column_name:
                                pk_value = self.storage_engine.search_index(index_name, index_check_condition['value'])
                                if pk_value:
                                    record = self.storage_engine.search_pk(table_name, pk_value)
                                    if record:
                                        initial_records = [record]
                                index_used = True
                                break
                except FileNotFoundError:
                     raise ValueError(f"Table '{table_name}' does not exist.")
        if not index_used:
            if from_clause['type'] == 'join':
                initial_records = self._execute_join(from_clause, data_context)
            else:
                initial_records = self._full_scan_with_filter(from_clause['name'], None, data_context)
        results = self._full_scan_with_filter(None, where_clause, records_to_filter=initial_records)
        select_parts, group_by_cols, order_by = command['columns'], command.get('group_by'), command.get('order_by')
        if group_by_cols:
            results = self._perform_grouping(select_parts, group_by_cols, results)
        else:
            if any(p['type'] == 'aggregate' for p in select_parts):
                results = self._perform_aggregation(select_parts, results)
        if not any(p['type'] == 'aggregate' for p in select_parts):
            results = self._project_columns(results, select_parts)
        if order_by and results:
            sort_column = order_by['column']
            actual_sort_key = None
            for key in results[0].keys():
                if key.endswith(f".{sort_column}") or key == sort_column:
                    actual_sort_key = key
                    break
            if not actual_sort_key:
                raise ValueError(f"Cannot order by column '{sort_column}' as it is not in the final result set.")
            results.sort(key=lambda x: x[actual_sort_key], reverse=(order_by['direction'] == 'DESC'))
        return results

    def _execute_join(self, join_clause, data_context):
        left_table_name, right_table_name, on_cond, join_type = join_clause['left']['name'], join_clause['right']['name'], join_clause['on'], join_clause['join_type']
        left_records = self._full_scan_with_filter(left_table_name, None, data_context)
        right_records = self._full_scan_with_filter(right_table_name, None, data_context)
        joined_records = []
        right_schema = self.storage_engine.get_table_metadata(right_table_name)['schema']
        null_right_row = {f"{right_table_name}.{col}": None for col in right_schema}
        for l_row in left_records:
            match_found = False
            for r_row in right_records:
                if l_row.get(on_cond['left_column']) == r_row.get(on_cond['right_column']):
                    match_found = True
                    new_row = {}
                    for col, val in l_row.items():
                        new_row[f"{left_table_name}.{col}"] = val
                    for col, val in r_row.items():
                        new_row[f"{right_table_name}.{col}"] = val
                    joined_records.append(new_row)
            if not match_found and join_type == 'LEFT':
                new_row = {}
                for col, val in l_row.items():
                    new_row[f"{left_table_name}.{col}"] = val
                new_row.update(null_right_row)
                joined_records.append(new_row)
        return joined_records

    def _project_columns(self, records, select_parts):
        if not records:
            return []
        if any(p['type'] == 'wildcard' for p in select_parts):
            return records
        projected_records = []
        for record in records:
            new_record = {}
            for part in select_parts:
                if part['type'] == 'column':
                    col_key = f"{part['table']}.{part['name']}" if part['table'] else part['name']
                    val_found = False
                    if col_key in record:
                        new_record[col_key] = record[col_key]
                        val_found = True
                    else:
                        for key, val in record.items():
                            if key.endswith(f".{part['name']}"):
                                new_record[key] = val
                                val_found = True
                                break
                    if not val_found:
                        new_record[part['name']] = None
            projected_records.append(new_record)
        return projected_records

    def _full_scan_with_filter(self, table_name, where_clause, data_context={}, records_to_filter=None):
        if records_to_filter is not None:
            all_records = records_to_filter
        elif table_name:
            if table_name in data_context:
                all_records = data_context[table_name]
            else:
                try:
                    all_records = self.storage_engine.get_all_records(table_name)
                except FileNotFoundError:
                    raise ValueError(f"Table '{table_name}' does not exist.")
        elif where_clause:
             raise ValueError("No records provided for filtering.")
        else:
            return []
        if not where_clause:
            return list(all_records)
        filtered_records = []
        for record in all_records:
            if self._evaluate_where_clause(record, where_clause):
                filtered_records.append(record)
        return filtered_records

    def _evaluate_where_clause(self, record, clause):
        clause_type = clause.get('type')
        if clause_type == 'OR':
            return any(self._evaluate_where_clause(record, cond) for cond in clause['conditions'])
        if clause_type == 'AND':
            return all(self._evaluate_where_clause(record, cond) for cond in clause['conditions'])
        if clause_type == 'condition':
            ops = {'=': op.eq, '!=': op.ne, '<': op.lt, '<=': op.le, '>': op.gt, '>=': op.ge}
            op_func = ops.get(clause['operator'])
            if not op_func:
                return False
            col_name_full = clause['column']
            record_val, val_found = None, False
            if col_name_full in record:
                record_val = record[col_name_full]
                val_found = True
            else:
                for key, val in record.items():
                    if key.endswith(f".{col_name_full}"):
                        record_val = val
                        val_found = True
                        break
            if not val_found:
                return False
            if record_val is None:
                return False
            try:
                return op_func(record_val, clause['value'])
            except TypeError:
                return False
        return False

    def _execute_create_index(self, command):
        index_name, table_name, column_name = command['index_name'], command['table_name'], command['column_name']
        self.storage_engine.create_index(index_name, table_name, column_name)
        return f"Index '{index_name}' created on table '{table_name}'."

    def _execute_with(self, command, data_context):
        new_data_context = data_context.copy()
        for cte in command['ctes']:
            cte_result = self.execute(cte['query'], data_context=new_data_context)
            new_data_context[cte['name']] = cte_result
        return self.execute(command['main_query'], data_context=new_data_context)

    def _execute_create_table(self, command):
        table_name, columns, primary_key = command['table_name'], command['columns'], command['primary_key']
        self.storage_engine.create_table(table_name, columns, primary_key)
        return f"Table '{table_name}' created successfully."

    def _execute_insert(self, command):
        table_name, values = command['table_name'], command['values']
        try:
            metadata = self.storage_engine.get_table_metadata(table_name)
            schema = metadata['schema']
        except FileNotFoundError:
            raise ValueError(f"Table '{table_name}' does not exist.")
        column_names = list(schema.keys())
        if len(values) != len(column_names):
            raise ValueError(f"Insert error: table '{table_name}' has {len(column_names)} columns, but {len(values)} values were provided.")
        record = dict(zip(column_names, values))
        self.storage_engine.insert_record(table_name, record)
        return "1 row inserted."

    def _perform_grouping(self, select_parts, group_by_cols, records):
        selected_cols = {p['name'] for p in select_parts if p['type'] == 'column'}
        if not selected_cols.issubset(set(group_by_cols)):
            raise ValueError("Selected column is not in GROUP BY clause and is not an aggregate function.")
        groups = collections.defaultdict(list)
        for record in records:
            group_key = tuple(record.get(col) for col in group_by_cols)
            groups[group_key].append(record)
        final_results = []
        aggregates = [p for p in select_parts if p['type'] == 'aggregate']
        for group_key, group_records in groups.items():
            result_row = dict(zip(group_by_cols, group_key))
            if aggregates:
                result_row.update(self._perform_aggregation(aggregates, group_records)[0])
            final_results.append(result_row)
        return final_results

    def _perform_aggregation(self, aggregates, records):
        if not records and any(agg['function'] != 'COUNT' for agg in aggregates):
            return [{agg['alias']: None for agg in aggregates}]
        result_row = {}
        for agg in aggregates:
            func, arg, alias = agg['function'], agg['argument'], agg['alias']
            if func == 'COUNT':
                result_row[alias] = len(records) if arg == '*' else sum(1 for r in records if r.get(arg) is not None)
            else:
                values = [r.get(arg) for r in records if r.get(arg) is not None and isinstance(r.get(arg), (int, float))]
                if not values:
                    result_row[alias] = None
                    continue
                if func == 'SUM':
                    result_row[alias] = sum(values)
                elif func == 'AVG':
                    result_row[alias] = sum(values) / len(values)
                elif func == 'MIN':
                    result_row[alias] = min(values)
                elif func == 'MAX':
                    result_row[alias] = max(values)
        return [result_row]
