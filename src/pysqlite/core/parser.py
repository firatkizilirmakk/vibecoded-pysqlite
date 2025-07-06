import re

class Parser:
    """
    The Parser is responsible for parsing a raw SQL query string into a
    structured representation that the execution engine can understand.
    Now supports LEFT JOIN.
    """

    def parse(self, query_string):
        query_string = query_string.strip()
        if query_string.upper().startswith('UPDATE'):
            return self._parse_update(query_string)
        if query_string.upper().startswith('DELETE FROM'):
            return self._parse_delete(query_string)
        if query_string.upper().startswith('CREATE INDEX'):
            return self._parse_create_index(query_string)
        if query_string.upper().startswith('WITH'):
            return self._parse_with(query_string)
        if query_string.upper().startswith('CREATE TABLE'):
            return self._parse_create_table(query_string)
        if query_string.upper().startswith('INSERT INTO'):
            return self._parse_insert(query_string)
        if query_string.upper().startswith('SELECT'):
            return self._parse_select(query_string)
        raise ValueError(f"Unsupported or invalid SQL query: {query_string}")

    def _parse_from_clause(self, from_str):
        """
        Parses the FROM clause, handling INNER and LEFT JOINs.
        """
        # Regex now captures the join type (INNER or LEFT)
        join_match = re.search(r'\s+(INNER|LEFT)\s+JOIN\s+', from_str, re.IGNORECASE)
        
        if not join_match:
            return {'type': 'table', 'name': from_str.strip()}

        join_type = join_match.group(1).upper()
        
        # Split by the full join phrase
        parts = re.split(r'\s+(?:INNER|LEFT)\s+JOIN\s+', from_str, maxsplit=1, flags=re.IGNORECASE)
        left_table_str = parts[0]
        
        on_split = re.split(r'\s+ON\s+', parts[1], maxsplit=1, flags=re.IGNORECASE)
        if len(on_split) != 2:
            raise ValueError("JOIN clause requires an ON condition.")
        
        right_table_str, on_condition_str = on_split

        on_match = re.match(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', on_condition_str.strip())
        if not on_match:
            raise ValueError("Invalid ON condition format. Expected: table1.col1 = table2.col2")
        
        t1_alias, t1_col, t2_alias, t2_col = on_match.groups()

        return {
            'type': 'join',
            'join_type': join_type, # Now dynamic
            'left': {'type': 'table', 'name': left_table_str.strip()},
            'right': {'type': 'table', 'name': right_table_str.strip()},
            'on': {
                'left_table': t1_alias,
                'left_column': t1_col,
                'right_table': t2_alias,
                'right_column': t2_col
            }
        }

    # ... (All other methods remain unchanged from the previous formatted version) ...
    def _parse_where(self, where_str):
        if not where_str:
            return None
        or_parts = re.split(r'\s+OR\s+', where_str, flags=re.IGNORECASE)
        if len(or_parts) == 1:
            and_parts = re.split(r'\s+AND\s+', or_parts[0], flags=re.IGNORECASE)
            if len(and_parts) == 1:
                return self._parse_single_condition(and_parts[0])
            else:
                return {'type': 'AND', 'conditions': [self._parse_single_condition(p) for p in and_parts]}
        else:
            or_conditions = []
            for part in or_parts:
                and_parts = re.split(r'\s+AND\s+', part, flags=re.IGNORECASE)
                if len(and_parts) == 1:
                    or_conditions.append(self._parse_single_condition(and_parts[0]))
                else:
                    or_conditions.append({'type': 'AND', 'conditions': [self._parse_single_condition(p) for p in and_parts]})
            return {'type': 'OR', 'conditions': or_conditions}

    def _parse_single_condition(self, condition_str):
        match = re.match(r'((?:\w+\.)?\w+)\s*(>=|<=|!=|=|>|<)\s*(.+)', condition_str.strip())
        if not match:
            raise ValueError(f"Unsupported WHERE condition format: '{condition_str}'")
        column_full, operator, value = match.groups()
        value, value_stripped = value.strip(), value.strip("'\"")
        try:
            processed_value = float(value_stripped)
            if processed_value.is_integer():
                processed_value = int(processed_value)
        except ValueError:
            processed_value = value_stripped
        return {'type': 'condition', 'column': column_full, 'operator': operator, 'value': processed_value}

    def _parse_select(self, query):
        parts = re.split(r'\s+ORDER BY\s+', query, maxsplit=1, flags=re.IGNORECASE)
        main_part, order_by_str = (parts[0], parts[1]) if len(parts) > 1 else (parts[0], None)
        parts = re.split(r'\s+GROUP BY\s+', main_part, maxsplit=1, flags=re.IGNORECASE)
        main_part, group_by_str = (parts[0], parts[1]) if len(parts) > 1 else (parts[0], None)
        parts = re.split(r'\s+WHERE\s+', main_part, maxsplit=1, flags=re.IGNORECASE)
        main_part, where_clause_str = (parts[0], parts[1]) if len(parts) > 1 else (parts[0], None)
        select_match = re.match(r'SELECT\s+(.+?)\s+FROM\s+(.+)', main_part, re.IGNORECASE)
        if not select_match:
            raise ValueError("Invalid SELECT syntax.")
        columns_str, from_str = select_match.groups()
        from_clause = self._parse_from_clause(from_str)
        columns = self._parse_select_columns(columns_str)
        where_clause = self._parse_where(where_clause_str)
        group_by = [col.strip() for col in group_by_str.split(',')] if group_by_str else None
        order_by = self._parse_order_by(order_by_str) if order_by_str else None
        return {'type': 'SELECT', 'columns': columns, 'from': from_clause, 'where': where_clause, 'group_by': group_by, 'order_by': order_by}

    def _parse_select_columns(self, columns_str):
        if columns_str.strip() == '*':
            return [{'type': 'wildcard'}]
        parsed_columns = []
        agg_regex = re.compile(r'(\w+)\((.+)\)', re.IGNORECASE)
        for col_part in columns_str.split(','):
            col_part = col_part.strip()
            agg_match = agg_regex.match(col_part)
            if agg_match:
                func_name, arg = agg_match.groups()
                func_name = func_name.upper()
                if func_name not in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']:
                    raise ValueError(f"Unsupported aggregate function: {func_name}")
                parsed_columns.append({'type': 'aggregate', 'function': func_name, 'argument': arg.strip(), 'alias': col_part})
            else:
                col_match = re.match(r'(?:(\w+)\.)?(\w+)', col_part)
                if not col_match:
                    raise ValueError(f"Invalid column name: {col_part}")
                table, column = col_match.groups()
                parsed_columns.append({'type': 'column', 'table': table, 'name': column})
        return parsed_columns

    def _parse_update(self, query):
        parts = re.split(r'\s+WHERE\s+', query, maxsplit=1, flags=re.IGNORECASE)
        main_part, where_clause_str = (parts[0], parts[1]) if len(parts) > 1 else (parts[0], None)
        match = re.match(r'UPDATE (\w+) SET (.+)', main_part, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid UPDATE syntax. Expected: UPDATE table_name SET col1 = val1, ...")
        table_name, set_str = match.groups()
        set_values = {}
        for pair in set_str.split(','):
            col_match = re.match(r'(\w+)\s*=\s*(.+)', pair.strip())
            if not col_match:
                raise ValueError(f"Invalid SET clause format: '{pair}'")
            col, val = col_match.groups()
            val_stripped = val.strip().strip("'\"")
            try:
                processed_val = float(val_stripped)
                if processed_val.is_integer():
                    processed_val = int(processed_val)
            except ValueError:
                processed_val = val_stripped
            set_values[col] = processed_val
        return {'type': 'UPDATE', 'table_name': table_name, 'set': set_values, 'where': self._parse_where(where_clause_str) if where_clause_str else None}

    def _parse_delete(self, query):
        parts = re.split(r'\s+WHERE\s+', query, maxsplit=1, flags=re.IGNORECASE)
        main_part, where_clause_str = (parts[0], parts[1]) if len(parts) > 1 else (parts[0], None)
        match = re.match(r'DELETE FROM (\w+)', main_part, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DELETE syntax. Expected: DELETE FROM table_name ...")
        return {'type': 'DELETE', 'table_name': match.group(1), 'where': self._parse_where(where_clause_str) if where_clause_str else None}

    def _parse_create_index(self, query):
        match = re.match(r'CREATE INDEX (\w+) ON (\w+) \((\w+)\)', query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid CREATE INDEX syntax. Expected: CREATE INDEX index_name ON table_name (column_name)")
        index_name, table_name, column_name = match.groups()
        return {'type': 'CREATE_INDEX', 'index_name': index_name, 'table_name': table_name, 'column_name': column_name}

    def _parse_create_table(self, query):
        match = re.match(r'CREATE TABLE (\w+)\s*\((.+)\)', query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")
        table_name, columns_str = match.groups()
        columns, primary_key = {}, None
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            is_pk = False
            if re.search(r'\s+PRIMARY\s+KEY', col_def, re.IGNORECASE):
                if primary_key is not None:
                    raise ValueError("Multiple PRIMARY KEY definitions are not allowed.")
                is_pk = True
                col_def = re.sub(r'\s+PRIMARY\s+KEY', '', col_def, flags=re.IGNORECASE).strip()
            parts = col_def.split()
            if len(parts) != 2:
                raise ValueError(f"Invalid column definition: '{col_def}'")
            col_name, col_type = parts
            columns[col_name] = col_type.upper()
            if is_pk:
                primary_key = col_name
        if primary_key is None:
            raise ValueError("No PRIMARY KEY defined for the table. A primary key is required.")
        return {'type': 'CREATE_TABLE', 'table_name': table_name, 'columns': columns, 'primary_key': primary_key}

    def _parse_with(self, query):
        query = query.lstrip()[4:].strip()
        ctes, cte_match = [], re.match(r'(\w+)\s+AS\s+\(', query, re.IGNORECASE)
        while cte_match:
            cte_name = cte_match.group(1)
            open_paren, start_index, end_index = 1, cte_match.end(), cte_match.end()
            for i, char in enumerate(query[start_index:]):
                if char == '(':
                    open_paren += 1
                elif char == ')':
                    open_paren -= 1
                if open_paren == 0:
                    end_index = start_index + i
                    break
            if open_paren != 0:
                raise ValueError("Mismatched parentheses in CTE definition.")
            subquery_str = query[start_index:end_index]
            ctes.append({'name': cte_name, 'query': self.parse(subquery_str)})
            query = query[end_index + 1:].strip()
            if query.startswith(','):
                query = query[1:].strip()
                cte_match = re.match(r'(\w+)\s+AS\s+\(', query, re.IGNORECASE)
            else:
                cte_match = None
        return {'type': 'WITH', 'ctes': ctes, 'main_query': self.parse(query)}

    def _parse_insert(self, query):
        match = re.match(r'INSERT INTO (\w+) VALUES \((.+)\)', query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT INTO syntax")
        table_name, values_str = match.groups()
        values, processed_values = [v.strip() for v in values_str.split(',')], []
        for v in values:
            v_stripped = v.strip().strip("'\"")
            try:
                processed_val = float(v_stripped)
                if processed_val.is_integer():
                    processed_val = int(processed_val)
                processed_values.append(processed_val)
            except ValueError:
                processed_values.append(v_stripped)
        return {'type': 'INSERT', 'table_name': table_name, 'values': processed_values}

    def _parse_order_by(self, order_by_str):
        parts = order_by_str.strip().split()
        column, direction = parts[0], 'ASC'
        if len(parts) > 1:
            direction = parts[1].upper()
            if direction not in ['ASC', 'DESC']:
                raise ValueError(f"Invalid ORDER BY direction: '{direction}'")
        return {'column': column, 'direction': direction}
