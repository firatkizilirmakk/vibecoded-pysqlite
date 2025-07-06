import unittest
from src.pysqlite.core.parser import Parser

class TestParserComprehensive(unittest.TestCase):
    """
    Comprehensive unit test suite for the Parser class, covering all features.
    """

    def setUp(self):
        self.parser = Parser()

    def test_parse_create_table_with_pk(self):
        """Tests parsing CREATE TABLE with a user-defined PRIMARY KEY."""
        query = "CREATE TABLE users (user_id INT PRIMARY KEY, email STR, age INT)"
        parsed = self.parser.parse(query)
        self.assertEqual(parsed['type'], 'CREATE_TABLE')
        self.assertEqual(parsed['table_name'], 'users')
        self.assertEqual(parsed['primary_key'], 'user_id')
        self.assertIn('user_id', parsed['columns'])

    def test_parse_create_index(self):
        """Tests parsing the CREATE INDEX command."""
        query = "CREATE INDEX idx_email ON users (email)"
        parsed = self.parser.parse(query)
        self.assertEqual(parsed['type'], 'CREATE_INDEX')
        self.assertEqual(parsed['index_name'], 'idx_email')
        self.assertEqual(parsed['table_name'], 'users')
        self.assertEqual(parsed['column_name'], 'email')

    def test_parse_update(self):
        """Tests parsing the UPDATE command."""
        query = "UPDATE users SET age = 30, email = 'new@e.com' WHERE user_id = 1"
        parsed = self.parser.parse(query)
        self.assertEqual(parsed['type'], 'UPDATE')
        self.assertEqual(parsed['table_name'], 'users')
        self.assertEqual(parsed['set'], {'age': 30, 'email': 'new@e.com'})
        self.assertEqual(parsed['where']['column'], 'user_id')

    def test_parse_delete(self):
        """Tests parsing the DELETE command."""
        query = "DELETE FROM users WHERE age > 65"
        parsed = self.parser.parse(query)
        self.assertEqual(parsed['type'], 'DELETE')
        self.assertEqual(parsed['table_name'], 'users')
        self.assertEqual(parsed['where']['operator'], '>')

    def test_parse_joins(self):
        """Tests parsing INNER and LEFT JOIN clauses."""
        # INNER JOIN
        query = "SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id"
        parsed = self.parser.parse(query)
        self.assertEqual(parsed['from']['type'], 'join')
        self.assertEqual(parsed['from']['join_type'], 'INNER')
        self.assertEqual(parsed['from']['on']['left_column'], 'dept_id')

        # LEFT JOIN
        query = "SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id"
        parsed = self.parser.parse(query)
        self.assertEqual(parsed['from']['type'], 'join')
        self.assertEqual(parsed['from']['join_type'], 'LEFT')

    def test_parse_complex_where(self):
        """Tests parsing a WHERE clause with AND and OR."""
        query = "SELECT * FROM users WHERE age > 25 AND role = 'Engineer' OR dept = 'HR'"
        parsed = self.parser.parse(query)
        where = parsed['where']
        self.assertEqual(where['type'], 'OR')
        self.assertEqual(len(where['conditions']), 2)
        
        # Check the AND part
        and_clause = where['conditions'][0]
        self.assertEqual(and_clause['type'], 'AND')
        self.assertEqual(len(and_clause['conditions']), 2)
        self.assertEqual(and_clause['conditions'][0]['column'], 'age')
        
        # Check the OR part
        hr_condition = where['conditions'][1]
        self.assertEqual(hr_condition['column'], 'dept')

if __name__ == '__main__':
    unittest.main()
