import unittest
import os
import shutil

from src.pysqlite.core.storage_engine import StorageEngine
from src.pysqlite.core.parser import Parser
from src.pysqlite.core.execution_engine import ExecutionEngine

class TestExecutionEngineComprehensive(unittest.TestCase):
    """
    Comprehensive integration test suite for the full database engine,
    covering CRUD, Joins, and complex WHERE clauses.
    """

    def setUp(self):
        """Set up a temporary database with sample data for each test."""
        self.test_db_dir = 'test_db_comprehensive'
        if os.path.exists(self.test_db_dir):
            shutil.rmtree(self.test_db_dir)
        
        self.storage = StorageEngine(database_path=self.test_db_dir)
        self.parser = Parser()
        self.engine = ExecutionEngine(self.storage)

        # --- Create and populate tables ---
        # Departments table
        self.engine.execute(self.parser.parse("CREATE TABLE departments (dept_id INT PRIMARY KEY, name STR, location STR)"))
        self.engine.execute(self.parser.parse("INSERT INTO departments VALUES (101, 'Engineering', 'New York')"))
        self.engine.execute(self.parser.parse("INSERT INTO departments VALUES (102, 'HR', 'London')"))
        self.engine.execute(self.parser.parse("INSERT INTO departments VALUES (103, 'Finance', 'Zurich')")) # Department with no employees

        # Employees table
        self.engine.execute(self.parser.parse("CREATE TABLE employees (emp_id INT PRIMARY KEY, name STR, role STR, salary INT, dept_id INT)"))
        self.engine.execute(self.parser.parse("INSERT INTO employees VALUES (1, 'Alice', 'Engineer', 120000, 101)"))
        self.engine.execute(self.parser.parse("INSERT INTO employees VALUES (2, 'Bob', 'Sr. Engineer', 150000, 101)"))
        self.engine.execute(self.parser.parse("INSERT INTO employees VALUES (3, 'Charlie', 'Recruiter', 90000, 102)"))
        self.engine.execute(self.parser.parse("INSERT INTO employees VALUES (4, 'David', 'Accountant', 110000, 103)"))
        self.engine.execute(self.parser.parse("INSERT INTO employees VALUES (5, 'Eve', 'Intern', 50000, 999)")) # Employee with no matching department

    def tearDown(self):
        """Clean up the temporary database after each test."""
        shutil.rmtree(self.test_db_dir)

    def test_update_statement(self):
        """Tests the UPDATE command."""
        # Give Alice a promotion
        query = "UPDATE employees SET role = 'Lead Engineer', salary = 140000 WHERE emp_id = 1"
        result = self.engine.execute(self.parser.parse(query))
        self.assertEqual(result, "1 row(s) updated.")

        # Verify the change
        select_query = "SELECT role, salary FROM employees WHERE emp_id = 1"
        updated_record = self.engine.execute(self.parser.parse(select_query))
        self.assertEqual(updated_record[0]['role'], 'Lead Engineer')
        self.assertEqual(updated_record[0]['salary'], 140000)

    def test_delete_statement(self):
        """Tests the DELETE command."""
        # Charlie leaves the company
        query = "DELETE FROM employees WHERE emp_id = 3"
        result = self.engine.execute(self.parser.parse(query))
        self.assertEqual(result, "1 row(s) deleted.")

        # Verify the deletion
        select_query = "SELECT * FROM employees WHERE emp_id = 3"
        deleted_record = self.engine.execute(self.parser.parse(select_query))
        self.assertEqual(len(deleted_record), 0)

        # Verify other records are untouched
        count_query = "SELECT COUNT(*) FROM employees"
        count_result = self.engine.execute(self.parser.parse(count_query))
        self.assertEqual(count_result[0]['COUNT(*)'], 4)

    def test_where_with_and(self):
        """Tests a WHERE clause with multiple AND conditions."""
        query = "SELECT name FROM employees WHERE role = 'Engineer' AND dept_id = 101 AND salary > 100000"
        result = self.engine.execute(self.parser.parse(query))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Alice')

    def test_where_with_or(self):
        """Tests a WHERE clause with an OR condition."""
        # Find employees who are in HR or have a salary over 140,000
        query = "SELECT name, role FROM employees WHERE dept_id = 102 OR salary > 140000"
        result = self.engine.execute(self.parser.parse(query))
        self.assertEqual(len(result), 2)
        names = {r['name'] for r in result}
        self.assertSetEqual(names, {'Bob', 'Charlie'})

    def test_inner_join(self):
        """Tests an INNER JOIN, which should exclude employees/depts without a match."""
        query = "SELECT employees.name, departments.name FROM employees INNER JOIN departments ON employees.dept_id = departments.dept_id ORDER BY employees.name"
        result = self.engine.execute(self.parser.parse(query))
        
        # Should be 4 results: Alice, Bob, Charlie, David. Excludes Eve (no dept) and Finance (no employees).
        self.assertEqual(len(result), 4)
        
        expected = [
            {'employees.name': 'Alice', 'departments.name': 'Engineering'},
            {'employees.name': 'Bob', 'departments.name': 'Engineering'},
            {'employees.name': 'Charlie', 'departments.name': 'HR'},
            {'employees.name': 'David', 'departments.name': 'Finance'}
        ]
        self.assertCountEqual(result, expected)

    def test_left_join(self):
        """Tests a LEFT JOIN, which should include all employees, even those without a matching department."""
        query = "SELECT employees.name, departments.name FROM employees LEFT JOIN departments ON employees.dept_id = departments.dept_id ORDER BY employees.name"
        result = self.engine.execute(self.parser.parse(query))

        # Should be 5 results: Alice, Bob, Charlie, David, AND Eve.
        self.assertEqual(len(result), 5)

        # Find Eve's record to check for None
        eve_record = next(r for r in result if r['employees.name'] == 'Eve')
        self.assertIsNone(eve_record['departments.name'])

        # Find Alice's record to check for a successful join
        alice_record = next(r for r in result if r['employees.name'] == 'Alice')
        self.assertEqual(alice_record['departments.name'], 'Engineering')

if __name__ == '__main__':
    unittest.main()
