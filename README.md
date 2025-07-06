# py-sqlite: A Relational Database from Scratch in Python

a pure vibecoding or context engineering :D based sqlite implementation in python.

py-sqlite is a custom-built, SQLite-like relational database written entirely in Python. This project was developed as a step-by-step exploration of the core components of a database management system, from low-level file I/O and data structures to a high-level SQL query processor.

It features a persistent, page-based storage engine using B-Trees for efficient indexing, a recursive-descent SQL parser, and a query execution engine with a simple optimizer. Most importantly, it is **fully ACID compliant**, ensuring data integrity through atomic transactions and file locking.

---

## Features

This database supports a rich subset of SQL, allowing for complex data manipulation and querying in a safe, reliable environment.

#### Full ACID Compliance
* **Atomicity:** Transactions are "all or nothing." Using a rollback journal, any operation that is interrupted (e.g., by a crash) is automatically undone, ensuring the database is never left in a corrupted state.
* **Consistency:** The database state is always valid, enforced by the atomic nature of transactions.
* **Isolation:** Concurrent access is managed safely using a file-locking mechanism. Multiple processes can read the database at the same time, but write operations acquire an exclusive lock, preventing race conditions.
* **Durability:** Once a `COMMIT` is executed, the changes are permanently saved to disk and will survive a system crash or power loss.

#### Core Commands (Full CRUD)
* **`CREATE TABLE`**: Supports various data types and user-defined `PRIMARY KEY` constraints.
* **`INSERT INTO`**: Adds new records to a table.
* **`SELECT`**: A powerful `SELECT` implementation with support for specific columns, `*` wildcard, and complex clauses.
* **`UPDATE`**: Modifies existing records based on conditions.
* **`DELETE FROM`**: Removes records from a table based on conditions.

#### Indexing
* **Primary Key Index (B-Tree)**: All tables are automatically indexed by their primary key using a B-Tree for fast lookups.
* **Secondary Indexes**: Supports the creation of secondary indexes on any column with the `CREATE INDEX` command to accelerate queries.

#### Advanced Querying
* **Complex `WHERE` Clause**: Filter records using a full range of comparison operators (`=`, `!=`, `>`, `<`, `>=`, `<=`) combined with logical operators `AND` and `OR`.
* **`JOIN` Operations**: Combine rows from two tables using `INNER JOIN` and `LEFT JOIN` with an `ON` condition.
* **Aggregation**: Perform calculations across a set of rows using `COUNT`, `SUM`, 'AVG', `MIN`, and `MAX`.
* **`GROUP BY`**: Group rows that have the same values into summary rows.
* **`ORDER BY`**: Sort the results of a query in `ASC` (ascending) or `DESC` (descending) order.
* **Common Table Expressions (CTEs)**: Define temporary, named result sets using the `WITH` clause for cleaner, more readable queries.

#### Command Line Interface (CLI)
* An interactive REPL (Read-Eval-Print Loop) for executing queries.
* Command history support (use arrow keys to navigate previous queries).
* Meta-commands like `.exit` to quit and `.tables` to list all tables.
* Well-formatted table output for query results.

---

## Project Structure

```text
py-sqlite/
├── pyproject.toml
├── README.md
├── src/
│   └── pysqlite/
│       ├── __init__.py
│       ├── cli.py
│       └── core/
│           ├── __init__.py
│           ├── locking.py
│           ├── parser.py
│           ├── storage_engine.py
│           └── execution_engine.py
└── tests/
    ├── __init__.py
    ├── test_acid_compliance.py
    ├── test_parser.py
    ├── test_storage_engine.py
    └── test_execution_engine.py
```

Installation and Usage1. InstallationAfter cloning the repository, navigate to the root directory and run:# Using pip
pip install .

# Or using the faster uv
uv pip install .
2. Running the CLIThis installation creates a pysqlite command. You can now run the database from any directory on your system:pysqlite my_company_db
3. Running the TestsTo run the complete test suite, navigate to the root directory and use Python's built-in unittest discovery tool:python -m unittest discover
Supported SQL Syntax ExamplesData Definition-- Create a table with a primary key
CREATE TABLE employees (emp_id INT PRIMARY KEY, name STR, role STR, salary INT, dept_id INT);

-- Create a secondary index for faster lookups on the 'role' column
CREATE INDEX idx_role ON employees (role);
Data Manipulation-- Insert a new record
INSERT INTO employees VALUES (1, 'Alice', 'Engineer', 120000, 101);

-- Update an existing record
UPDATE employees SET salary = 125000 WHERE emp_id = 1;

-- Delete a record
DELETE FROM employees WHERE emp_id = 1;
Transaction Control-- Start a transaction
BEGIN TRANSACTION;

-- Make some changes
UPDATE employees SET salary = 130000 WHERE emp_id = 1;
INSERT INTO employees VALUES (2, 'Bob', 'Manager', 150000, 101);

-- Make the changes permanent
COMMIT;

-- Or, undo all changes since the transaction began
ROLLBACK;

Queries-- Select all data from a table
```
SELECT * FROM employees;

-- Select specific columns with a filter
SELECT name, role FROM employees WHERE salary > 100000;

-- Use AND and OR in a WHERE clause
SELECT * FROM employees WHERE (role = 'Engineer' AND salary > 100000) OR dept_id = 102;

-- Perform an INNER JOIN
SELECT employees.name, departments.name FROM employees INNER JOIN departments ON employees.dept_id = departments.dept_id;

-- Perform a LEFT JOIN to include all employees, even those without a department
SELECT employees.name, departments.name FROM employees LEFT JOIN departments ON employees.dept_id = departments.dept_id;

-- Use aggregation and grouping
SELECT dept_id, COUNT(*), AVG(salary) FROM employees GROUP BY dept_id;

-- A complex query combining multiple clauses
SELECT d.name, COUNT(e.emp_id)
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.name
ORDER BY d.name;

-- Use a Common Table Expression (CTE)
WITH high_earners AS (SELECT name, salary FROM employees WHERE salary > 150000)
SELECT * FROM high_earners;
```
