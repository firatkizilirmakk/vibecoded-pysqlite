import unittest
import os
import shutil
import time
import multiprocessing

# We need to import all the core components
from src.pysqlite.core.storage_engine import StorageEngine
from src.pysqlite.core.parser import Parser
from src.pysqlite.core.execution_engine import ExecutionEngine

# --- Helper function for the multiprocessing Isolation test ---
def worker_process_tries_to_read(db_path, queue):
    """
    This function runs in a separate process. It tries to read from the database
    and measures how long it takes. It will be blocked by the main process's lock.
    """
    start_time = time.time()
    try:
        # Each process needs its own engine instances
        storage = StorageEngine(database_path=db_path)
        parser = Parser()
        engine = ExecutionEngine(storage)
        
        # This SELECT will attempt to acquire a SHARED lock and will have to wait.
        engine.execute(parser.parse("SELECT * FROM accounts"))
        
    except Exception as e:
        # Put any exceptions in the queue to be seen by the main process
        queue.put(e)
    
    end_time = time.time()
    duration = end_time - start_time
    queue.put(duration)


class TestAcidCompliance(unittest.TestCase):
    """
    Test suite specifically for verifying ACID properties.
    """

    def setUp(self):
        self.test_db_dir = 'test_db_acid'
        if os.path.exists(self.test_db_dir):
            shutil.rmtree(self.test_db_dir)
        
        self.storage = StorageEngine(database_path=self.test_db_dir)
        self.parser = Parser()
        self.engine = ExecutionEngine(self.storage)

        # Create and populate a simple table
        self.engine.execute(self.parser.parse("CREATE TABLE accounts (acc_id INT PRIMARY KEY, balance INT)"))
        self.engine.execute(self.parser.parse("INSERT INTO accounts VALUES (101, 1000)"))

    def tearDown(self):
        # Ensure locks are released even if a test fails
        try:
            self.engine.storage_engine.db_locker.unlock()
        except Exception:
            pass
        if os.path.exists(self.test_db_dir):
            shutil.rmtree(self.test_db_dir)

    def test_atomicity_with_rollback(self):
        """
        Tests Atomicity: A manual ROLLBACK should undo all changes within a transaction.
        """
        self.engine.execute(self.parser.parse("BEGIN TRANSACTION"))
        self.engine.execute(self.parser.parse("UPDATE accounts SET balance = 50 WHERE acc_id = 101"))
        
        # Check state inside the transaction
        res = self.engine.execute(self.parser.parse("SELECT balance FROM accounts WHERE acc_id = 101"))
        self.assertEqual(res[0]['balance'], 50)

        # Now, roll back
        self.engine.execute(self.parser.parse("ROLLBACK"))

        # Verify that the original data is restored
        res_after_rollback = self.engine.execute(self.parser.parse("SELECT balance FROM accounts WHERE acc_id = 101"))
        self.assertEqual(res_after_rollback[0]['balance'], 1000)

    def test_atomicity_with_crash_recovery(self):
        """
        Tests Atomicity: Simulates a crash by not committing. On restart,
        the recovery mechanism should roll back the changes.
        """
        self.engine.execute(self.parser.parse("BEGIN TRANSACTION"))
        self.engine.execute(self.parser.parse("UPDATE accounts SET balance = 50 WHERE acc_id = 101"))
        
        # --- SIMULATE CRASH ---
        # A real crash would terminate the process, and the OS would release the lock.
        # We simulate this by manually unlocking before creating the new engine.
        self.engine.storage_engine.db_locker.unlock()
        
        print("\nSimulating crash and recovery...")
        # The __init__ of the new StorageEngine should trigger the recovery process.
        engine2 = ExecutionEngine(StorageEngine(database_path=self.test_db_dir))

        # Verify that the original data is still there in the new session
        res_after_crash = engine2.execute(self.parser.parse("SELECT balance FROM accounts WHERE acc_id = 101"))
        self.assertEqual(res_after_crash[0]['balance'], 1000)

    def test_durability_with_commit(self):
        """
        Tests Durability: After a COMMIT, changes must be permanent, even after a "restart".
        """
        self.engine.execute(self.parser.parse("BEGIN TRANSACTION"))
        self.engine.execute(self.parser.parse("UPDATE accounts SET balance = 500 WHERE acc_id = 101"))
        self.engine.execute(self.parser.parse("COMMIT"))

        # --- SIMULATE RESTART ---
        engine2 = ExecutionEngine(StorageEngine(database_path=self.test_db_dir))
        
        # Verify the new data is present in the new session
        res_after_restart = engine2.execute(self.parser.parse("SELECT balance FROM accounts WHERE acc_id = 101"))
        self.assertEqual(res_after_restart[0]['balance'], 500)

    def test_isolation_with_locking(self):
        """
        Tests Isolation: A writer (exclusive lock) should block a reader (shared lock).
        """
        # This test can be flaky on some systems, especially Windows.
        # It's a good demonstration but might need adjustment for CI/CD environments.
        if os.name == 'nt':
            self.skipTest("Skipping multiprocessing lock test on Windows due to platform differences.")

        queue = multiprocessing.Queue()

        # 1. Main process starts a transaction, acquiring an EXCLUSIVE lock.
        self.engine.execute(self.parser.parse("BEGIN TRANSACTION"))
        
        # 2. Start a separate process that will try to read from the database.
        #    It should be blocked until the main process commits.
        reader_process = multiprocessing.Process(
            target=worker_process_tries_to_read,
            args=(self.test_db_dir, queue)
        )
        reader_process.start()

        # 3. Main process "works" for a bit, holding the lock.
        print("\nMain process holds exclusive lock for 0.5s...")
        time.sleep(0.5)

        # 4. Main process commits, releasing the lock.
        self.engine.execute(self.parser.parse("COMMIT"))
        print("Main process released lock.")

        # 5. Wait for the reader process to finish.
        reader_process.join(timeout=2)

        # 6. Check the result from the reader process.
        result = queue.get()
        if isinstance(result, Exception):
            self.fail(f"Reader process failed with an exception: {result}")
        
        # The duration should be slightly more than the sleep time,
        # proving it had to wait for the lock to be released.
        duration = result
        print(f"Reader process was blocked for approximately {duration:.2f}s.")
        self.assertGreater(duration, 0.4)


if __name__ == '__main__':
    unittest.main()
