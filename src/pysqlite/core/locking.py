import os
import time

# We need to use different modules for file locking depending on the OS.
if os.name == 'nt':  # For Windows
    import msvcrt
else:  # For Unix-like systems (Linux, macOS)
    import fcntl

class Locker:
    """
    A simple, cross-platform file locking utility, made more robust for
    multiprocessing by re-opening the file handle on each lock attempt.
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_handle = None

    def lock(self, exclusive=False, timeout=10):
        """
        Acquires a lock on the file.
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Open the file in append mode on each attempt. This is safer
                # than 'w' mode as it won't truncate the file, and it will
                # create the file if it doesn't exist.
                self.file_handle = open(self.file_path, 'a')
                
                if os.name == 'nt':
                    # On Windows, we lock a specific region of the file.
                    # Locking the first byte is sufficient.
                    mode = msvcrt.LK_NBLCK if exclusive else msvcrt.LK_NBRLCK
                    self.file_handle.seek(0)
                    msvcrt.locking(self.file_handle.fileno(), mode, 1)
                else:
                    # On Unix, the lock applies to the whole file.
                    mode = fcntl.LOCK_EX | fcntl.LOCK_NB if exclusive else fcntl.LOCK_SH | fcntl.LOCK_NB
                    fcntl.flock(self.file_handle, mode)
                
                # If we reach here, the lock was acquired successfully.
                return
            except (IOError, BlockingIOError):
                # The file is locked by another process.
                # Close our handle and wait before retrying with a new one.
                if self.file_handle:
                    self.file_handle.close()
                time.sleep(0.1)
        
        # If we exit the loop, we timed out.
        raise TimeoutError(f"Could not acquire lock on {self.file_path} within {timeout} seconds.")

    def unlock(self):
        """Releases the lock."""
        if self.file_handle:
            if os.name == 'nt':
                self.file_handle.seek(0)
                msvcrt.locking(self.file_handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self.file_handle, fcntl.LOCK_UN)
            
            self.file_handle.close()
            self.file_handle = None
