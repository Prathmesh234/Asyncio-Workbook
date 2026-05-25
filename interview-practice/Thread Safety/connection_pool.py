## =====================================================================
## INTERVIEW SYSTEM DESIGN & CODING QUESTION
## =====================================================================
##
## QUESTION:
## Design and implement a thread-safe, blocking connection pool.
##
## REQUIREMENTS:
## 1. The pool has a maximum limit on open connections (max_size).
## 2. If a thread requests a connection and all are in use, the thread must block 
##    (wait) until another thread returns one.
## 3. Support an optional checkout timeout; if exceeded, raise a TimeoutError.
## 4. When a thread returns a connection, it should wake up a waiting thread.
## 5. Implement context manager support so connections are automatically returned 
##    after use.
##
## =====================================================================
import asyncio
import threading
class ConnectionPool:
    def __init__(self, max_size):
        self.max_size = max_size
        self.semaphore = threading.Semaphore(max_size)
        self.lock = threading.Lock()
        self.pool = [f"conn-{i}" for i in range(max_size)]
    def checkout(self):
        self.semaphore.acquire()
        with self.lock:
            return self.pool.pop()
        self.semaphore.release()
    def checkin(self, conn):
        with self.lock:
            self.pool.append(conn)
        self.semaphore.release()


