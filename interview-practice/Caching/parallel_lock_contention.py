from asyncio import locks
from asyncio import locks
from typing import OrderedDict
from collections import deque
import time 
import asyncio
import threading
'''
We have to make the LRU cache thread safe for concurrent get and put
Then we have to reduce the lock contention with lock stripping 
The issue is we have a single lock and in a multi processing env acquire and losing thread is a huge sequential burden 
So in order to prevent this we will have 4 locks 
lock0, lock1, lock2, lock3
each lock will take care of hash(key) % 4 so key = 0,1 lock0 ,  key=2,3 - lock1 etc..
We will also have to divide the cache accodingly so the cache becomes a list of sub caches 

'''
class LRUCache:
    def __init__(self, size, is_daemon):
        # we will divide into buckets
       self.cache = [{}, {}, {}, {}]
       # now we need all of the threads in a list 
       self.locks = [threading.Lock() for _ in range(len(self.cache))]
       self.is_daemon = is_daemon
       if self.is_daemon:
        threading.Thread(target=self._daemon, args=(True,), daemon=True).start()
    def get(self, key):
        index= hash(key) % 4
        ## this will give us the lock number 
        with self.locks[index]:
            #now we have the particular lock for the key we will have to jump to that bucket 
            curr_cache = self.cache[index]
            # curr_cache is the current cache we have 
            ## now just our simple cache look up
            if key in curr_cache:
                val, ttl = curr_cache[key]
                return val
            return -1
    
    def put(self, key, value, ttl):
        ## puts might be a bit complicated but scaffolding is the same 
        index = hash(key) % 4 #gives either 0,1,2,3 according to the lock number
        with self.locks[index]:
            curr_cache = self.cache[index]
            curr_cache[key] = (value, ttl)
    
    def _daemon(self, is_run: bool):
        while True:
            time.sleep(1)
            ## we iterate over the entire cache
            for index in range(len(self.cache)):
                with self.locks[index]:
                    curr_cache = self.cache[index]
                    key_list = list(curr_cache.keys())
                    for key in key_list:
                        value, ttl = curr_cache[key]
                        if ttl < time.time():
                            curr_cache.pop(key)

                

            
    def return_cache(self):
        print(self.cache)
        return self.cache

def main():
    cache = LRUCache(size=3, is_daemon=True)
    
    print("=== TEST 1: Basic TTL Expiration ===")
    now = time.time()
    cache.put(1, 10, ttl=now + 1.0)
    cache.put(2, 20, ttl=now + 3.0)
    cache.return_cache()
    
    print("\nWait 1.5 seconds...")
    time.sleep(1.5)
    print("Getting key 1 (expected -1):", cache.get(1))
    print("Getting key 2 (expected 20):", cache.get(2))
    cache.return_cache()
    
    print("\n=== TEST 2: Active Daemon Eviction (No 'get' needed) ===")
    print("Inserting key 3 with 1.0s TTL...")
    cache.put(3, 30, ttl=time.time() + 1.0)
    cache.return_cache()
    
    print("\nWait 1.5 seconds (checking if background daemon cleans it up)...")
    time.sleep(1.5)
    cache.return_cache() # Expected: Key 3 is gone, only active keys remain

main()

            
    


