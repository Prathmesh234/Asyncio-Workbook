## we are going to implement a LRU cache 
## this is a Least recently used cache 
## for simplicity I will use a hashmap for storing the cache values and a deque for storing the least recently used keys


##Part 2  - We will add TTL to our cache or time to live. 
##in the question it is mentioned we do this lazily i.e only when it is accessed 
## We will do it lazily, and then we will do a daemon lru based/also we will have to make this thread safe 
##for using a thread we will need a threading lock too 
##We will have to lock the daemon as well as lock during both the put and the get methods to prevent race conditions
##Also the issue with this is that, during the removal from the cache, the biggest issue is we will not be able to iterate
## thus we will have to do something interesting -> we will maintain copy of the keys as a list and iterate over that in the daemon
## HOWEVER WE WILL CREATE THIS IN THE DAEMON ITSELF
from asyncio import locks
from asyncio import locks
from typing import OrderedDict
from collections import deque
import time 
import asyncio
import threading
class LRUCache:
    def __init__(self, size, is_daemon):
        # initialize the hashmap 
        # cache[key] = value, get O(1) we will get the value given the key 
        ## however every single itme we touch the queue we will have to move it around 
        # the queue will basically have the keys ..
        ## left most is the least recentlt used and right is most recently used 
        self.cache = OrderedDict()
        self.size = size
        self.lock = threading.Lock()
        self.is_daemon = is_daemon
        ## we will have to run the daemon when initialized 
        if self.is_daemon:
            threading.Thread(target=self._daemon, args=(True,), daemon=True).start()
        
    
    def get(self, key):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                value, ttl = self.cache[key]
                return value
        return -1
    
    def put(self, key, value, ttl):
        with self.lock:
            self.cache[key] = (value, ttl)
            self.cache.move_to_end(key)
            if len(self.cache) > self.size:
                self.cache.popitem(last=False)
    
    def _daemon(self, is_run: bool):
        while True:
            time.sleep(1)
            ##we will have to check for all the values 
            with self.lock:
                key_list = list(self.cache.keys())
                for key in key_list:
                    value, ttl = self.cache[key]
                    if ttl < time.time():
                    # it is expired we remove it, but first acquire the lock 
                            self.cache.pop(key)

                

            
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
    
    print("\n=== TEST 2: LRU Capacity Eviction (Capacity = 3) ===")
    # Cache currently has key 2. Let's add keys 3 and 4
    now = time.time()
    cache.put(3, 30, ttl=now + 5.0)
    cache.put(4, 40, ttl=now + 5.0)
    cache.return_cache() # Expected keys: 2, 3, 4
    
    print("\nInserting key 5 (exceeds size 3. Key 2 is the oldest/LRU, so it gets evicted):")
    cache.put(5, 50, ttl=now + 5.0)
    cache.return_cache() # Expected keys: 3, 4, 5
    print("Getting key 2 (expected -1):", cache.get(2))
    
    print("\n=== TEST 3: Access Updates LRU Order ===")
    # Cache has 3 (oldest), 4, 5 (newest).
    print("Accessing key 3 (moves it to newest/MRU):", cache.get(3))
    print("\nInserting key 6 (exceeds size 3. Key 4 is now the oldest, so it gets evicted instead of 3):")
    cache.put(6, 60, ttl=time.time() + 5.0)
    cache.return_cache() # Expected keys: 5, 3, 6
    print("Getting key 4 (expected -1):", cache.get(4))
    print("Getting key 3 (expected 30):", cache.get(3))
    
    print("\n=== TEST 4: Active Daemon Eviction (No 'get' needed) ===")
    # Insert key 7 with a short 1 second TTL
    print("Inserting key 7 with 1.0s TTL...")
    cache.put(7, 70, ttl=time.time() + 1.0)
    cache.return_cache()
    
    print("\nWait 1.5 seconds (we do NOT call get, checking if the background daemon cleans it up)...")
    time.sleep(1.5)
    cache.return_cache() # Expected: Key 7 is gone, only active keys remain

main()

            
    


