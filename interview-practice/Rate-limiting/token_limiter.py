##here we will implement token based rate limiter 
##now we will have to make this thread safev (called by many programs)
##any changes made will have to be using a lock 

##Final - let us implement a tiered limiter
# we will have 3 user maps and based on their tiered levels we will increase the rate limit 

import time
import threading
import asyncio
class RateLimiter:
    def __init__(self, budget, refill_time, id):
        self.user_map = {}
        self.budget = budget
        self.refill_time = refill_time
        self.id = id
        self.lock = threading.Lock()

    
    def request(self, user_id):
        ##this is very important always get the current time in time.time() (because as our code will run it will progresss)
        with self.lock:
            current_time = time.time()
            if user_id not in self.user_map:
                self.user_map[user_id] = {
                    "budget": 0,
                    "last_refill": current_time
                }
            request = self.user_map[user_id]
            if current_time - request["last_refill"] >= self.refill_time:
                request["last_refill"] = current_time
                request["budget"] = 0
 
            if request["budget"] >= self.budget:
            #this means it is invalid 
                return False
            request["budget"] += 1
        
            return True

async def worker(task_name, rate_limiter, user_id):
    # Make multiple requests in a loop to test the rate limit
    for i in range(20):
        allowed = rate_limiter.request(user_id)
        print(f"[{task_name}] Request {i+1} for {user_id}: {'Allowed' if allowed else 'Blocked'}")
        await asyncio.sleep(1)  # Simulate delay/work between requests

async def main():
    user1 = "user_bronze"
    user2 = "user_silver"
    user3 = "user_gold"
    user4 = "user_platinum"
    membership = [user1, user2, user3, user4]
    
    tasks = []
    
    for member in membership:
        if member == "user_bronze":
            rate_limiter = RateLimiter(budget=3, refill_time=10.000, id=member)
        elif member == "user_silver":
            rate_limiter = RateLimiter(budget=5, refill_time=8.000, id=member)
        elif member == "user_gold":
            rate_limiter = RateLimiter(budget=8, refill_time=6.000, id=member)
        else:
            rate_limiter = RateLimiter(budget=15, refill_time=2.000, id=member)
            
        # Fix: pass the called coroutine to create_task
        task = asyncio.create_task(worker(f"Task-{member}", rate_limiter, member))
        tasks.append(task)
        
    # Fix: await tasks so main doesn't exit immediately
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())


