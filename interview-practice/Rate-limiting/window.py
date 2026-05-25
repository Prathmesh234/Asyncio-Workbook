##This is interesting 
## our map will maintain user_id :  [....] the list will be the timestamp of the request 
## Our algorithm which is sliding window will basically be run on each of the user rather than on the entire hashmap 
##to start we will have the following functions 
## request  - will take user_id into account (will not care about the exact request)
# map[user_id].append(time.time())
## the other function will be check(window_size) and will keep checking it per user 
##it will also be checked lazily, when the user requests it rather than checking in the background for simplicity
import time 

class RateLimiter:
    def __init__(self, window_size, max_requests):
        self.map = {}
        self.window_size = window_size
        self.max_requests = max_requests

    def request(self, user_id):
        ##sliding window algorithm 
        ##we will not have to do anything complicated like the actual sliding window 
        # but we will have to check the requests 
        current_time = time.time()
        ##first we will check if the user id is not in map
        if user_id not in self.map:
            #then first request fine 
            self.map[user_id] = []
        
        requests = self.map[user_id]
        #now we have to do two checks first if the capacity is already there or not 
        if self.max_requests > len(requests):
            self.map[user_id].append(current_time)
            return True
        
        #outside this means our window size has exceeded so we will do the following 
        ## first check the limit size 
        # the time has PASSED means we can add more, kind of confusing but if our window is T=5 seconds 
        ## considring request 1 arrives at T=0 and current time is T = 6
        ## that means 6-0 = 6 > 5, means our request is valid and not within the time window
        if current_time - requests[0] >= self.window_size:
            requests.pop(0)  # Evict the oldest expired request
            self.map[user_id].append(current_time)
            return True
        return False

def main():
    T_size = 5.00000
    max_requests = 3
    rate_limiter = RateLimiter(T_size, max_requests)
    
    user = "user123"
    
    print("--- Sending 4 immediate requests (Limit: 3 per 5s) ---")
    print(f"Request 1: {rate_limiter.request(user)} (Expected: True)")
    print(f"Request 2: {rate_limiter.request(user)} (Expected: True)")
    print(f"Request 3: {rate_limiter.request(user)} (Expected: True)")
    print(f"Request 4: {rate_limiter.request(user)} (Expected: False)")
    
    print("\nWaiting 5.1 seconds for the window to slide...")
    time.sleep(5.1)
    
    print("\n--- Sending request after waiting ---")
    print(f"Request 5: {rate_limiter.request(user)} (Expected: True)")

if __name__ == "__main__":
    main()







