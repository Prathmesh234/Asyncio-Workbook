##we will implement a simple priority job queue 
##next part of the job queue we will implement a delayed retry queue. Here a failed job is reen queued 
#3 with exponential backoff capped with a max delay 

import random
import heapq
import asyncio

##we will do this for cron type jobs too 
class PriorityQueue:
    def __init__(self, size, max_retry):
        self.pqueue = []  # Empty list is already a valid heap
        self.max_size = size
        self.max_retry = max_retry
    def create_job(self, key, priority, failed, count):
        ##in heap lowest is at the top, so higher the priority 1-5 we will do inverse of the priority 
        heapq.heappush(self.pqueue, (-priority, key, failed, count))
    async def process_job(self):
        while len(self.pqueue):
            priority, job, failed, count = heapq.heappop(self.pqueue)
            if failed:
                print(f"Job: {job} with priority: {-priority} has failed: {failed}")
                failed= False
                heapq.heappush(self.pqueue, (priority, job, failed, count + 1))
            print(f"Job: {job} with priority: {-priority} has failed: {failed}")
            await asyncio.sleep(0.5)
        return 


async def main():
    pqueue = PriorityQueue(size=15, max_retry=2)
    for i in range(10):
        fail= True if i %3 == 0 else False
        pqueue.create_job(key=i, priority=random.randint(1,5),failed=fail,count=0)
    await pqueue.process_job()

if __name__ == "__main__":
    asyncio.run(main())