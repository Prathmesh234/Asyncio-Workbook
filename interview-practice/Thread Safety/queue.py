##we will build a producer consumer pipeline with asyncio 
import asyncio

async def producer(queue, items):
    while True:
        await asyncio.sleep(1)
        for i in items:
            await queue.put(i)
    await queue.put(None)
    return 
async def consumer(queue, processed):
    while True:
        await asyncio.sleep(1)
        val= await queue.get()
        processed.append(val)
        if val is None:
            break
    return 

async def main():
    queue = asyncio.Queue()
    items = [1,2,3,4,5]
    processed_list = []
    await asyncio.gather(
        consumer(queue, processed_list, 0.15), 
        producer(queue,items, 0.15)
    )
    return processed_list


        

    

