import asyncio

class Task:
    def __init__(self, name, func, dependencies=None):
        self.name = name
        self.func = func
        self.dependencies = dependencies or []

class DAGScheduler:
    def __init__(self):
        self.tasks = {}
        self.in_degree = {}
        self.adj = {}

    def add_task(self, name, func, dependencies=None):
        """Register a task with its prerequisites."""
        task = Task(name, func, dependencies)
        self.tasks[name] = task
        self.in_degree[name] = len(task.dependencies)
        
        # Build adjacency list
        for dep in task.dependencies:
            if dep not in self.adj:
                self.adj[dep] = []
            self.adj[dep].append(name)
            
        # Ensure all tasks are in adj map
        if name not in self.adj:
            self.adj[name] = []

    async def run(self):
        """
        Executes tasks concurrently as their dependencies finish.
        Uses Kahn's Algorithm to track in-degree (dependency counts) dynamically.
        """
        # Find all tasks with 0 in-degree (ready to run immediately)
        queue = asyncio.Queue()
        for name in self.tasks:
            if self.in_degree[name] == 0:
                await queue.put(name)

        completed_count = 0
        total_tasks = len(self.tasks)
        active_futures = set()

        # Execute as long as we have tasks in the queue or tasks running
        while not queue.empty() or active_futures:
            # 1. Pull all currently ready tasks and start them in parallel
            while not queue.empty():
                name = await queue.get()
                task = self.tasks[name]
                
                # Start task execution in the background
                future = asyncio.create_task(self._run_task(task))
                active_futures.add(future)
                queue.task_done()

            # 2. Wait for at least one active task to complete
            if active_futures:
                done, active_futures = await asyncio.wait(
                    active_futures, 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for future in done:
                    completed_name = future.result()
                    completed_count += 1
                    
                    # Decrement in-degree of all child tasks (dependents)
                    for child in self.adj[completed_name]:
                        self.in_degree[child] -= 1
                        if self.in_degree[child] == 0:
                            await queue.put(child)

        # Cycle detection
        if completed_count < total_tasks:
            raise ValueError("Cycle detected in dependency graph! Some tasks could not be executed.")

    async def _run_task(self, task):
        print(f"[Starting] {task.name}")
        await task.func()
        print(f"[Completed] {task.name}")
        return task.name

async def main():
    scheduler = DAGScheduler()
    
    # Helper to create simulated async work
    async def make_task(name, duration):
        await asyncio.sleep(duration)
        
    # Task dependencies:
    # A has no dependencies
    # B depends on A (takes 1.0s)
    # C depends on A (takes 0.5s)  --> B and C should execute in parallel
    # D depends on B and C (takes 0.2s)
    scheduler.add_task("A", lambda: make_task("A", 0.5))
    scheduler.add_task("B", lambda: make_task("B", 1.0), ["A"])
    scheduler.add_task("C", lambda: make_task("C", 0.5), ["A"])
    scheduler.add_task("D", lambda: make_task("D", 0.2), ["B", "C"])
    
    print("Starting DAG execution...")
    start_time = asyncio.get_event_loop().time()
    await scheduler.run()
    end_time = asyncio.get_event_loop().time()
    
    # Total time should be 0.5s (A) + max(1.0s (B), 0.5s (C)) + 0.2s (D) = 1.7 seconds
    print(f"DAG execution completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())
