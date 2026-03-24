# Asyncio Practice Workbooks

10 progressive workbooks for mastering Python asyncio and async programming.

## Setup with uv

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the project
uv pip install -e ".[dev]"
```

## Workbook Progression

| Workbook | Difficulty | Topics |
|----------|------------|--------|
| 01 | Beginner | Basic async/await, coroutines |
| 02 | Beginner | asyncio.gather, running multiple coroutines |
| 03 | Beginner-Intermediate | create_task, task lifecycle, cancellation |
| 04 | Intermediate | Timeouts, wait, exception handling |
| 05 | Intermediate | Async iterators, generators, context managers |
| 06 | Intermediate | asyncio.Queue, producer-consumer basics |
| 07 | Intermediate-Advanced | ThreadPoolExecutor, blocking I/O |
| 08 | Advanced | ProcessPoolExecutor, CPU-bound tasks |
| 09 | Advanced | Synchronization primitives, async DS & Algo |
| 10 | Expert | High-availability producer-consumer queue |

## Running a Workbook

```bash
uv run python workbook_01_basics.py
```

## Testing Your Solutions

Each workbook contains `# YOUR CODE HERE` markers. Implement the solutions and run:

```bash
uv run pytest -v
```
# Asyncio-Workbook
