"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 09: APPLIED CONCURRENT PIPELINES
================================================================================
Difficulty: Applied / integration (combines everything from Workbooks 01-08)
Model:      Python `threading` + `concurrent.futures` (preemptive, multi-threaded)
Primitives: Lock, Semaphore (dict-of), Condition, queue.Queue, ThreadPoolExecutor

Maps to INTERVIEW.MD:
    Multithreaded Web Crawler (LC 1242), parallel-download-with-retry style
    system-design questions, per-host rate limiting, and DAG task schedulers
    (build systems / CI pipelines / Airflow-style DAG runners).

HOW TO USE THIS FILE
--------------------
1. Read each PRIMER block, run its `_demo_*` to SEE the pattern work.
       python3 "CodeSignal-Concurrency/workbook_09_pipelines.py"
2. Each EXERCISE below is a SKELETON — the docstring tells you the contract,
   `# YOUR CODE HERE` is where your solution goes. Nothing is solved for you.
3. Run the file. Demos print; `_check_*` prints [FAIL] until you implement the
   matching function — that's expected. Implement one, re-run, watch it flip
   to [PASS]. Every check has a watchdog: a broken/deadlocked attempt reports
   TIMEOUT instead of hanging the process forever.

THE ONE IDEA
------------
Almost every "real" concurrency interview problem is the SAME four pieces,
recombined:
    1. a WORK QUEUE (or a pool of tasks submitted up front)
    2. a BOUNDED POOL of workers pulling from it (don't spawn unbounded threads)
    3. thread-safe SHARED STATE (a "seen"/visited set, a results dict, counters)
    4. a way to know you're DONE (queue drained, all futures resolved, or a
       Condition whose predicate finally holds)
Everything in this workbook is those four pieces wearing different costumes:
a crawler, a retrying downloader, a rate-limited scatter-gather, a DAG runner.
================================================================================
"""

import collections
import concurrent.futures
import queue
import random
import threading
import time


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 0: THE SHAPE OF A CONCURRENT PIPELINE                                ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Work queue + bounded pool + shared state + a done condition. The simplest
version: put N items in a `queue.Queue`, start K worker threads that loop
`get -> process -> task_done`, then `queue.join()` blocks the main thread until
every item has been `task_done()`-ed. No manual counting required — the Queue
tracks "unfinished tasks" for you.
"""

def _demo_worker_pool_queue():
    """>>> _demo_worker_pool_queue()"""
    print("\n--- DEMO: bounded worker pool draining a work queue ---")
    work = queue.Queue()
    for i in range(12):
        work.put(i)
    n_workers = 3
    results = []
    results_lock = threading.Lock()

    def worker():
        while True:
            try:
                item = work.get_nowait()
            except queue.Empty:
                return
            time.sleep(0.005)               # pretend to do work
            with results_lock:
                results.append(item * item)
            work.task_done()

    threads = [threading.Thread(target=worker) for _ in range(n_workers)]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"  {n_workers} workers drained {len(results)} items "
          f"-> sum of squares = {sum(results)} (want {sum(i*i for i in range(12))})")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 1: VISITED-SET DEDUP — check-then-act MUST be one atomic step        ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
"if item not in seen: seen.add(item); go_process(item)" is a check-then-act
race exactly like `counter += 1`. Two threads can both see `item not in seen`
before EITHER inserts it, and both process it — a crawler that "visits" the
same URL twice, or a scheduler that runs the same task twice.

FIX: hold ONE lock across the check AND the insert:
    with lock:
        if item in seen:
            return                  # someone else already claimed it
        seen.add(item)              # WE claimed it — safe to release now
    process(item)                   # do the (possibly slow) work OUTSIDE the lock
Do the slow work outside the lock — the lock only needs to protect the set.
"""

def _demo_visited_dedup():
    """>>> _demo_visited_dedup()"""
    print("\n--- DEMO: thread-safe visited-set dedup (atomic check-then-insert) ---")
    n_threads = 20
    item = "same-url-for-everyone"
    seen = set()
    seen_lock = threading.Lock()
    claimed_by = []
    claimed_lock = threading.Lock()

    def try_claim(i):
        with seen_lock:                      # check + insert: ONE atomic step
            if item in seen:
                return
            seen.add(item)
        with claimed_lock:
            claimed_by.append(i)

    threads = [threading.Thread(target=try_claim, args=(i,)) for i in range(n_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"  {n_threads} threads raced for 1 item -> {len(claimed_by)} claimed it "
          f"(want exactly 1); winner(s)={claimed_by}")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 2: PER-HOST CONCURRENCY CAPS — a dict of Semaphores                  ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
"Fetch anything, anywhere, but never more than K requests in flight to the SAME
host at once" needs one Semaphore(K) PER HOST, not one global semaphore (that
would cap TOTAL concurrency, not per-host). Since hosts are discovered at
runtime, the dict of semaphores is itself built lazily — and "look up or
create" is ANOTHER check-then-act race, guarded by its own lock:
    with sems_lock:
        if host not in sems:
            sems[host] = threading.Semaphore(k)
        sem = sems[host]
    with sem:
        do_the_request(...)
"""

def _demo_per_host_semaphores():
    """>>> _demo_per_host_semaphores()"""
    print("\n--- DEMO: per-host Semaphore caps concurrency independently ---")
    per_host_limit = 2
    sems = {}
    sems_lock = threading.Lock()
    current = collections.defaultdict(int)
    peak = collections.defaultdict(int)
    state_lock = threading.Lock()

    def sem_for(host):
        with sems_lock:                       # lazy-create is check-then-act too
            if host not in sems:
                sems[host] = threading.Semaphore(per_host_limit)
            return sems[host]

    def hit(host):
        sem = sem_for(host)
        with sem:
            with state_lock:
                current[host] += 1
                peak[host] = max(peak[host], current[host])
            time.sleep(0.03)
            with state_lock:
                current[host] -= 1

    hosts = (["a.com"] * 6) + (["b.com"] * 4)
    threads = [threading.Thread(target=hit, args=(h,)) for h in hosts]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"  peak concurrent per host: {dict(peak)} (cap {per_host_limit} each)")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 3: BOUNDED RETRIES + PER-ATTEMPT TIMEOUT                             ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A flaky call gets retried, but never forever, and never allowed to hang forever
either — two DIFFERENT bounds:
  - RETRY bound: give up after `retries` extra attempts (so `1 + retries` total).
  - TIMEOUT bound: each individual attempt must finish within `timeout` seconds
    — `future.result(timeout=...)` raises `concurrent.futures.TimeoutError` if
    the worker thread hasn't finished yet (the thread itself keeps running —
    you can't force-kill it — but your CALLER stops waiting on it).
Submit a NEW future for each retry; a Future represents one already-started
call and cannot be "rerun".
"""

def _demo_retry_with_timeout():
    """>>> _demo_retry_with_timeout()"""
    print("\n--- DEMO: bounded retries + per-attempt timeout ---")
    attempts = {"n": 0}
    attempts_lock = threading.Lock()

    def flaky():
        with attempts_lock:
            attempts["n"] += 1
            n = attempts["n"]
        if n < 3:
            raise ConnectionError(f"simulated failure #{n}")
        return "ok"

    result = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        for attempt in range(1, 5):                 # bounded: at most 4 tries
            future = pool.submit(flaky)
            try:
                result = future.result(timeout=1)    # bounded: 1s per attempt
                break
            except Exception as e:
                print(f"  attempt {attempt} failed: {e}")
    print(f"  final result after retries: {result!r}")


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED FIXTURE — a fake "web" so nothing here touches the network.
# ══════════════════════════════════════════════════════════════════════════════
"""
FAKE_WEB models a tiny link graph across two hosts, a.com and b.com, including
a cycle (a.com/1 <-> a.com/2) and a cross-host link (a.com -> b.com/x -> back to
a.com) — both classic crawler edge cases (must not infinite-loop, must not
wander off-host).
"""

FAKE_WEB = {
    "http://a.com":   {"host": "a.com", "links": ["http://a.com/1", "http://a.com/2", "http://b.com/x"]},
    "http://a.com/1": {"host": "a.com", "links": ["http://a.com/2", "http://a.com/3"]},
    "http://a.com/2": {"host": "a.com", "links": ["http://a.com/1"]},
    "http://a.com/3": {"host": "a.com", "links": []},
    "http://b.com/x": {"host": "b.com", "links": ["http://b.com/y"]},
    "http://b.com/y": {"host": "b.com", "links": ["http://a.com"]},
}


def hostname(url):
    """'http://a.com/1' -> 'a.com'  (tiny manual parse, no urllib needed)."""
    return url.split("//", 1)[-1].split("/", 1)[0]


def fetch(url):
    """IMPLEMENTED. Simulates a network fetch of `url` against FAKE_WEB: sleeps
    briefly (so concurrency is actually necessary/visible), then returns the
    page dict {"host": ..., "links": [...]}. Raises KeyError for unknown urls.
    """
    time.sleep(0.01 + random.random() * 0.01)
    if url not in FAKE_WEB:
        raise KeyError(f"404 not found: {url}")
    return FAKE_WEB[url]


def _make_flaky_fetch(fail_counts):
    """Test fixture (NOT an exercise). Wraps `fetch` so each url fails a
    deterministic number of times before succeeding.
    fail_counts: {url: N}  -> fails the first N attempts, succeeds after.
    fail_counts: {url: None} -> fails EVERY attempt, forever.
    Thread-safe: many workers may hit the same url concurrently.
    """
    lock = threading.Lock()
    attempts = collections.defaultdict(int)

    def flaky_fetch(url):
        with lock:
            attempts[url] += 1
            n = attempts[url]
        limit = fail_counts.get(url, 0)
        if limit is None or n <= limit:
            raise ConnectionError(f"simulated failure #{n} for {url}")
        return fetch(url)

    return flaky_fetch


def _make_instrumented_fetch(delay=0.03):
    """Test fixture (NOT an exercise). Wraps `fetch` to record, per host, how
    many calls were concurrently IN FLIGHT — used to verify per-host caps.
    Returns (instrumented_fetch, peak_dict).
    """
    lock = threading.Lock()
    current = collections.defaultdict(int)
    peak = collections.defaultdict(int)

    def instrumented_fetch(url):
        host = hostname(url)
        with lock:
            current[host] += 1
            peak[host] = max(peak[host], current[host])
        try:
            time.sleep(delay)
            return fetch(url)
        finally:
            with lock:
                current[host] -= 1

    return instrumented_fetch, peak


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  DEMO — ThreadPoolExecutor scatter-gather over the fake fetch (FULLY WORKED) ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
The idiom you'll reuse in every exercise below: submit one task per unit of
work, then gather results as they complete with `as_completed`, keeping a
{future: key} map so you know WHICH result you're looking at.
"""

def _demo_scatter_gather_fetch():
    """>>> _demo_scatter_gather_fetch()"""
    print("\n--- DEMO: ThreadPoolExecutor scatter-gather over a fake fetch ---")
    urls = ["http://a.com", "http://a.com/1", "http://a.com/2", "http://b.com/x", "http://nope.com"]
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        future_to_url = {pool.submit(fetch, u): u for u in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                results[url] = future.result()
            except Exception as e:
                results[url] = f"ERROR: {e}"
    for u in urls:
        print(f"  {u:20s} -> {results[u]}")


# ══════════════════════════════════════════════════════════════════════════════
#  CHEAT SHEET — which pipeline shape do I reach for?
# ══════════════════════════════════════════════════════════════════════════════
"""
┌───────────────────────────┬────────────────────────────────────────────────┐
│ Need                      │ Shape                                          │
├───────────────────────────┼────────────────────────────────────────────────┤
│ Explore a graph/site       │ frontier queue + visited set (Lock) + pool     │
│ Tolerate flaky calls       │ per-task retry loop + future.result(timeout=)  │
│ Cap load per resource      │ dict of Semaphore(N), keyed & lazy-created     │
│ Respect a dependency order │ remaining-deps counter per node + ready queue  │
│ "Are we done yet?"         │ queue.join() / all futures resolved / Condition│
└───────────────────────────┴────────────────────────────────────────────────┘
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — Multithreaded Web Crawler (LeetCode 1242)                     ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Crawl starting at `start_url`, following `fetch(url)["links"]`, but ONLY follow
links whose host matches `start_url`'s host (use `hostname()`). Return the set
of every same-host URL reached (including start_url itself), each fetched
EXACTLY ONCE, using a pool of worker threads running concurrently (not one URL
at a time).

    >>> crawl("http://a.com", fetch)
    {"http://a.com", "http://a.com/1", "http://a.com/2", "http://a.com/3"}

Hint: a thread-safe `visited` set guarded by a Lock (PRIMER 1 — check-then-add
must be atomic, or two workers both fetch the same fresh link). A `queue.Queue`
as the frontier works well: workers loop get -> fetch -> filter+enqueue new
same-host links -> task_done; the main thread calls `frontier.join()` to know
when the whole graph has been drained, then signals workers to stop (e.g. a
sentinel or a stop Event) before joining the threads.
"""

def crawl(start_url, fetch):
    """
    Concurrently crawl all URLs reachable from `start_url` that share its host.

    Args:
        start_url: the URL to start from.
        fetch: callable url -> {"host": str, "links": [url, ...]}.
    Returns:
        set of every same-host url visited (including start_url).
    """
    # YOUR CODE HERE
    raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — Parallel Downloader with Retries + Timeout                    ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Fetch every url in `urls` concurrently using a bounded thread pool
(`max_workers`). `fetch` may randomly raise. Retry a failing url up to
`retries` additional times (so up to `1 + retries` total attempts), each single
attempt bounded by `timeout` seconds. If a url still hasn't succeeded after
all attempts, its result is `None` — never raise out of `download_all`, and
never let one bad url block the others.

    >>> download_all(urls, fetch, max_workers=4, retries=2, timeout=2)
    {url1: <page>, url2: None, ...}

Hint: ThreadPoolExecutor + a per-url retry loop that submits a NEW future for
each attempt (a Future can't be rerun) and calls
`future.result(timeout=timeout)`, catching BOTH the fetch's own exception and
`concurrent.futures.TimeoutError`.
"""

def download_all(urls, fetch, max_workers, retries, timeout):
    """
    Args:
        urls: iterable of urls to fetch.
        fetch: callable url -> page (may raise).
        max_workers: size of the thread pool.
        retries: extra attempts allowed per url after the first failure.
        timeout: seconds allowed per individual attempt.
    Returns:
        {url: result_or_None}
    """
    # YOUR CODE HERE
    raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 3 — Scatter-Gather with Per-Host Concurrency Limits                ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Fetch every url in `urls` concurrently, but never let more than
`per_host_limit` fetches to the SAME host run at once (other hosts are
unaffected — this is NOT a global cap).

    >>> scatter_gather(urls, fetch, per_host_limit=2)
    {url: result, ...}

Hint: PRIMER 2 — a dict of `threading.Semaphore(per_host_limit)` keyed by
`hostname(url)`, created lazily under a Lock. Submit all urls to a pool (it can
be as large as `len(urls)`; the per-host semaphores do the real throttling),
acquire that host's semaphore around the call to `fetch`, release after.
Assume `fetch` does not raise here.
"""

def scatter_gather(urls, fetch, per_host_limit):
    """
    Args:
        urls: iterable of urls to fetch.
        fetch: callable url -> page.
        per_host_limit: max concurrent in-flight fetches per host.
    Returns:
        {url: result}
    """
    # YOUR CODE HERE
    raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 4 — Dependency-DAG Task Scheduler                                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Given `tasks` (iterable of task ids) and `deps` (dict: task_id -> list of
task_ids that must finish first), run every task exactly once via
`run_task(task_id)`, respecting dependency order, while running independent
tasks IN PARALLEL up to `max_workers` at a time (a serial topological sort is
correct but wrong for this exercise — maximize parallelism).

    >>> run_dag(["A","B","C"], {"A": [], "B": [], "C": ["A","B"]}, run_task, 3)
    # A and B may run concurrently; C only starts after BOTH finish.

Hint: track each task's remaining-dependency COUNT under a Lock. Tasks with
count 0 are "ready" — submit them to a pool immediately. When a task finishes,
decrement the count of each of its dependents; any that hit 0 become newly
ready and get submitted too. Keep submitting/collecting (e.g. via
`concurrent.futures.wait(..., return_when=FIRST_COMPLETED)` in a loop, or a
`queue.Queue` of ready task ids) until every task has run.
"""

def run_dag(tasks, deps, run_task, max_workers):
    """
    Args:
        tasks: iterable of task ids.
        deps: dict task_id -> list of prerequisite task_ids.
        run_task: callable task_id -> None, does the work for one task.
        max_workers: max tasks running concurrently.
    Returns:
        None (side effect: run_task called exactly once per task, in a valid
        dependency order, maximizing parallelism).
    """
    # YOUR CODE HERE
    raise NotImplementedError


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade your solutions. FAIL is expected until
#  you implement the exercise above; each check is watchdog-guarded so a
#  deadlocked attempt reports TIMEOUT instead of hanging forever.
# ══════════════════════════════════════════════════════════════════════════════

def _watchdog(fn, timeout=5):
    """Run fn() in a daemon thread; return (ok, value_or_error_message).
    Guards the checks against a hung/deadlocked exercise solution — the
    process can still exit even if `fn` never returns (daemon thread)."""
    box = {}

    def target():
        try:
            box["value"] = fn()
        except Exception as e:
            box["error"] = f"{type(e).__name__}: {e}"

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return False, f"TIMEOUT after {timeout}s (possible deadlock/hang)"
    if "error" in box:
        return False, box["error"]
    return True, box.get("value")


def _check_crawl():
    counts = collections.defaultdict(int)
    counts_lock = threading.Lock()

    def counting_fetch(url):
        with counts_lock:
            counts[url] += 1
        return fetch(url)

    ok, result = _watchdog(lambda: crawl("http://a.com", counting_fetch), timeout=5)
    if not ok:
        print(f"  [FAIL] crawl -> {result}")
        return False

    expected = {"http://a.com", "http://a.com/1", "http://a.com/2", "http://a.com/3"}
    off_host = ("http://b.com/x", "http://b.com/y")
    once_each = all(counts[u] == 1 for u in expected) and all(counts.get(u, 0) == 0 for u in off_host)
    passed = isinstance(result, set) and result == expected and once_each
    shown = sorted(result) if isinstance(result, set) else result
    print(f"  [{'PASS' if passed else 'FAIL'}] crawl -> {shown} "
          f"(want {sorted(expected)}, each same-host url fetched once: {once_each})")
    return passed


def _check_download_all():
    fail_counts = {
        "http://a.com":   1,      # fails once, then succeeds
        "http://a.com/1": 2,      # fails twice, succeeds on 3rd try
        "http://a.com/2": None,   # always fails
        "http://a.com/3": 0,      # succeeds immediately
    }
    urls = list(fail_counts.keys())
    flaky = _make_flaky_fetch(fail_counts)

    ok, result = _watchdog(
        lambda: download_all(urls, flaky, max_workers=4, retries=2, timeout=2), timeout=8
    )
    if not ok:
        print(f"  [FAIL] download_all -> {result}")
        return False

    passed = (
        isinstance(result, dict)
        and result.get("http://a.com") == FAKE_WEB["http://a.com"]
        and result.get("http://a.com/1") == FAKE_WEB["http://a.com/1"]
        and result.get("http://a.com/2") is None
        and result.get("http://a.com/3") == FAKE_WEB["http://a.com/3"]
    )
    summary = {u: ("OK" if v else None) for u, v in result.items()} if isinstance(result, dict) else result
    print(f"  [{'PASS' if passed else 'FAIL'}] download_all -> {summary} "
          f"(want a.com/2 -> None, all others OK)")
    return passed


def _check_scatter_gather():
    instrumented, peak = _make_instrumented_fetch(delay=0.03)
    per_host_limit = 2
    urls = (
        ["http://a.com", "http://a.com/1", "http://a.com/2", "http://a.com/3"] * 3
        + ["http://b.com/x", "http://b.com/y"] * 3
    )

    ok, result = _watchdog(lambda: scatter_gather(urls, instrumented, per_host_limit), timeout=8)
    if not ok:
        print(f"  [FAIL] scatter_gather -> {result}")
        return False

    within_cap = bool(peak) and all(p <= per_host_limit for p in peak.values())
    complete = isinstance(result, dict) and set(result.keys()) == set(urls)
    passed = within_cap and complete
    print(f"  [{'PASS' if passed else 'FAIL'}] scatter_gather -> peak per host {dict(peak)} "
          f"(cap {per_host_limit}), {len(result) if isinstance(result, dict) else '?'} urls returned")
    return passed


def _check_run_dag():
    tasks = ["A", "B", "C", "D", "E"]
    deps = {"A": [], "B": [], "C": ["A", "B"], "D": ["B"], "E": ["C", "D"]}
    timeline = []
    timeline_lock = threading.Lock()

    def run_task(task_id):
        start = time.monotonic()
        time.sleep(0.02)
        finish = time.monotonic()
        with timeline_lock:
            timeline.append((task_id, start, finish))

    def run():
        run_dag(tasks, deps, run_task, max_workers=3)
        return list(timeline)

    ok, result = _watchdog(run, timeout=8)
    if not ok:
        print(f"  [FAIL] run_dag -> {result}")
        return False

    ran_once = sorted(t for t, _, _ in result) == sorted(tasks) and len(result) == len(tasks)
    by_task = {t: (s, f) for t, s, f in result}
    order_ok = ran_once and all(
        by_task[t][0] >= max((by_task[d][1] for d in deps[t]), default=0.0)
        for t in tasks
    )
    passed = ran_once and order_ok
    print(f"  [{'PASS' if passed else 'FAIL'}] run_dag -> ran exactly once each: {ran_once}, "
          f"respected dependency order: {order_ok}")
    return passed


def _run_demos():
    _demo_worker_pool_queue()
    _demo_visited_dedup()
    _demo_per_host_semaphores()
    _demo_retry_with_timeout()
    _demo_scatter_gather_fetch()


def _run_checks():
    print("\n=== SELF-TESTS (FAIL is expected until you implement each exercise) ===")
    results = [
        _check_crawl(),
        _check_download_all(),
        _check_scatter_gather(),
        _check_run_dag(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: keep iterating on the 4 exercises above until every check PASSes —"
          " that's the whole applied-concurrency toolkit in one file.")
