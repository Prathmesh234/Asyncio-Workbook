
from dataclasses import fields
from unittest import result
from dataclasses import field
import random
import heapq
import asyncio
import time

class CronScheduler:
    def __init__(self):
        self.jobs = []
        self.schedule = {}
    def parse_field(self, field: str, min_val: int, max_val: int) -> set[int]:
        result = set()
 
    # "," — list: "1,3,5" → parse each part individually
        if "," in field:
            for part in field.split(","):
                result |= self.parse_field(part, min_val, max_val)
                return result
 
    # "*" — wildcard: every value in range
        if field == "*":
            return set(range(min_val, max_val + 1))
 
    # "*/2" — step on wildcard: every N values
        if field.startswith("*/"):
            step = int(field[2:])
            return set(range(min_val, max_val + 1, step))
 
    # "1-5" — range
        if "-" in field:
            start, end = field.split("-")
            return set(range(int(start), int(end) + 1))
 
    # "1-5/2" — range with step
        if "/" in field:
            range_part, step = field.split("/")
            start, end = range_part.split("-")
            return set(range(int(start), int(end) + 1, int(step)))
 
    # plain number
        return {int(field)}
    def cron_parser(self, id, job_time):
        fields = job_time.split()
        if len(fields) != 5:
            raise ValueError("Cron expression must have exactly 5 fields")
 
        minute, hour, dom, month, dow = fields
        self.schedule[id] = {"minute": minute, "hour":hour, "dom": dom, "month": month, "dow":dow}
    
    def daemon(self, id):
        import datetime
        
        sched = self.schedule[id]
        allowed_minutes = self.parse_field(sched["minute"], 0, 59)
        allowed_hours = self.parse_field(sched["hour"], 0, 23)
        allowed_dom = self.parse_field(sched["dom"], 1, 31)
        allowed_month = self.parse_field(sched["month"], 1, 12)
        allowed_dow = self.parse_field(sched["dow"], 0, 6)
        
        while True:
            dt = datetime.datetime.now()
            if (dt.minute in allowed_minutes and
                dt.hour in allowed_hours and
                dt.day in allowed_dom and
                dt.month in allowed_month and
                dt.weekday() in allowed_dow):
                print(f"[Trigger] Job {id} triggered at {dt}")
            time.sleep(1)

