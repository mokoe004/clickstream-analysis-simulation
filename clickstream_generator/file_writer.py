import csv
import json
from pathlib import Path
from typing import List, Dict

def save_events_to_csv(events: List[Dict], filepath: Path):
    if not events:
        return
    with filepath.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=events[0].keys())
        writer.writeheader()
        writer.writerows(events)

def save_events_to_json(events: List[Dict], filepath: Path):
    with filepath.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
