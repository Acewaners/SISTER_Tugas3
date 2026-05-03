import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List
from collections import defaultdict
import threading


@dataclass
class MetricPoint:
    value: float
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    def __init__(self):
        self._metrics: Dict[str, List[MetricPoint]] = defaultdict(list)
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()

    def increment(self, name: str, value: float = 1, labels: Dict = None):
        with self._lock:
            self._counters[name] += value
            self._metrics[name].append(MetricPoint(value, time.time(), labels or {}))

    def gauge(self, name: str, value: float, labels: Dict = None):
        with self._lock:
            self._gauges[name] = value
            self._metrics[name].append(MetricPoint(value, time.time(), labels or {}))

    def histogram(self, name: str, value: float, labels: Dict = None):
        with self._lock:
            self._metrics[name].append(MetricPoint(value, time.time(), labels or {}))

    def get_counter(self, name: str) -> float:
        return self._counters.get(name, 0)

    def get_gauge(self, name: str) -> float:
        return self._gauges.get(name, 0)

    def get_histogram(self, name: str) -> List[MetricPoint]:
        return self._metrics.get(name, [])

    def get_all_metrics(self) -> Dict:
        return {
            'counters': dict(self._counters),
            'gauges': dict(self._gauges),
            'histograms': {k: len(v) for k, v in self._metrics.items()}
        }

    def reset(self):
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._gauges.clear()


metrics_collector = MetricsCollector()
