from dataclasses import dataclass, asdict
from typing import List, Any


@dataclass
class Query:
    query_name: str

    def to_dict(self):
        return asdict(self)


@dataclass
class QueryOutput:
    output: Any

    def to_dict(self):
        return asdict(self)


@dataclass
class QueryRun:
    run: int
    execution_time: float
    query_output: QueryOutput

    def to_dict(self):
        return asdict(self)


@dataclass
class Timing:
    execution_times: List[float]
    avg_time: float
    min_time: float
    max_time: float

    def to_dict(self):
        return asdict(self)


@dataclass
class BenchmarkQueryResult:
    query: Query
    timing: Timing
    query_output: QueryOutput

    def to_dict(self):
        return asdict(self)


@dataclass
class BenchmarkResult:
    config: dict
    timestamp: str
    query_results: List[BenchmarkQueryResult]

    def to_dict(self):
        return asdict(self)
