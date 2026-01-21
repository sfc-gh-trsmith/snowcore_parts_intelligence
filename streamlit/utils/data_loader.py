from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict


def run_queries_parallel(session, query_map: Dict[str, str]) -> Dict[str, object]:
    results: Dict[str, object] = {}

    def run_one(name: str, sql: str) -> object:
        return session.sql(sql).to_pandas()

    with ThreadPoolExecutor(max_workers=min(8, len(query_map))) as executor:
        futures = {executor.submit(run_one, name, sql): name for name, sql in query_map.items()}
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()

    return results
