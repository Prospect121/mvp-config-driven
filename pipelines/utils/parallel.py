from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, List, Any, Dict

def run_in_threads(funcs: Iterable[Callable[[], Any]], max_workers: int = 4) -> List[Any]:
    """Run independent callables concurrently and return results.

    Args:
        funcs: Iterable of zero-arg callables
        max_workers: Max threads
    Returns:
        List of results ordered by completion
    """
    results: List[Any] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(f): f for f in funcs}
        for fut in as_completed(futs):
            results.append(fut.result())
    return results


def run_map_in_threads(func: Callable[[Any], Any], items: Iterable[Any], max_workers: int = 4) -> List[Any]:
    """Apply func(item) concurrently and return list of results in submission order."""
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        return list(ex.map(func, items))