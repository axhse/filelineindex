from functools import wraps
from time import time
from typing import Callable, List

from filelineindex.abstract import LineIndex


def evaluate_milliseconds(function: Callable) -> Callable[..., float]:
    def __return_milliseconds(*args, **kwargs):
        start_time = time()
        function(*args, **kwargs)
        return 1000 * (time() - start_time)

    return __return_milliseconds


def repeat(number: int) -> Callable[[Callable], Callable[..., float]]:
    def __repeat_evaluator(evaluator: Callable[..., float]):
        def __evaluate(*args, **kwargs):
            return sum(evaluator(*args, **kwargs) for _ in range(number))

        return __evaluate

    return __repeat_evaluator


@evaluate_milliseconds
def check_lines(index: LineIndex, lines: List[str]) -> None:
    for line in lines:
        _ = line in index


def benchmark(function: Callable, iterations: int, *args, **kwargs) -> None:
    elapsed_milliseconds = repeat(iterations)(function)(*args, **kwargs)
    print(
        f"\x1B[34mElapsed milliseconds:    \x1B[1m\x1B[94m{round(elapsed_milliseconds)}\x1B[0m"
    )


def benchmark_line_check(iterations: int, index: LineIndex, lines: List[str]) -> None:
    benchmark(check_lines, iterations, index, lines)
