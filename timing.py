from time import perf_counter
from functools import wraps


def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        result = func(*args, **kwargs)
        end = perf_counter()
        print(f'{func.__name__} took {end - start:.4f} seconds')
        return result

    return wrapper
