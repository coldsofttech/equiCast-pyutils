import random
import time
from functools import wraps


def retry(max_retries=5, delay=0.0, backoff=2.0, jitter=True, max_delay=60.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            e_attempt = kwargs.get("attempt", 0)
            for i_retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    c_delay = delay + e_attempt * (e_attempt + 1) // 2
                    c_delay = min(c_delay, max_delay)
                    if jitter:
                        j_factor = random.uniform(0.1, 0.5)
                        c_delay += j_factor

                    print(f"⏳ Retrying in {c_delay:.2f} seconds...")
                    time.sleep(c_delay)
            raise RuntimeError(f"❌ {func.__name__} failed after {max_retries} retries.")

        return wrapper

    return decorator
