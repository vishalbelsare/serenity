import time

from decorator import decorate


class HighPerformanceTimer(object):
    def __init__(self, callback):
        self._callback = callback

    def _new_timer(self):
        return self.__class__(self._callback)

    def __enter__(self):
        self._start = time.perf_counter_ns()

    def __exit__(self, typ, value, traceback):
        # Time can go backwards.
        duration = max(time.perf_counter_ns() - self._start, 0)
        self._callback(duration / 1000)

    def __call__(self, f):
        def wrapped(func, *args, **kwargs):
            # Obtaining new instance of timer every time
            # ensures thread safety and reentrancy.
            with self._new_timer():
                return func(*args, **kwargs)

        return decorate(f, wrapped)
