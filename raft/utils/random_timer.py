from threading import Timer
from random import uniform
from typing import Callable


class RandomTimer:
    _create_timer: Callable[[], Timer]
    _timer: Timer

    def __init__(self, lower: int, upper: int, callback, args=None, kwargs=None):
        self._create_timer = lambda: Timer(
            uniform(lower, upper), callback, args, kwargs
        )
        self._timer = self._create_timer()

    def start(self):
        self._timer.start()

    def reset(self):
        self._timer.cancel()
        self._timer = self._create_timer()
        self._timer.start()

    def stop(self):
        self._timer.cancel()
