from enum import Enum


class MLPhase(Enum):
    TRAINING = 1
    EVALUATION = 2
    TESTING = 3

    def __json__(self):
        return str(self)
