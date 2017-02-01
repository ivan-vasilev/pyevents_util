from pyevents.events import *
from pyeventsml.singleton import *


class GlobalListeners(ChainedLists, metaclass=SingletonType):
    """Singleton ilst for the purpose of holding global listeners"""
