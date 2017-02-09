from pyevents.events import *
from pyeventsml.singleton import *


class GlobalListeners(AsyncListeners, metaclass=SingletonType):
    """Singleton ilst for the purpose of holding global listeners"""
