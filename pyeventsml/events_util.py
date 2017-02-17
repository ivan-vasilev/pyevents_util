from pyevents.events import *
from pyeventsml.singleton import *


class GlobalListeners(AsyncListeners, metaclass=SingletonType):
    """Singleton ilst for the purpose of holding global listeners"""


class AttrDict(dict):
    """access dictionary keys as if they are attributes"""

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self
