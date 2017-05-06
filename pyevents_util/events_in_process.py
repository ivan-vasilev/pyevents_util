import pyevents.events as events
import multiprocessing
import threading


class EventsInProcess(object, metaclass=events.GlobalRegister):
    """Run events in separate process"""

    def __init__(self, runnable):
        self.q = multiprocessing.Queue()

        self.process = multiprocessing.Process(target=runnable, args=(self.q,))

        def event_listener():
            while True:
                self._default_event_generator(self.q.get())

        self.events_thread = threading.Thread(target=event_listener, daemon=True)

    @events.after
    def _default_event_generator(self, event):
        return event

    def __call__(self, *args, **kwargs):
        if self.process.is_alive():
            raise Exception("You can call this method only once")

        self.events_thread.start()
        self.process.start()
