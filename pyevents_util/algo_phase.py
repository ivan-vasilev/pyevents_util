import pyevents.events as events
import logging
import threading


class AlgoPhase(object, metaclass=events.GlobalRegister):
    """Simple training/testing/evaluation class"""

    def __init__(self, model, phase=None, event_processor=None):
        self._phase = phase
        self._model = model
        self._iteration = 0
        self._lock = threading.RLock()

        if event_processor is not None:
            self.input_event_processor = event_processor
        else:
            self.input_event_processor = self.onevent

    def process(self, data):
        with self._lock:
            self._iteration += 1
            iteration = self._iteration

        self.before_iteration({'model': self._model, 'phase': self._phase, 'iteration': iteration, 'model_input': data})

        logging.getLogger(__name__).debug("Phase " + str(self._phase) + " iteration " + str(iteration))

        model_output = self._model(data)

        self.after_iteration({'model': self._model, 'phase': self._phase, 'iteration': iteration, 'model_input': data, 'model_output': model_output})

    @events.listener
    def onevent(self, event):
        if event['type'] == 'data' and 'phase' in event and event['phase'] == self._phase:
            self.process(event['data'])

    @events.after
    def before_iteration(self, event):
        event['type'] = 'before_iteration'
        return event

    @events.after
    def after_iteration(self, event):
        event['type'] = 'after_iteration'
        return event
