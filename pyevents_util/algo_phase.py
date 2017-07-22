import logging
import queue
import threading
import typing

from pyevents import events


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


class AlgoPhaseEventsOrder(object, metaclass=events.GlobalRegister):
    def __init__(self, phases: typing.List[typing.Tuple[str, int]], phase_suffix='_unordered'):
        self.phases = phases
        self.phase_suffix = phase_suffix

        self._lock = threading.RLock()

        self.event_queues = {p[0]: queue.Queue() for p in phases}

        self.phases_queue = queue.Queue()

        self.phases_count = {p[0]: 0 for p in phases}

        self.thread = None

    @events.listener
    def listener(self, event):
        if isinstance(event, dict) and 'type' in event and event['type'] == 'after_iteration' and 'phase' in event:
            if event['phase'].endswith(self.phase_suffix):
                raise Exception("after_iteration events cannot be unordered")

            self.start_generator()

            with self._lock:
                phase = event['phase']

                self.phases_count[phase] += 1

                ind = [p[0] for p in self.phases].index(phase)

                if self.phases_count[phase] == self.phases[ind][1]:
                    self.phases_count[phase] = 0
                    self.phases_queue.put(self.phases[(ind + 1) % len(self.phases)][0])
        elif isinstance(event, dict) and 'type' in event and event['type'] == 'data' and 'phase' in event and event['phase'].endswith(self.phase_suffix):
            self.start_generator()
            self.event_queues[event['phase'].replace(self.phase_suffix, '')].put(event)

    @events.after
    def generator(self, event):
        return event

    def start_generator(self):
        with self._lock:
            if self.thread is None:
                self.phases_queue.put(self.phases[0][0])

                def events_generator():
                    while True:
                        phase = self.phases_queue.get()

                        if phase is None:
                            break

                        for i in range(self.phases[[p[0] for p in self.phases].index(phase)][1]):
                            event = self.event_queues[phase].get().copy()
                            event['phase'] = phase
                            self.generator(event)
                            self.event_queues[phase].task_done()

                        self.phases_queue.task_done()

                self.thread = threading.Thread(target=events_generator, daemon=True)
                self.thread.start()
