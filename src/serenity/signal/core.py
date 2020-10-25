from tau.core import Event, NetworkScheduler, Signal, MutableSignal


class Pipe(Event):
    def __init__(self, scheduler: NetworkScheduler, in_signal: Signal, out_signal: MutableSignal):
        self.scheduler = scheduler
        self.in_signal = in_signal
        self.out_signal = out_signal
        self.scheduler.network.connect(in_signal, self)

    def on_activate(self) -> bool:
        if self.in_signal.is_valid():
            value = self.in_signal.get_value()
            self.scheduler.schedule_update(self.out_signal, value)
            return True
        else:
            return False
