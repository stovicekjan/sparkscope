import logging

TRACE_LEVEL_NUM = 5


class SparkscopeLogger(logging.getLoggerClass()):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

    def trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(TRACE_LEVEL_NUM):
            self.log(TRACE_LEVEL_NUM, msg, args, **kwargs)


logging.setLoggerClass(SparkscopeLogger)
