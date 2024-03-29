import logging

TRACE_LEVEL_NUM = 5


class SparkscopeLogger(logging.getLoggerClass()):
    """
    Custom logging class. Enables logging with TRACE level.
    """
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

    def trace(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'TRACE'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.debug("Houston, we have a %s", "thorny problem", exc_info=1)
        """
        if self.isEnabledFor(TRACE_LEVEL_NUM):
            self.log(TRACE_LEVEL_NUM, msg, *args, **kwargs)


logging.setLoggerClass(SparkscopeLogger)
