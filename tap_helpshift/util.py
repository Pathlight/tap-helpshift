import queue
import logging
import singer


def consume_q(q):
    while True:
        try:
            yield q.get_nowait()
        except queue.Empty:
            return

class TapLoggerAdapter(logging.LoggerAdapter):
    """
    TapLoggerAdapter is a subclass of LoggerAdapter that:

    1. Merges in `extra` props passed by the user to the final `extra` props
       (LoggerAdapter simply ignores any `extra` passed into its log methods)
    2. If we are wrapping another LoggerAdapter, TapLoggerAdapter will merge
       the `extra` objects of both into the final output (LoggerAdapter only
       uses the wrapped adapter's `extra`)
    """
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra)

    def process(self, msg, kwargs):
        extra = kwargs.pop('extra', {})
        msg, kwargs = super().process(msg, kwargs)
        if isinstance(self.logger, logging.LoggerAdapter):
            msg, kwargs = self.logger.process(msg, kwargs)
        kwargs['extra'] = {**kwargs['extra'], **extra}
        formatted_msg = ', '.join([msg] + [f'{k}={v}' for k, v in kwargs['extra'].items()])
        return formatted_msg, kwargs

def get_logger(extra=None):
    return TapLoggerAdapter(singer.get_logger(), extra)
