import logging
import inspect
import json

class SimpleJsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "levelname": record.levelname,
            "asctime": self.formatTime(record, self.datefmt),
            "name": record.name,
            "pathname": record.pathname,
            "lineno": record.lineno,
            "process": record.process,
            "thread": record.thread,
            "request_id": getattr(record, "request_id", None),
            "message": record.getMessage(),
        }
        return json.dumps(log_record)


def get_logger(name: str | None = None) -> logging.Logger:
    """Get Logger"""

    if name is None:
        caller_frame = inspect.stack()[1]
        name = caller_frame.filename.split("/")[-1]
    root = logging.getLogger(name)

    if not getattr(root, "handler_set", None):
        root.setLevel(logging.INFO)
        h = logging.StreamHandler()
        f = SimpleJsonFormatter()
        root.propagate = False

        h.setFormatter(f)
        root.addHandler(h)
        root.handler_set = True

    return root