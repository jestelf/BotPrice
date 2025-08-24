import logging
import re

class PiiFilter(logging.Filter):
    _pat = re.compile(r"chat_id=\d+")

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - trivial
        msg = record.getMessage()
        msg = self._pat.sub("chat_id=<redacted>", msg)
        record.msg = msg
        record.args = ()
        return True

def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().addFilter(PiiFilter())
