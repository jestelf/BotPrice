import re

_BANNED = [r"(?i)extremism", r"(?i)terror", r"(?i)spam"]

class PolicyError(ValueError):
    pass

def check_text(text: str) -> None:
    for pat in _BANNED:
        if re.search(pat, text):
            raise PolicyError("Запрос нарушает правила использования")
