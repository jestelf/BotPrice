from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.processing.detectors import is_fake_msrp


def test_is_fake_msrp_true():
    assert is_fake_msrp(200, 100, 90)


def test_is_fake_msrp_false():
    assert not is_fake_msrp(120, 100, 90)
    assert not is_fake_msrp(None, 100, 90)
