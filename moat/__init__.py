__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from moat._dev_fix import _fix

_fix()
