import anyio
import pytest

@pytest.fixture
def anyio_backend():
    return 'trio'

def anyio_run(p,*a,**k):
    if 'backend' not in k:
        k['backend'] = "trio"
    return anyio.run(p,*a,**k)
__builtins__['anyio_run'] = anyio_run  # ugh
