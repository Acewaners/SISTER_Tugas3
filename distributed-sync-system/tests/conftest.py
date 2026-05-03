import pytest
import asyncio


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def reset_modules():
    yield
    import sys
    modules_to_clear = [key for key in sys.modules.keys() if key.startswith('src.')]
    for module in modules_to_clear:
        if module in sys.modules:
            del sys.modules[module]
