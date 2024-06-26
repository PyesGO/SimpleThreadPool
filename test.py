import logging
import time

import core

logging.basicConfig(
    format="%(name)s-%(asctime)s-[%(levelname)s]: %(message)s",
    level=logging.INFO,
)

LOGGER = logging.getLogger(__name__)


def test(sec: int) -> None:
    # data = b"\0" * 1024**2
    time.sleep(2)
    LOGGER.info("The test function has running finished.")
    return "Test Str"


def cb(task: core.ThreadTask) -> None:
    LOGGER.info(task.get_result())


pool = core.SimpleThreadPool(threads=16)
for i in range(128):
    task = pool.submit(test, 1)
    task.add_callback(cb)

time.sleep(8)
pool.shutdown()
