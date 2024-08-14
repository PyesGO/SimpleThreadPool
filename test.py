import logging
import time

import core

logging.basicConfig(
    format="%(name)s-%(asctime)s-[%(levelname)s]: %(message)s",
    level=logging.INFO,
)

LOGGER = logging.getLogger(__name__)


def test(x: int) -> None:
    LOGGER.info("The test function %d start running." % x)
    time.sleep(2)
    LOGGER.info("The test function %d  has running finished." % x)
    return "function return."


def cb(callback_task: core.ThreadTask) -> None:
    LOGGER.info(callback_task.get_result())


pool = core.SimpleThreadPool(threads=32)
for i in range(64):
    task = pool.submit(test, i)
    task.add_callback(cb)

# wait timeout 2 seconds.
# pool.wait(2)
pool.wait()
print("main: wait release")
pool.shutdown()
# pool.shutdown(1)
print("main: pool has been shutdown successfully.")
