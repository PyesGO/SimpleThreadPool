# built-in modules:
import os as _os
import typing as _tp
from collections import (
    deque as _deque,
)
from logging import (
    getLogger as _getLogger,
)
from _thread import (
    allocate_lock as _thread_get_allocate_lock,
    start_new_thread as _thread_new_instance,
)
from weakref import (
    WeakSet as _WeakSet,
)

_LOGGER = _getLogger(__name__)


def _counter(
    n: int = 0,
) -> _tp.Generator[int, _tp.Any, None]:
    """
    Every time you call the '__next__' or 'send' method of this Generator,
    it forever yield n+1.

    Any type of data can be sent,
    but will not have any effect.

    Finally, this Generator will not return any result.
    """
    while True:
        yield n
        n += 1


class ThreadTask(object):
    def __init__(
        self,
        name: str,
        any_callable: _tp.Callable,
        callable_args: _tp.Tuple[_tp.Any, ...],
        callable_kwargs: _tp.Mapping[str, _tp.Any],
    ) -> None:
        self._name = name
        self._cb = any_callable
        self._args = callable_args
        self._kwargs = callable_kwargs

        self._done = False
        self._result = None
        self._exception = None
        self._cabs: _tp.Deque[_tp.Callable[["ThreadTask"], _tp.Any]] = _deque()

    def _invoke_callback(self, callback: _tp.Callable[["ThreadTask"], _tp.Any]) -> None:
        _LOGGER.debug(
            "Ready to invoke '%s' (callback name) callback." % callback.__name__
        )
        try:
            callback(self)
        except Exception as e:
            _LOGGER.warning(
                (
                    "The '%s' (thread task name) encountered an error "
                    "while running the '%s' (callback name) callback. "
                    "The callback error is '%r'"
                )
                % (self._name, callback.__name__, e)
            )
        else:
            _LOGGER.debug("The '%s' (callback name) callback was run successful.")

    def _invoke_callbacks(self) -> None:
        while self._cabs:
            self._invoke_callback(self._cabs.popleft())

    def _run(self) -> None:
        _LOGGER.debug("The '%s' (thread task name) is ready to start." % self._name)
        try:
            self._result = self._cb(*self._args, **self._kwargs)
        except Exception as e:
            self._exception = e
        finally:
            self._done = True
            if not self._exception:
                _LOGGER.debug(
                    "The '%s' (thread task name) has correctly finished." % self._name
                )
            else:
                _LOGGER.warning(
                    (
                        "Oops, the '%s' (thread task name) is running with a little error."
                        "The error is '%r'"
                    )
                    % (self._name, self._exception)
                )
            self._invoke_callbacks()

    def _ensure_done(self, method: str) -> _tp.Optional[_tp.NoReturn]:
        if not self._done:
            raise RuntimeError(
                "You should not call the '%s' method because this thread task has not finished running."
                % method
            )

    @property
    def callbacks(self) -> _tp.List[_tp.Callable[["ThreadTask"], _tp.Any]]:
        return list(self._cabs)

    def add_callback(
        self, specific_callable: _tp.Callable[["ThreadTask"], _tp.Any]
    ) -> None:
        if self._done:
            self._invoke_callback(specific_callable)
        self._cabs.append(specific_callable)

    def is_done(self) -> bool:
        return self._done

    def get_result(self) -> _tp.Optional[_tp.Any]:
        self._ensure_done("get_result")
        if self._exception:
            raise self._exception
        return self._result

    def get_exception(self) -> _tp.Optional[Exception]:
        self._ensure_done("get_exception")
        return self._exception


class WorkerAndTaskManager(object):
    def __init__(self) -> None:
        self.workers: _tp.Deque[ThreadWorker] = _deque()
        self.running_workers: _WeakSet[ThreadWorker] = _WeakSet()
        self.waiting_workers: _WeakSet[ThreadWorker] = _WeakSet()
        #
        self.waiting_tasks: _tp.Deque[ThreadTask] = _deque()
        #
        self.global_allocate_lock = _thread_get_allocate_lock()
        self.get_local_allocate_lock = _thread_get_allocate_lock  # note: This callable is not have end parentheses.


class ThreadWorker(object):
    def __init__(self, manager: WorkerAndTaskManager, name: str) -> None:
        self._name = name
        self._manager = manager
        self._should_stop = False
        self._local_allocate_lock = manager.get_local_allocate_lock()

    def _wait(self) -> None:
        (not self._local_allocate_lock.locked()) and (
            self._local_allocate_lock.acquire(False)
        )
        self._local_allocate_lock.acquire(True)

    def _release(self) -> None:
        (self._local_allocate_lock.locked()) and (self._local_allocate_lock.release())

    def _interal_loop(self) -> None:
        while not self._should_stop:
            if (not self._manager.global_allocate_lock.locked()) and (
                self._manager.waiting_tasks
            ):
                self._manager.global_allocate_lock.acquire(True)
                task = self._manager.waiting_tasks.popleft()
                self._manager.global_allocate_lock.release()
                self._manager.running_workers.add(self)
                try:
                    task._run()
                except Exception as e:
                    _LOGGER.warning(e)
                finally:
                    self._manager.running_workers.discard(self)
                continue

            self._manager.waiting_workers.add(self)
            self._wait()
            self._manager.waiting_workers.discard(self)
        else:
            self._release()
            _LOGGER.debug(
                "The '%s' (thread worker name) has been correctly dismissed."
                % self._name
            )

    def start(self) -> None:
        _thread_new_instance(self._interal_loop, ())
        _LOGGER.debug(
            "Successful created thread worker '%s' (thread worker name)" % self._name
        )

    def dismiss(self) -> None:
        self._should_stop = True
        self._release()
        _LOGGER.debug("Dismiss thread worker command is already sent.")


class SimpleThreadPool(object):
    _task_counter = _counter()
    _worker_counter = _counter()
    _default_task_prefix = "thread_task_%d"
    _default_name_prefix = "thread_work_%d"

    def __init__(self, threads: int = 0) -> None:
        if threads <= 0:
            threads = _os.cpu_count()
            if not threads:
                raise TypeError(
                    "Because the os module does not provide the cpu count value, "
                    "so you need to pass in a number that is at least one for the 'threads' parameter."
                )

        self._should_shutdown = False
        self._manager = WorkerAndTaskManager()

        count = 0
        while count < threads:
            worker = ThreadWorker(
                manager=self._manager,
                name=self._default_name_prefix % self._worker_counter.__next__(),
            )
            self._manager.workers.append(worker)
            worker.start()
            count += 1

    def submit(
        self,
        any_callable: _tp.Callable,
        *arguments: _tp.Any,
        **keyword_arguments: _tp.Any
    ) -> ThreadTask:
        if self._should_shutdown:
            raise RuntimeError(
                "This simple thread pool has been closed, so you can not recall the 'submit' method."
            )
        task = ThreadTask(
            name=self._default_task_prefix % self._task_counter.__next__(),
            any_callable=any_callable,
            callable_args=arguments,
            callable_kwargs=keyword_arguments,
        )
        self._manager.waiting_tasks.append(task)
        return task

    def shutdown(self, wait: bool = True) -> None:
        self._should_shutdown = True
        _LOGGER.debug("Close the simple thread pool command is already sent.")

        for worker in self._manager.workers:
            worker.dismiss()

        while (wait) and (self._manager.running_workers):
            worker = self._manager.running_workers.pop()
            worker._wait()

        _LOGGER.debug("The simple thread pool has been correctly closed.")
