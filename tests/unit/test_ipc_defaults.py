import multiprocessing
import threading

from pyarrow.bigquery import exchange
from pyarrow.bigquery.read import reader


def test_process_default_ipc_is_arrow_ipc():
    r = reader("project.dataset.table", worker_type=multiprocessing.Process)
    assert isinstance(r.ipc_exchange, exchange.ArrowIpc)


def test_thread_default_ipc_is_memory():
    r = reader("project.dataset.table", worker_type=threading.Thread)
    assert isinstance(r.ipc_exchange, exchange.Memory)
