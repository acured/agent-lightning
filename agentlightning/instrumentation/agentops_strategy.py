import json
import threading
import time
import queue
from typing import List, Dict, Any, Literal, Optional


UploadMode = Literal["upload", "batch", "local", "skip"]


# -------------------------------
# 1️⃣ Endpoint Interface Definition
# -------------------------------
class IStorageOperater():
    """All storage backends must implement this interface."""

    def save(self, traces: List[Dict[str, Any]]) -> None:
        """Upload traces to the destination."""
        ...


# -------------------------------
# 2️⃣ Endpoint Implementations
# -------------------------------

class AgentOpsOperater(IStorageOperater):
    """Upload traces to the remote AgentOps service."""

    def __init__(self):
        pass

    def save(self, traces: List[Dict[str, Any]]):
        print(f"[AgentOpsEndpoint] Uploading {len(traces)} traces to AgentOps")
        # TODO: Real upload logic (e.g. requests.post(self.url, json=traces))


class LocalFileOperater(IStorageOperater):
    """Write traces to a local file."""

    def __init__(self, file_path: str = "traces.jsonl"):
        self.file_path = file_path

    def save(self, traces: List[Dict[str, Any]]):
        with open(self.file_path, "a", encoding="utf-8") as f:
            for t in traces:
                f.write(json.dumps(t, ensure_ascii=False) + "\n")
        print(f"[LocalFileEndpoint] Wrote {len(traces)} traces to {self.file_path}")



# -------------------------------
# 3️⃣ Upload Strategy Controller
# -------------------------------
class StorageUploader:
    _instance = None
    _lock = threading.Lock()  # class-level lock for singleton init

    def __new__(cls, *args, **kwargs):
        # Thread-safe singleton instantiation
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(StorageUploader, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        mode: UploadMode = "upload",
        storage: Optional[IStorageOperater] = None,
        batch_size: int = 10,
        flush_interval: float = 5.0,
    ):
        """
        Unified uploader controller (thread-safe + singleton).
        """
        # Prevent reinitialization when called multiple times
        if hasattr(self, "_initialized") and self._initialized:
            return

        self.mode = mode
        self.storage = storage or AgentOpsOperater()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._thread_started = False
        self._queue_lock = threading.Lock()

        if self.mode == "batch":
            self._start_batch_thread()

        self._initialized = True

    # -------------------------------
    # Public API
    # -------------------------------
    def upload_trace(self, trace: Dict[str, Any]):
        """Thread-safe trace upload interface."""
        if self.mode == "skip":
            return

        if self.mode in ("upload", "local"):
            # Direct send; protect file I/O
            self.storage.save([trace])

        elif self.mode == "batch":
            with self._queue_lock:
                self._queue.put(trace)

    def shutdown(self):
        """Stop background thread and flush remaining traces."""
        self._stop_event.set()
        if self.mode == "batch" and self._thread_started:
            self._thread.join(timeout=5)

    # -------------------------------
    # Internal Batch Thread
    # -------------------------------
    def _start_batch_thread(self):
        if not self._thread_started:
            self._thread = threading.Thread(target=self._batch_uploader, daemon=True)
            self._thread.start()
            self._thread_started = True

    def _batch_uploader(self):
        batch = []
        last_flush = time.time()

        while not self._stop_event.is_set():
            try:
                trace = self._queue.get(timeout=1)
                batch.append(trace)
            except queue.Empty:
                pass

            if len(batch) >= self.batch_size or time.time() - last_flush >= self.flush_interval:
                if batch:
                    self.storage.save(batch)
                    batch.clear()
                    last_flush = time.time()

        # Final flush before shutdown
        if batch:
            self.storage.save(batch)


if __name__ == "__main__":
    import random

    def worker(thread_id: int):
        uploader = StorageUploader(mode="batch")
        for i in range(5):
            trace = {"thread": thread_id, "id": f"{thread_id}-{i}", "msg": "trace event"}
            uploader.upload_trace(trace)
            time.sleep(random.random() * 0.5)

    # Spawn multiple threads generating traces
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Stop background batch thread and flush
    StorageUploader().shutdown()
    print("✅ All traces uploaded safely.")