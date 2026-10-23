import logging
from dataclasses import dataclass

import torch

try:
    import pynvml

    PYNVML_AVAILABLE = True
except ImportError:
    PYNVML_AVAILABLE = False

logger = logging.getLogger("auraflow.gpu")


class GPUOutOfMemoryBudgetError(Exception):
    """Raised when available GPU VRAM is below the requested threshold."""


@dataclass
class GPUMemorySnapshot:
    device_index: int
    device_name: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    utilization_pct: float


class GPUMemoryManager:
    def __init__(self, device_index: int = 0, safety_margin_mb: int = 512):
        self.device_index = device_index
        self.safety_margin_bytes = safety_margin_mb * 1024 * 1024
        self.cuda_available = torch.cuda.is_available()

        if self.cuda_available and PYNVML_AVAILABLE:
            try:
                pynvml.nvmlInit()
                self._nvml_handle = pynvml.nvmlDeviceGetHandleByIndex(device_index)
            except Exception as e:
                logger.warning(
                    f"Failed to initialize NVML: {e}. Falling back to PyTorch CUDA APIs."
                )
                self._nvml_handle = None
        else:
            self._nvml_handle = None

    def get_memory_snapshot(self) -> GPUMemorySnapshot:
        """
        Returns hardware telemetry for the configured GPU device.
        """
        if not self.cuda_available:
            # Emulated fallback for CPU-only testing environments
            return GPUMemorySnapshot(
                device_index=-1,
                device_name="CPU Emulation",
                total_bytes=16 * 1024 * 1024 * 1024,
                used_bytes=2 * 1024 * 1024 * 1024,
                free_bytes=14 * 1024 * 1024 * 1024,
                utilization_pct=12.5,
            )

        if self._nvml_handle is not None:
            info = pynvml.nvmlDeviceGetMemoryInfo(self._nvml_handle)
            name = pynvml.nvmlDeviceGetName(self._nvml_handle)
            if isinstance(name, bytes):
                name = name.decode("utf-8")
            total = info.total
            used = info.used
            free = info.free
        else:
            total = torch.cuda.get_device_properties(self.device_index).total_memory
            reserved = torch.cuda.memory_reserved(self.device_index)
            used = reserved
            free = total - reserved
            name = torch.cuda.get_device_name(self.device_index)

        util_pct = (used / total) * 100.0 if total > 0 else 0.0

        return GPUMemorySnapshot(
            device_index=self.device_index,
            device_name=name,
            total_bytes=total,
            used_bytes=used,
            free_bytes=free,
            utilization_pct=round(util_pct, 2),
        )

    def can_allocate(self, required_bytes: int) -> bool:
        """
        Validates if required_bytes fits within free VRAM plus safety margin.
        """
        snapshot = self.get_memory_snapshot()
        available_headroom = snapshot.free_bytes - self.safety_margin_bytes
        return available_headroom >= required_bytes

    def reserve_budget(self, required_bytes: int) -> None:
        """
        Throws GPUOutOfMemoryBudgetError if insufficient headroom is available.
        """
        snapshot = self.get_memory_snapshot()
        available_headroom = snapshot.free_bytes - self.safety_margin_bytes
        if required_bytes > available_headroom:
            req_mb = required_bytes / (1024 * 1024)
            avail_mb = available_headroom / (1024 * 1024)
            raise GPUOutOfMemoryBudgetError(
                f"Requested {req_mb:.2f}MB VRAM exceeds available headroom ({avail_mb:.2f}MB) "
                f"on {snapshot.device_name} (Safety Reserve: {self.safety_margin_bytes / (1024 * 1024):.0f}MB)"
            )

    def clear_cache(self) -> None:
        """
        Forces explicit release of cached PyTorch CUDA allocations.
        """
        if self.cuda_available:
            torch.cuda.empty_cache()
            torch.cuda.ipc_collect()
