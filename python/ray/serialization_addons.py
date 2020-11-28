"""
This module is intended for implementing internal serializers for some
site packages.
"""

from importlib import invalidate_caches
from importlib.abc import SourceLoader
from importlib.machinery import FileFinder
import sys
import warnings

_hook_installed = False
_post_import_hooks = {}


class SerializerLoader(SourceLoader):
    def __init__(self, fullname, path):
        self.fullname = fullname
        self.path = path

    def get_filename(self, fullname):
        return self.path

    def get_data(self, filename):
        with open(filename) as f:
            return f.read()

    def get_code(self, fullname):
        code = super().get_code(fullname)
        if fullname in _post_import_hooks:
            _post_import_hooks[fullname]()
        return code


def lazy_register_serializer(module_name, func):
    if module_name not in sys.modules:
        _post_import_hooks[module_name] = func
    else:
        func()


def install():
    global _hook_installed
    if _hook_installed:
        return
    _hook_installed = True
    loader_details = (SerializerLoader, [".py"])
    # insert the path hook ahead of other path hooks
    sys.path_hooks.insert(0, FileFinder.path_hook(loader_details))
    # clear any loaders that might already be in use by the FileFinder
    sys.path_importer_cache.clear()
    invalidate_caches()


####################
# PyTorch serialzier
####################

_TORCH_WARNING_FILTER_ACTIVATE = True


class _TorchTensorReducingHelper:
    def __init__(self, tensor):
        self.tensor = tensor

    @classmethod
    def rebuild_tensor(cls, rebuild_func, device, ndarray, params):
        import torch
        global _TORCH_WARNING_FILTER_ACTIVATE
        # filtering warning messages would be the bottleneck for
        # deserializing torch tensors. Since the warning only prompts once,
        # we would only deal with it for the first time.
        if _TORCH_WARNING_FILTER_ACTIVATE:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=UserWarning,
                    message="The given NumPy array is not writeable")
                _tensor = torch.from_numpy(ndarray)
            _TORCH_WARNING_FILTER_ACTIVATE = False
        else:
            _tensor = torch.from_numpy(ndarray)
        if device != torch.device("cpu"):
            _tensor = _tensor.to(device)
        tensor = rebuild_func(_tensor.storage(), *params)
        return cls(tensor)

    @classmethod
    def rebuild_sparse_tensor(cls, rebuild_func, content):
        tensor = rebuild_func(*content)
        return cls(tensor)

    def __reduce_ex__(self, protocol):
        _rebuild_func, content = self.tensor.__reduce_ex__(protocol)
        if self.tensor.is_sparse:
            # Torch will help us reduce the sparse tensor into
            # several continuous tensors.
            return self.rebuild_sparse_tensor, (_rebuild_func, content)
        # By only replacing the storage with a numpy array, we can reuse
        # zero-copy serialization while keeping all other params of the
        # torch tensor.
        return self.rebuild_tensor, (_rebuild_func, self.tensor.device,
                                     self.tensor.detach().cpu().numpy(),
                                     content[1:])


def _unwrap_tensor(s):
    return s.tensor


def torch_tensor_reducer(tensor):
    return _unwrap_tensor, (_TorchTensorReducingHelper(tensor), )


def apply(serialization_context):
    install()

    def init_pytorch_serializer():
        import torch
        serialization_context._register_cloudpickle_reducer(
            torch.Tensor, torch_tensor_reducer)

    lazy_register_serializer("torch", init_pytorch_serializer)
