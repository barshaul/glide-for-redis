import importlib
import sys

# # Try importing the sync or async backend depending on what's installed
# try:
#     backend = importlib.import_module("glide_sync")
# except ImportError:
#     try:
#         backend = importlib.import_module("glide_async")
#     except ImportError:
#         raise ImportError(
#             "No Glide backend installed. Install with `valkey-glide[sync]` or `valkey-glide[async]`"
#         )

# # Populate the module's namespace with the backend's exports
# globals().update({name: getattr(backend, name) for name in getattr(backend, "__all__", [])})
# __all__ = getattr(backend, "__all__", [])

import importlib
import sys

__all__ = []

def try_import(module_name):
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None

sync_backend = try_import("glide_sync")
async_backend = try_import("glide_async")

if not sync_backend and not async_backend:
    raise ImportError(
        "No Glide backend installed. Install with `valkey-glide[sync]`, `valkey-glide[async]`, or `valkey-glide[full]`"
    )

# Sync-only mode
if sync_backend and not async_backend:
    globals().update({name: getattr(sync_backend, name) for name in getattr(sync_backend, "__all__", [])})
    __all__.extend(getattr(sync_backend, "__all__", []))

# Async-only mode
elif async_backend and not sync_backend:
    globals().update({name: getattr(async_backend, name) for name in getattr(async_backend, "__all__", [])})
    __all__.extend(getattr(async_backend, "__all__", []))

# Full mode – expose both under separate names
elif sync_backend and async_backend:
    from glide.sync.glide_client import GlideClient as SyncGlideClient, GlideClusterClient as SyncGlideClusterClient, TGlideClient as TSyncGlideClient
    from glide.glide_async.glide_client import GlideClient as AsyncGlideClient, GlideClusterClient as AsyncGlideClusterClient, TGlideClient as TAsyncGlideClient

    globals().update({
        "SyncGlideClient": SyncGlideClient,
        "SyncGlideClusterClient": SyncGlideClusterClient,
        "TSyncGlideClient": TSyncGlideClient,
        "AsyncGlideClient": AsyncGlideClient,
        "AsyncGlideClusterClient": AsyncGlideClusterClient,
        "TAsyncGlideClient": TAsyncGlideClient,
    })
    globals().update({name: getattr(async_backend, name) for name in getattr(async_backend, "__all__", [])})
    __all__.extend(getattr(async_backend, "__all__", [
        "SyncGlideClient", "SyncGlideClusterClient", "TSyncGlideClient",
        "AsyncGlideClient", "AsyncGlideClusterClient", "TAsyncGlideClient",
    ]))
