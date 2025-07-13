import os
import subprocess
from pathlib import Path
from setuptools import setup
from setuptools.command.build_py import build_py as build_py_orig
import shutil
from setuptools.dist import Distribution

class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True
from wheel.bdist_wheel import bdist_wheel as bdist_wheel_orig

class bdist_wheel(bdist_wheel_orig):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False  # 💡 Important: tells wheel this is platform-specific
        
class build_py(build_py_orig):
    def run(self):
        # Patch PATH if cargo is not found
        if shutil.which("cargo") is None:
            os.environ["PATH"] += os.pathsep + os.path.expanduser("~/.cargo/bin")
            os.environ["PATH"] += os.pathsep + os.path.expanduser("$HOME/.cargo/bin")
            if shutil.which("cargo") is None:
                print("[ERROR] Failed to find cargo")
                exit(1)
        # Patch protobuf if cargo is not found
        if shutil.which("protoc") is None:
            os.environ["PATH"] += os.pathsep + os.path.expanduser("~/.local/bin")
            os.environ["PATH"] += os.pathsep + os.path.expanduser("$HOME/.local/bin")
            if shutil.which("protoc") is None:
                print("[ERROR] Failed to find protoc")
                exit(1)
        # Build the Rust FFI lib via cargo
        release = os.environ.get("GLIDE_SYNC_RELEASE", "0") == "1"
        target_dir = "release" if release else "debug"
        env = os.environ.copy()
        env.update({
            "GLIDE_NAME": env.get("GLIDE_NAME", "GlidePySync"),
            "GLIDE_VERSION": env.get("GLIDE_VERSION", "0.0.0"),
        })

        print(f"[INFO] Building FFI lib using cargo... {Path(__file__).resolve()}")
        subprocess.run(["cargo", "build"] + (["--release"] if release else []),
                       cwd=Path(__file__).resolve().parent.parent.parent / "ffi",
                       env=env,
                       check=True)

        # Copy resulting .so/.dll/.dylib to the Python package directory
        lib_name = "libglide_ffi"
        suffix = {
            "linux": ".so",
            "darwin": ".dylib",
            "win32": ".dll"
        }[os.sys.platform]

        built_lib = Path(__file__).resolve().parent.parent.parent / "ffi" / "target" / target_dir / f"{lib_name}{suffix}"
        dest_dir = Path(self.build_lib) / "glide_sync"  
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / f"{lib_name}{suffix}"
        print(f"[INFO] Copying built lib from {built_lib} to {dest_path}")
        dest_path.write_bytes(built_lib.read_bytes())

        super().run()

setup(
    name="valkey-glide-sync",
    packages=["glide_sync"],
    package_data={"glide_sync": ["*.so", "*.dll", "*.dylib", "*.pyi", "py.typed"]},
    distclass=BinaryDistribution,
    cmdclass={
        "build_py": build_py,
        "bdist_wheel": bdist_wheel,
    },
)
