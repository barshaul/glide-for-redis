import os
import shutil
import subprocess
from pathlib import Path
import sys
from setuptools import setup
from setuptools.command.build_py import build_py as build_py_orig
from setuptools.command.build_ext import build_ext as build_ext_orig
from setuptools.command.sdist import sdist as sdist_orig
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel as bdist_wheel_orig

ROOT = Path(__file__).resolve().parent
REAL_FFI_PATH = ROOT.parent.parent / "ffi"            # ../.. from glide-sync/
LOCAL_FFI_SYMLINK = ROOT / "ffi"                      # glide-sync/ffi

def remove_existing(path: Path):
    if path.is_symlink():
        print(f"[INFO] Removing symlink: {path}")
        path.unlink()
    elif path.is_dir():
        print(f"[INFO] Removing directory: {path}")
        shutil.rmtree(path)
    elif path.exists():
        print(f"[INFO] Removing file: {path}")
        path.unlink()

from distutils.core import Command

class CleanCommand(Command):
    """Custom clean command to remove build artifacts."""
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import glob

        to_clean = [
            "build", "dist", "*.egg-info",
            "glide_shared", "ffi", "glide-core", "logger_core",
        ]

        for pattern in to_clean:
            for match in glob.glob(pattern):
                remove_existing(Path(match))

class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

class bdist_wheel(bdist_wheel_orig):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

class build_ext(build_ext_orig):
    def run(self):
        self.ensure_ffi_symlink()
        release = os.environ.get("GLIDE_SYNC_RELEASE", "0") == "1"
        env = os.environ.copy()
        env.update({
            "GLIDE_NAME": env.get("GLIDE_NAME", "GlidePySync"),
            "GLIDE_VERSION": env.get("GLIDE_VERSION", "0.0.0"),
        })

        print(f"[INFO] Building Rust FFI lib with cargo in {LOCAL_FFI_SYMLINK}")
        subprocess.run(
            ["cargo", "build"] + (["--release"] if release else []),
            cwd=LOCAL_FFI_SYMLINK,
            env=env,
            check=True
        )


        # Copy built library to package dir
        release = os.environ.get("GLIDE_SYNC_RELEASE", "0") == "1"
        target_dir = "release" if release else "debug"
        suffix = {
            "linux": ".so",
            "darwin": ".dylib",
            "win32": ".dll"
        }[os.sys.platform]
        lib_name = "libglide_ffi" + suffix

        built_lib = LOCAL_FFI_SYMLINK / "target" / target_dir / lib_name
        dest_dir = Path(self.build_lib) / "glide_sync"
        dest_dir.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] Copying {built_lib} → {dest_dir / lib_name}")
        shutil.copy2(built_lib, dest_dir / lib_name)
        
        super().run()

    def ensure_ffi_symlink(self):
        if not LOCAL_FFI_SYMLINK.exists():
            print(f"[INFO] Creating symlink: {LOCAL_FFI_SYMLINK} → {REAL_FFI_PATH}")
            LOCAL_FFI_SYMLINK.symlink_to(REAL_FFI_PATH, target_is_directory=True)

class sdist(sdist_orig):
    def run(self):
        print("[INFO] Preparing source distribution (sdist) with vendored Rust sources")
        to_copy = {
            "glide_shared": ROOT.parent / "glide-shared" / "glide_shared",
            "ffi": ROOT.parent.parent / "ffi",
            "glide-core": ROOT.parent.parent / "glide-core",
            "logger_core": ROOT.parent.parent / "logger_core",
        }

        def ignore_dirs(_, names):
            ignored = []
            if "target" in names:
                ignored.append("target")
            if "tests" in names:
                ignored.append("tests")
            return ignored

        for name, src_path in to_copy.items():
            dest_path = ROOT / name
            if dest_path.exists():
                remove_existing(dest_path)
            print(f"[INFO] Copying {src_path} → {dest_path}")
            shutil.copytree(src_path, dest_path, ignore=ignore_dirs)

        super().run()
        
class build_py(build_py_orig):
    def run(self):
        # Ensure dependencies in PATH
        for tool, hint_path in [("cargo", "~/.cargo/bin"), ("protoc", "~/.local/bin")]:
            if shutil.which(tool) is None:
                os.environ["PATH"] += os.pathsep + os.path.expanduser(hint_path)
                if shutil.which(tool) is None:
                    raise RuntimeError(f"[ERROR] Failed to find {tool} in PATH")

        # Vendor glide_shared    
        print("[INFO] Vendoring glide_shared into the built library folder")
        from_sdist = Path("PKG-INFO").exists()  # heuristic: sdist leaves PKG-INFO
        source = ROOT / "glide_shared" if from_sdist else ROOT.parent / "glide-shared" / "glide_shared"
        dest = Path(self.build_lib) / "glide_shared"
        shutil.copytree(source, dest, dirs_exist_ok=True)


        super().run()


class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import glob

        paths_to_remove = [
            "build", "dist", "*.egg-info", "glide_shared", "ffi", "glide-core", "logger_core"
        ]

        for path in paths_to_remove:
            for match in glob.glob(path):
                full_path = Path(match)
                if full_path.is_symlink():
                    print(f"[CLEAN] Removing symlink: {full_path}")
                    full_path.unlink()
                elif full_path.is_dir():
                    print(f"[CLEAN] Removing directory: {full_path}")
                    shutil.rmtree(full_path, ignore_errors=True)
                elif full_path.exists():
                    print(f"[CLEAN] Removing file: {full_path}")
                    full_path.unlink()

        # Optionally, clean Rust build artifacts
        ffi_path = ROOT / "ffi"
        target_path = ffi_path / "target"
        if target_path.exists():
            print(f"[CLEAN] Removing Rust target directory: {target_path}")
            shutil.rmtree(target_path, ignore_errors=True)

setup(
    name="valkey-glide-sync",
    packages=["glide_sync", "glide_shared"],
    install_requires=[
        "cffi>=1.0.0",
        "typing-extensions>=4.8.0",
        "protobuf>=3.20",
    ],
    package_data={"glide_sync": ["*.so", "*.dll", "*.dylib", "*.pyi", "py.typed"]},
    distclass=BinaryDistribution,
    cmdclass={
        "build_py": build_py,
        "build_ext": build_ext,
        "bdist_wheel": bdist_wheel,
        "sdist": sdist,
        "clean": CleanCommand,
    },
)
