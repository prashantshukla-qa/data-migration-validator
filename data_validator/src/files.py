import glob
import os.path


def list_yamls(directory: str) -> list[str]:
    return glob.glob(os.path.join(directory, "*.yaml"))
