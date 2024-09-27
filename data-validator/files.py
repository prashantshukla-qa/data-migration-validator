#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Helper functions to read YAML files from a directory.
"""

import glob
import os.path


def list_yamls(directory: str) -> list[str]:
    """Get YAML file glob from a directory."""
    return glob.glob(os.path.join(directory, "*.yaml"))
