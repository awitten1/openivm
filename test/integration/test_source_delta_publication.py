#!/usr/bin/env python3
"""Run namespace lifecycle checks with an isolated database for real reopen coverage."""

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory(prefix="openivm-lifecycle-") as directory:
    binary = str(Path(sys.argv[1]).resolve())
    with subprocess.Popen(
        [binary, str(Path(directory) / "namespace.db")], stderr=subprocess.PIPE, text=True
    ) as process:
        for line in process.stderr:
            print(line, end="", file=sys.stderr, flush=True)
            # Static Linux binaries retain symbols but backtrace_symbols cannot
            # resolve them. Decode DuckDB's addresses while the binary is available.
            if '"stack_trace_pointers"' in line and shutil.which("addr2line"):
                pointers = json.loads(line).get("stack_trace_pointers", "")
                if pointers:
                    subprocess.run(
                        ["addr2line", "-f", "-C", "-e", binary] + [hex(int(p)) for p in pointers.split(";")],
                        check=True,
                    )
        if process.wait():
            raise subprocess.CalledProcessError(process.returncode, process.args)
