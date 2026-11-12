#!/usr/bin/env python3
"""MeshCore coverage/link worker process entrypoint."""

import logging
import os
from worker_settings import parse_settings
from worker_context import WorkerContext
from osgeo import gdal
from worker_runtime import main

if __name__ == "__main__":
    gdal.UseExceptions()
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    main(context=WorkerContext(settings=parse_settings(os.environ)))
