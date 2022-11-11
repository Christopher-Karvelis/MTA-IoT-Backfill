import logging

import azure.functions as func
import pydevd_pycharm

from .azure_function_streaming import AzureFunctionStreaming

pydevd_pycharm.settrace(
    "127.0.0.1", port=9091, stdoutToServer=True, stderrToServer=True
)

azure_streaming = AzureFunctionStreaming()


async def main(jsonblob: func.InputStream):
    logging.info("Python Blob trigger function processed %s", jsonblob.name)
    if jsonblob.length is not None:
        await azure_streaming.input(jsonblob)
    else:
        pass
