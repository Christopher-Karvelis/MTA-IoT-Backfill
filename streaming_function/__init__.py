import logging

import azure.functions as func
import pydevd_pycharm

from .azure_function_streaming import AzureFunctionStreaming

pydevd_pycharm.settrace(
    "127.0.0.1", port=9091, stdoutToServer=True, stderrToServer=True
)

azure_streaming = AzureFunctionStreaming()


async def main(myblob: func.InputStream):
    logging.info("Python Blob trigger function processed %s", myblob.name)
    if myblob.length > 1:
        await azure_streaming.input(myblob)
