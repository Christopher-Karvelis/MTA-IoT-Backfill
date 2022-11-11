import logging

import azure.functions as func

from .azure_function_streaming import AzureFunctionStreaming

# import pydevd_pycharm
# pydevd_pycharm.settrace(
#     "127.0.0.1", port=9091, stdoutToServer=True, stderrToServer=True
# )

azure_streaming = AzureFunctionStreaming()


async def main(json_blob: func.InputStream):
    logging.info("Python Blob trigger function processed %s", json_blob.name)
    if json_blob.length is not None:
        await azure_streaming.input(json_blob)
    else:
        pass
