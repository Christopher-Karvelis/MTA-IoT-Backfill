import logging
from datetime import datetime

import azure.functions as func

from .azure_function_streaming import AzureFunctionStreaming

# import pydevd_pycharm


# pydevd_pycharm.settrace(
#     "127.0.0.1", port=9091, stdoutToServer=True, stderrToServer=True
# )

azure_streaming = AzureFunctionStreaming()


async def main(jsonblob: func.InputStream):
    logging.info("Python Blob trigger function processed %s", jsonblob.name)
    if jsonblob.length is not None:
        day_of_chunk = datetime.strptime(jsonblob.name.split("/")[1], "%Y-%m-%d")
        await azure_streaming.input(jsonblob, date_accepted=day_of_chunk)
