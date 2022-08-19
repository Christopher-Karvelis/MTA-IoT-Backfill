import azure.functions as func
from .azure_function_streaming import AzureFunctionStreaming

import pydevd_pycharm
pydevd_pycharm.settrace('127.0.0.1', port=9091, stdoutToServer=True, stderrToServer=True)

azure_streaming = AzureFunctionStreaming()


async def main(myblob: func.InputStream):
    await azure_streaming.input(myblob)