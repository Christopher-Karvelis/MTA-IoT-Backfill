import azure.functions as func
from .azure_function_streaming import AzureFunctionStreaming

azure_streaming = AzureFunctionStreaming()

async def main(myblob: func.InputStream):
    await azure_streaming.input(myblob)