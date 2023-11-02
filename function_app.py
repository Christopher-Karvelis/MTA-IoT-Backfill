import azure.functions as func
from Triggerer import bp

app = func.FunctionApp()

app.register_functions(bp)

