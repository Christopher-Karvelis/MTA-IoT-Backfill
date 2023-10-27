import json
import logging
from typing import Dict

import azure.durable_functions as df
import azure.functions as func

from Triggerer.input_structure import InputParameters


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    try:
        req_body: Dict = req.get_json()
        input_parameters = InputParameters(**req_body)
        logging.info(input_parameters)
    except ValueError:
        logging.error(f"JSON could not be read from body: {req.get_body().decode()}")
        error_message = "Bad Request: Invalides Json."
        return func.HttpResponse(json.dumps({"error": error_message}), status_code=400)

    instance_id = await client.start_new("Orchestrator", client_input=input_parameters.dict())

    return client.create_check_status_response(req, instance_id)
