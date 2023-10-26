import json
import logging
from typing import Dict

import azure.functions as func

from Triggerer.input_structure import InputParameters
from Triggerer.move_blobs_to_backfill_container import move_blobs



async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body: Dict = req.get_json()
        input_parameters = InputParameters(**req_body)
        logging.info(input_parameters)
    except ValueError:
        logging.error(f"JSON could not be read from body: {req.get_body().decode()}")

    return json.dumps(move_blobs(input_parameters.dict()))

