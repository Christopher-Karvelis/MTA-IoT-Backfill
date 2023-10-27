import logging

from CopyBlobs.move_blobs_to_backfill_container import move_blobs
from Triggerer import InputParameters


def main(inputParameters: dict) -> str:
    logging.info(f"Running with {inputParameters=}")
    input_parameters = InputParameters(**inputParameters)
    move_blobs(input_parameters.dict())
    return "Success"

