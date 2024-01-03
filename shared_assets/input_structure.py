from typing import List
from pydantic import BaseModel


class InputParameters(BaseModel):
    ts_start: str
    ts_end: str


class BackFillInputParameters(InputParameters):
    blobs_to_consider: List[str]
