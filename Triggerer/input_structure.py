from pydantic import BaseModel


class InputParameters(BaseModel):
    ts_start: str
    ts_end: str
