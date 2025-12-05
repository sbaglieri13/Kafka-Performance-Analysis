import os
import time
import itertools
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

RUN_ID = os.getenv("RUN_ID")
PAYLOAD_SIZE_BYTES = int(os.getenv("PAYLOAD_SIZE_BYTES"))

# contatore di key incrementale
key_counter = itertools.count(start=1)


def make_message_string() -> str:
    base = "xxxx"
    padding_len = max(PAYLOAD_SIZE_BYTES - len(base), 0)
    s = base + ("x" * padding_len)
    return s[:PAYLOAD_SIZE_BYTES]


class Message(BaseModel):
    key: str
    run_id: str
    message: str
    generator_timestamp_ms: float


@app.get("/next-message", response_model=Message)
def next_message():
    key = str(next(key_counter))
    msg_text = make_message_string()
    ts_ms = time.time_ns() / 1_000_000  # converto in millisecondi

    return Message(
        key=key,
        run_id=RUN_ID,
        message=msg_text,
        generator_timestamp_ms=ts_ms,
    )
