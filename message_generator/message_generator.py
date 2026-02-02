import os
import time
import itertools
from fastapi import FastAPI
from pydantic import BaseModel
import random, string

app = FastAPI()

RUN_ID = os.getenv("RUN_ID")
PAYLOAD_SIZE_BYTES = int(os.getenv("PAYLOAD_SIZE_BYTES"))

# contatore di key incrementale
key_counter = itertools.count(start=1)


import random, string
def make_message_string() -> str:
    # genera una stringa casuale di lettere minuscole della lunghezza specificata
    return ''.join(random.choices(string.ascii_lowercase, k=PAYLOAD_SIZE_BYTES))

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
