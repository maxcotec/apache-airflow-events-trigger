import json


def process_msg(message):
    raw = message.value()
    print(f"Raw type: {type(raw)}, raw content: {raw}")
    val = json.loads(raw)  # may fail if raw is not str/bytes
    print(f"Value in message is {val}")
    return val

def apply_function(*args, **kwargs):
    message = args[-1]
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return val