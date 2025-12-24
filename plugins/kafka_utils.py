import json


def process_msg(message):
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return val

def apply_function(*args, **kwargs):
    message = args[-1]
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return val