import json

from pytest import fixture


@fixture(scope="session")
def messages():
    data_list = [
        {
            "type": "SCHEMA",
            "stream": "my_stream",
            "schema": {},
            "key_properties": []
        },
        {
            "type": "RECORD",
            "stream": "my_stream",
            "record": {
                "name": "kyrie",
                "age": 10,
            }
        },
        {
            "type": "RECORD",
            "stream": "my_stream",
            "record": {
                "name": "paul",
                "age": 20,
            }
        }
    ]
    return [json.dumps(d) for d in data_list]

