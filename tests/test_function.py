import json
import pytest
from target_parquet import persist_messages


def jsonize_messages(messages):
    messages = [json.dumps(d) for d in messages]
    return messages


def test_saved_path_name(mocker):
    mocker_to_parquet = mocker.patch("pandas.DataFrame.to_parquet")
    messages = [
        {"type": "SCHEMA", "stream": "my_stream", "schema": {}, "key_properties": []},
        {"type": "RECORD", "stream": "my_stream", "record": {"name": "kyrie", "age": 10}},
        {"type": "RECORD", "stream": "my_stream", "record": {"name": "paul", "age": 20}},
    ]
    messages = jsonize_messages(messages)
    persist_messages(messages, "/tmp", "wow", "a.parquet", compression_method=None)
    mocker_to_parquet.assert_called_with("/tmp/my_stream/wow/a.parquet", engine="pyarrow")


def test_send_records_before_sending_schema(mocker):
    mocker.patch("pandas.DataFrame.to_parquet")

    messages = [
        {"type": "RECORD", "stream": "my_stream", "record": {"name": "kyrie", "age": 10}},
        {"type": "RECORD", "stream": "my_stream", "record": {"name": "paul", "age": 20}},
    ]
    messages = jsonize_messages(messages)
    with pytest.raises(Exception):
        persist_messages(messages, "/tmp", "", "a.parquet", compression_method=None)


def test_send_two_type_messages(mocker):
    mocker_to_parquet = mocker.patch("pandas.DataFrame.to_parquet")

    schema1 = {
        'title': 'MyStream',
        'type': 'object',
        'properties': {
            'name': {'title': 'Name', 'type': 'string'},
            'age': {'title': 'Age', 'type': 'integer'}
        },
        'required': ['name', 'age']
    }
    schema2 = {
        'title': 'YourStream',
        'type': 'object',
        'properties': {
            'nickname': {'title': 'Nickname', 'type': 'string'},
        },
        'required': ['nickname']
    }
    messages = [
        {"type": "SCHEMA", "stream": "my_stream", "schema": schema1, "key_properties": []},
        {"type": "RECORD", "stream": "my_stream", "record": {"name": "kyrie", "age": 10}},
        {"type": "RECORD", "stream": "my_stream", "record": {"name": "paul", "age": 20}},
        {"type": "SCHEMA", "stream": "your_stream", "schema": schema2, "key_properties": []},
        {"type": "RECORD", "stream": "your_stream", "record": {"nickname": "river"}},
        {"type": "RECORD", "stream": "your_stream", "record": {"nickname": "cheese"}},
    ]
    messages = jsonize_messages(messages)
    persist_messages(messages, "/tmp", "", "a.parquet", compression_method=None)

    assert mocker_to_parquet.call_count == 2

    first_call = mocker_to_parquet.call_args_list[0]
    assert first_call.args[0] == '/tmp/my_stream/a.parquet'

    second_call = mocker_to_parquet.call_args_list[1]
    assert second_call.args[0] == '/tmp/your_stream/a.parquet'
