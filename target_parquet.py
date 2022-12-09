#!/usr/bin/env python3
import argparse
import collections
import io
import http.client
import json
import os
import pandas as pd
import pkg_resources
import singer
import sys
import urllib
import threading

from uuid import uuid4


LOGGER = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(dictionary, parent_key="", sep="__"):
    """Function that flattens a nested structure, using the separater given as parameter, or uses '__' as default
    E.g:
     dictionary =  {
                        'key_1': 1,
                        'key_2': {
                               'key_3': 2,
                               'key_4': {
                                      'key_5': 3,
                                      'key_6' : ['10', '11']
                                 }
                        }
                       }
    By calling the function with the dictionary above as parameter, you will get the following strucure:
        {
             'key_1': 1,
             'key_2__key_3': 2,
             'key_2__key_4__key_5': 3,
             'key_2__key_4__key_6': "['10', '11']"
         }
    """
    items = []
    for k, v in dictionary.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def jsonschema_to_dataframe_schema(json_schema):
    properties = json_schema.get("properties")
    if properties is not None:
        pandas_target_schema = {}
        for field_name, dtype_dict in properties.items():
            if dtype_dict.get("type"):
                if dtype_dict["type"] == "integer":
                    pandas_target_schema[field_name] = "Int64"
                elif dtype_dict["type"] == "number":
                    pandas_target_schema[field_name] = "Float64"
                elif dtype_dict["type"] == "string":
                    pandas_target_schema[field_name] = "string"
                elif dtype_dict["type"] == "boolean":
                    pandas_target_schema[field_name] = "boolean"
                elif dtype_dict["type"] == "array":
                    pandas_target_schema[field_name] = "object"
                else:
                    raise Exception("Unknown dtype: {}".format(dtype_dict.get("type")))
            else:
                pandas_target_schema[field_name] = "object"
        return pandas_target_schema
    else:
        return {}


def save_data(
    records, schema, destination_path, stream_name, destination_partition_path, file_name, compression_method
):
    if len(records) == 0:
        LOGGER.info("There were not any records retrieved.")
    else:
        # Create a dataframe out of the record list and store it into a parquet file with the timestamp in the name.
        df = pd.DataFrame.from_records(records, coerce_float=True)

        df = df.astype(jsonschema_to_dataframe_schema(schema))

        if not file_name:
            file_name = str(uuid4()) + ".parquet"

        if destination_partition_path:
            filepath = os.path.join(destination_path, stream_name, destination_partition_path, file_name)
        else:
            filepath = os.path.join(destination_path, stream_name, file_name)

        if compression_method:
            # The target is prepared to accept all the compression methods provided by the pandas module, with the mapping below,
            # but, at the moment, pyarrow only allow gzip compression.
            extension_mapping = {"gzip": ".gz", "bz2": ".bz2", "zip": ".zip", "xz": ".xz"}
            df.to_parquet(
                filepath + extension_mapping[compression_method], engine="pyarrow", compression=compression_method
            )
        else:
            df.to_parquet(filepath, engine="pyarrow")


def persist_messages(messages, destination_path, destination_partition_path, file_name, compression_method):
    state = None
    schema = None

    current_stream_name = None
    records = []
    for message in messages:
        try:
            message = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            raise Exception("Unable to parse:\n{}".format(message))

        message_type = message["type"]
        if message_type == "STATE":
            LOGGER.debug("Setting state to {}".format(message["value"]))
            state = message["value"]

        elif message_type == "RECORD":
            stream_name = message["stream"]
            if current_stream_name is None or (current_stream_name is not None and current_stream_name != stream_name):
                raise Exception("You must send schema first before sending records")
            records.append(message["record"])

        elif message_type == "SCHEMA":
            stream_name = message["stream"]
            if current_stream_name != stream_name:
                save_data(
                    records,
                    schema,
                    destination_path,
                    current_stream_name,
                    destination_partition_path,
                    file_name,
                    compression_method,
                )
            current_stream_name = stream_name
            schema = message["schema"]
            records = []

        else:
            raise Exception(
                "Unknown message type {} in message {}. Message type should be one of {}".format(
                    message["type"], message, ", ".join(["RECORD", "STATE", "SCHEMA"])
                )
            )

    save_data(records, schema, destination_path, stream_name, destination_partition_path, file_name, compression_method)

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution("target-parquet").version
        conn = http.client.HTTPConnection("collector.singer.io", timeout=10)
        conn.connect()
        params = {
            "e": "se",
            "aid": "singer",
            "se_ca": "target-parquet",
            "se_ac": "open",
            "se_la": version,
        }
        conn.request("GET", "/i?" + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        LOGGER.debug("Collection request failed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")

    args = parser.parse_args()
    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}
    if not config.get("disable_collection", False):
        LOGGER.info(
            "Sending version information to singer.io. "
            + "To disable sending anonymous usage data, set "
            + 'the config parameter "disable_collection" to true'
        )
        threading.Thread(target=send_usage_stats).start()
    # The target expects that the tap generates UTF-8 encoded text.
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    state = persist_messages(
        input_messages,
        destination_path=config["destination_path"],
        destination_partition_path=config.get("destination_partition_path", ""),
        file_name=config.get("file_name", ""),
        compression_method=config.get("compression_method"),
    )

    emit_state(state)
    LOGGER.debug("Exiting normally")


if __name__ == "__main__":
    main()
