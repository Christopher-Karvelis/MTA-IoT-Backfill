import os
import json

from InitializeSignalHashTable.signal_client import SignalClient


def main(inputParameters: dict) -> str:
    target_connection_string = os.getenv("AzureWebJobsStorage")
    target_storage_options = {"connection_string": target_connection_string}

    signal_client = SignalClient()
    hash_table = signal_client.provide_hash_table()
    with open("signal_dictionary.json", "w") as signal_file:
        json.dump(hash_table, signal_file)

    return "Success"

