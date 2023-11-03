import json

from utils.azure_blob import download_string_blob

BLOB_NAME = "signal_hash_table"


class SignalHashTablePersistence:
    def __init__(self, container_client):
        self.container_client = container_client

    async def upload_signal_hash_table(self, hash_table):
        blob_client = self.container_client.get_blob_client(blob=BLOB_NAME)

        data = json.dumps(hash_table)

        await blob_client.upload_blob(data, overwrite=True)

    async def download_signal_hash_table(self):
        signal_hash_table = json.load(
            await download_string_blob(BLOB_NAME, self.container_client)
        )
        return signal_hash_table
