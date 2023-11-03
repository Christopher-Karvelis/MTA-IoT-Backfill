from InitializeSignalHashTable.signal_client import SignalClient
from utils.azure_blob import get_backfilling_container_client
from utils.signal_hash_table import SignalHashTablePersistence


async def main(inputParameters: dict) -> str:
    # it is not ideal to block like this in an async function
    hash_table = SignalClient().provide_hash_table()

    container_client = get_backfilling_container_client()
    signal_hash_table_persistence = SignalHashTablePersistence(container_client)
    await signal_hash_table_persistence.upload_signal_hash_table(hash_table)

    return "Success"
