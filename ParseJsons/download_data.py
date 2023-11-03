import asyncio

from utils.azure_blob import download_string_blob, get_source_container_client


async def load_one_hour_of_data_starting_at(date, hour):
    source_container_client = get_source_container_client()

    pattern_to_read = f"{date}/{hour}"
    blobs_with_start_to_consider = source_container_client.list_blobs(
        name_starts_with=pattern_to_read
    )
    tasks = [
        _download_blob_with_name(blob["name"], source_container_client)
        async for blob in blobs_with_start_to_consider
        if blob["name"].endswith("json")
    ]
    result = await asyncio.gather(*tasks)

    return result, pattern_to_read


async def _download_blob_with_name(blob_name, container_client):
    data = await download_string_blob(blob_name, container_client)
    return blob_name, data
