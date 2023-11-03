from io import BytesIO

import pandas as pd


async def upload_grouped_as_parquet(container_client, df, read_from):
    blobs_to_consider = []
    for group_name, group in df.groupby(pd.Grouper(key="ts", freq="1d")):
        parquet_blob_name = f"{group_name.strftime('%Y-%m-%d')}/_from_{read_from}"
        if len(group) > 0:
            await upload_parquet(container_client, group, parquet_blob_name)
            blobs_to_consider.append(parquet_blob_name)
    return blobs_to_consider


async def upload_parquet(container_client, group, parquet_blob_name):
    parquet_file = BytesIO()
    group.to_parquet(parquet_file)
    parquet_file.seek(0)
    # I need to put some async with's
    await container_client.upload_blob(
        data=parquet_file, name=parquet_blob_name, overwrite=True
    )
