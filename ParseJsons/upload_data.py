from io import BytesIO

import pandas as pd


def group_by_day_with_names(df, read_from):
    grouped_by_days = df.groupby(pd.Grouper(key="ts", freq="1d"))
    non_empty_groups_with_names = [(f"{group_name.strftime('%Y-%m-%d')}/_from_{read_from}", group) for group_name, group
                                   in grouped_by_days if len(group) > 0]
    return non_empty_groups_with_names


async def upload_grouped_as_parquet(container_client, df, read_from):
    grouped_by_day_with_names = group_by_day_with_names(df, read_from)
    for group_name, group in grouped_by_day_with_names:
        await upload_parquet(container_client, group, group_name)
    return [group_with_name[0] for group_with_name in grouped_by_day_with_names]


async def upload_parquet(container_client, group, parquet_blob_name):
    parquet_file = BytesIO()
    group.to_parquet(parquet_file)
    parquet_file.seek(0)
    # I need to put some async with's
    await container_client.upload_blob(
        data=parquet_file, name=parquet_blob_name, overwrite=True
    )
