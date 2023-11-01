async def load_one_hour_of_data_starting_at(ts_start):
    parsed_input = inputParameters['ts_start'].split('T')
    date = parsed_input[0]
    hour = parsed_input[1][:2]
    # put axh-opcpublisher instead of backfill for real stuff
    pattern_to_read = f"{date}/{hour}"
    logging.info(f"will try to read {pattern_to_read=}")

    result = asyncio.new_event_loop().run_until_complete(get_data(storage_options, pattern_to_read))