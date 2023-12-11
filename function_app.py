import azure.functions as func
import azure.durable_functions as df
from signal_hashtable.initialize_signal_hashtable import bp
from parse_jsons.json_to_parquet import bp as bp2
from backfill.decompress_backfill import bp as bp3

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_functions(bp)
app.register_functions(bp2)  
app.register_functions(bp3)  