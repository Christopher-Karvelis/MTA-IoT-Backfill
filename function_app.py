import azure.functions as func
import azure.durable_functions as df
from signal_hashtable.initialize_signal_hashtable import bp as signal_hashtable_bp
from parse_jsons.json_to_parquet import bp as parse_jsons_bp
from backfill.decompress_backfill import bp as decompress_backfill_bp

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_functions(signal_hashtable_bp)
app.register_functions(parse_jsons_bp)  
app.register_functions(decompress_backfill_bp)
