# Azure function for Streaming Data to Timescale DB
To Backfill data while running from a desired blob storage while running locally your local.settings.json should look like this:

        {
        "IsEncrypted": false,
        "Values": {
            "FUNCTIONS_EXTENSION_VERSION": "~4",
            "AzureWebJobsStorage": "Connection string of blob to read the data from",
            "DOCKER_REGISTRY_SERVER_URL": "https://axh4nonprod4shared4cr.azurecr.io",
            "DOCKER_REGISTRY_SERVER_USERNAME": "axh4nonprod4shared4cr",
            "DOCKER_REGISTRY_SERVER_PASSWORD": "",
            "APPINSIGHTS_INSTRUMENTATIONKEY":"instrumentation key of asset api web app",
            "WEBSITES_ENABLE_APP_SERVICE_STORAGE": "false",
            "AzureWebJobsDashboard": "Connection string of blob to read the data from",
            "WEBSITE_CONTENTSHARE": "axh-dev-appl-iotback-func-12b4",
            "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING": "Connection string of blob to read the data from",
            "FUNCTIONS_WORKER_RUNTIME": "python",
            "ASSET_API_BASE": "https://axh-prod-appl-app-asset-api.azurewebsites.net",
            "API_CLIENT_ID": "azure-func-prod",
            "API_CLIENT_SECRET": "azure func client secret"
        },
        "ConnectionStrings": {}
        }


# Testing "localy" with github codespaces
Copy the path after opening the running azure function on another tab.

For postman requests you need the github token. Open a terminal in visual studio code in codespaces and type:

    echo $GITHUB_TOKEN 

copy the token and add it in your postman request as a Header with key= "X-GITHUB-TOKEN" and value="the copied token"

# Triggering the apis with postman
First you need to create the signal hashtable:

    POST: https://{host}/orchestrators/initialize_signal_hashtable
    body: 
        {"ts_start": "2023-11-05T00:00:00+00:00", "ts_end": "2023-11-06T00:00:00+00:00"}


Then we need to create the parquet files for the json files of interest 


    POST: https://{host}/orchestrators/json_to_parquet
    body: 
        {"ts_start": "2023-11-05T00:00:00+00:00", "ts_end": "2023-11-06T00:00:00+00:00"}

Finaly we need to trigger the backfill function:

    POST: https://{host}/orchestrators/backfill

    body:
    {
        "ts_start": "2023-11-05T00:00:00",
        "ts_end": "2023-11-07T00:00:00",
        "blobs_to_consider": ["2023-11-06/_from_2023-11-07/18", "2023-11-06/_from_2023-11-07/23"]
    }
    
"blobs_to_consider" contains the new blobs created that contain the parquet files created in the previous request

