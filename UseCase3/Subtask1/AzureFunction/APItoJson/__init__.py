import datetime
import requests
import json
import os
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
import azure.functions as func
from azure.eventgrid import EventGridEvent, EventGridPublisherClient
import string


def main(req: func.HttpRequest) -> func.HttpResponse:
    # Variables 

    # TODO: uncomment the row below and name of your Azure Key Vault to the variable
    keyvault_name=''

    secret_name_apikey = 'apikey'
    secret_name_storage_account_name = 'storageaccountname'
    secret_name_storage_account_key = 'storageaccountkey'
    secret_name_topic_key = 'topickey'

    container_name = 'data'
    default_directory = 'bronze\\tmdb\\'

    # TODO: uncomment the row below and assign your topic endpoint URL to the variable
    # topic_endpoint = ''

    # Get keys from Keyvault    
    retrieved_storage_account_key = return_value_from_keyvault(par_keyvault_name=keyvault_name,
                                                               par_secret_name=secret_name_storage_account_key)
    storage_account = return_value_from_keyvault(par_keyvault_name=keyvault_name,
                                                 par_secret_name=secret_name_storage_account_name)
    storage_account_url = f"https://{storage_account}.blob.core.windows.net"
    api_key = return_value_from_keyvault(par_keyvault_name=keyvault_name, par_secret_name=secret_name_apikey)
    topic_key = return_value_from_keyvault(par_keyvault_name=keyvault_name, par_secret_name=secret_name_topic_key)

    # Get trending items from TMDB API
    trending_url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={api_key}"
    trending_response = requests.get(trending_url)
    trending_data = trending_response.json()

    # Get movie genres from TMDB API    
    # TODO: uncomment the rows below and complete missing parts with your code
    # genres_url =
    # genres_response =
    # genres_data =

    # Save API response bodies to JSON files
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d")
    trending_filename = f"{default_directory}tmdb_trending_item_{timestamp}.json"
    genres_filename = f"{default_directory}tmdb_genres_{timestamp}.json"

    # Create Json data
    json_trending_filename = json.dumps(trending_data)
    json_genres_filename = json.dumps(genres_data)

    # Upload JSON files to data lake
    try:
        blob_service_client = BlobServiceClient(account_url=storage_account_url,
                                                credential=retrieved_storage_account_key)

        # upload JSON file with ternding movies list
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=trending_filename)
        blob_client.upload_blob(json_trending_filename, overwrite=True)

        # upload JSON file with genres list
        # TODO: uncomment the rows below and complete missing parts with your code
        # blob_client =
        # blob_client.upload_blob()
    except Exception as e:
        return func.HttpResponse(f"Error on file storage: {e}")

    # Send custom event to EventGrid Topic
    # Set the necessary Event Grid event properties
    subject = "tmdb"
    data = {
        "trending_filename": trending_filename,
        "genres_filename": genres_filename
    }
    event_id = 1000
    event_type = "copycompleted"
    event_time = datetime.datetime.utcnow().isoformat()
    data_version = 1

    event = EventGridEvent(
        id=event_id,
        subject=subject,
        data=data,
        event_type=event_type,
        data_version=data_version,
        event_time=event_time
    )

    topic_credential = AzureKeyCredential(topic_key)

    event_grid_client = EventGridPublisherClient(endpoint=topic_endpoint, credential=topic_credential)
    try:
        event_grid_client.send([event])
    except Exception as e:
        return func.HttpResponse(f"Error on sending the event: {e}")

    return func.HttpResponse("Files uploaded and event sent.")


def return_value_from_keyvault(par_keyvault_name: string, par_secret_name: string) -> string:
    from azure.keyvault.secrets import SecretClient
    from azure.identity import DefaultAzureCredential

    # Set up Key Vault client
    if __debug__:
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False,
                                            exclude_environment_credential=True,
                                            exclude_visual_studio_code_credential=True,
                                            exclude_shared_token_cache_credential=True,
                                            exclude_managed_identity_credential=False)
    else:
        credential = DefaultAzureCredential()
    keyvault_name = par_keyvault_name
    secret_name = par_secret_name
    kv_uri = f'https://{keyvault_name}.vault.azure.net'
    client = SecretClient(vault_url=kv_uri, credential=credential)

    # Retrieve secret from Key Vault
    try:
        retrieved_secret = client.get_secret(secret_name)
    except Exception as e:
        return func.HttpResponse(f"Error Accessing Keyvault: {e}")
    # Return secret value
    return retrieved_secret.value
