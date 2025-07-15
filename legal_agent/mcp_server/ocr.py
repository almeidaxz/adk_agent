import os
from pathlib import Path

from google.cloud import documentai
from google.cloud import storage
from google.api_core.client_options import ClientOptions

# project_id = os.getenv("PROJECT_ID")
project_id = "third-zephyr-464615-d6"
location = "us"
processor_id = "5be5eb7447f1e852"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str((Path(__file__).parent / "credentials" / "credentials.json").resolve())

gcs_client = storage.Client()

def process_documents(contract_name: str):
    opts = ClientOptions(api_endpoint="us-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    gcs_input_uri = f"gs://signed_contracts_adk/signed_contracts/{contract_name}"
    gcs_document = documentai.GcsDocument(
        gcs_uri=gcs_input_uri, mime_type='application/pdf'
    )

    name = client.processor_path(project_id, location, processor_id)

    request = documentai.ProcessRequest(
        name=name,
        gcs_document=gcs_document
    )

    result = client.process_document(request=request)
    document = result.document

    return document.text