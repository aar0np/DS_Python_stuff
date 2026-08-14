from pathlib import Path
from docling.service_client import DoclingServiceClient
import os

SERVICE_URL = os.getenv("DOCLING_SERVICE_URL")
API_KEY = os.getenv("DOCLING_API_KEY")
with DoclingServiceClient(url=SERVICE_URL, api_key=API_KEY) as client:
    result = client.convert(
        source="https://arxiv.org/pdf/2408.09869"
    )
    
    # Export to Markdown
    markdown = result.document.export_to_markdown()
    print(markdown)
