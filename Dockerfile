FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY export_assets_to_akeneo/requirements-gcp.txt .
RUN pip install --no-cache-dir -r requirements-gcp.txt

# Copy codebase
COPY . .

# Set entrypoint to the worker script
CMD ["python", "export_assets_to_akeneo/gcp_worker.py"]