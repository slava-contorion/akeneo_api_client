import os
import sys
import tempfile
import logging
from google.cloud import storage

# Ensure we can import the sibling script by adding current dir to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the original script as a module
try:
    import export_assets_to_akeneo as exporter
except ImportError:
    try:
        from custom_scripts import export_assets_to_akeneo as exporter
    except ImportError:
        # Fallback if running in an environment where custom_scripts is not a package
        import export_assets_to_akeneo as exporter

def get_env_var(name):
    val = os.environ.get(name)
    if not val:
        raise ValueError(f"Missing environment variable: {name}")
    return val

def move_blob(bucket_name, blob_name, new_prefix):
    """Moves a blob to a new prefix (folder) within the same bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    
    # Construct new name: replace current folder with new_prefix
    # Assumes structure folder/filename.csv
    filename = os.path.basename(blob_name)
    new_name = f"{new_prefix.rstrip('/')}/{filename}"
    
    print(f"Moving {blob_name} to {new_name}")
    bucket.copy_blob(source_blob, bucket, new_name)
    source_blob.delete()

def process_single_file(bucket_name, file_name):
    """Core logic to process a single CSV file from GCS."""
    print(f"Processing file: {file_name} from {bucket_name}")

    # 1. Setup Akeneo Client
    try:
        base_url = get_env_var("AKENEO_BASE_URL")
        client_id = get_env_var("AKENEO_CLIENT_ID")
        secret = get_env_var("AKENEO_SECRET")
        username = get_env_var("AKENEO_USERNAME")
        password = get_env_var("AKENEO_PASSWORD")
        
        # Initialize client using the class from the original script
        akeneo = exporter.Client(base_url, client_id, secret, username, password)
        http = exporter.AkeneoHttp(akeneo, base_url)
        
    except Exception as e:
        print(f"Configuration error: {e}")
        # If we can't configure, we probably can't process anything. 
        return False

    # 2. Download CSV to temp file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    _, temp_local_filename = tempfile.mkstemp(suffix=".csv")
    try:
        print(f"Downloading {file_name} to {temp_local_filename}")
        blob.download_to_filename(temp_local_filename)
        
        # 3. Detect format & process
        handler = exporter.detect_csv_format(temp_local_filename)
        products = handler.load_csv(temp_local_filename)
        print(f"Loaded {len(products)} products from CSV")
        
        stats = {"total": len(products), "updated": [], "not_found": [], "ambiguous": [], "other": []}
        
        for entry in products:
            try:
                exporter.process_product(http, entry, stats, handler)
            except Exception as e:
                sku = entry.get('sku')
                print(f"Unhandled error processing SKU {sku}: {e}")
                stats["other"].append((sku, f"Unhandled: {e}"))
        
        exporter.print_summary(stats)
        
        # 4. Move to processed
        move_blob(bucket_name, file_name, "processed")
        print(f"Successfully processed {file_name}")
        return True

    except Exception as e:
        print(f"Fatal error processing file {file_name}: {e}")
        move_blob(bucket_name, file_name, "error")
        return False
    finally:
        if os.path.exists(temp_local_filename):
            os.remove(temp_local_filename)

def process_gcs_event(event, context):
    """Background Cloud Function triggered by Cloud Storage."""
    file_data = event
    bucket_name = file_data['bucket']
    file_name = file_data['name']
    
    print(f"Event ID: {context.event_id}")
    
    # Filter: only process files in 'queue/' folder
    if not file_name.startswith('queue/') or not file_name.endswith('.csv'):
        print(f"Skipping {file_name}: not in queue/ or not a CSV")
        return

    process_single_file(bucket_name, file_name)

def run_batch_job():
    """Cloud Run Job Entry Point.
    
    Scans the 'queue/' folder and processes a subset of files based on the task index.
    """
    bucket_name = get_env_var("BUCKET_NAME")
    
    # Cloud Run Jobs environment variables
    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
    task_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))
    
    print(f"Starting Worker {task_index}/{task_count} on bucket {bucket_name}")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # List all CSVs in queue
    blobs = list(bucket.list_blobs(prefix="queue/"))
    csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
    
    # Sort to ensure consistent assignment across tasks
    csv_blobs.sort(key=lambda x: x.name)
    
    print(f"Found {len(csv_blobs)} files in queue.")
    
    # Sharding: Pick files where index % task_count == task_index
    my_blobs = [b for i, b in enumerate(csv_blobs) if i % task_count == task_index]
    
    print(f"This worker will process {len(my_blobs)} files.")
    
    for blob in my_blobs:
        print(f"--- Starting {blob.name} ---")
        process_single_file(bucket_name, blob.name)
        print(f"--- Finished {blob.name} ---")

if __name__ == "__main__":
    # If run directly, assume Cloud Run Job mode
    # This allows testing locally or running in Cloud Run
    if os.environ.get("CLOUD_RUN_TASK_INDEX"):
        run_batch_job()
    else:
        print("Not running in Cloud Run Job mode (CLOUD_RUN_TASK_INDEX not set).")
        print("Usage: Deploy as Cloud Function (entry point: process_gcs_event) OR Cloud Run Job.")
