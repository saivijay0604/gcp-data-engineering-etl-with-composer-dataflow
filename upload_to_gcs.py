# Import the Google Cloud Storage client library to interact with GCS
from google.cloud import storage
# Import os to set environment variables for authentication
import os


# -----------------------------
# Configuration
# -----------------------------

# Name of the GCS bucket where the file will be uploaded
BUCKET_NAME = "foodservice_bucket"  # Fix: removed leading space from " foodservice_bucket"

# Path to the local CSV file to be uploaded
LOCAL_FILE_PATH = "food_daily.csv"

# Folder path inside the GCS bucket where the file will be stored
GCS_FOLDER = "raw/transactions"

# Name the file will have inside the GCS bucket
DESTINATION_FILE_NAME = "food_daily.csv"

# Full GCS destination path combining folder and filename
# Result: "raw/transactions/food_daily.csv"
DESTINATION_BLOB_NAME = f"{GCS_FOLDER}/{DESTINATION_FILE_NAME}"

# Set the environment variable to point to your service account key JSON file
# This tells the GCS client how to authenticate with GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account-key.json"


def upload_file_to_gcs(bucket_name, local_file_path, destination_blob_name):  # Fix: was destination-blob_name (hyphen), must be underscore
    """
    Upload a local CSV file to Google Cloud Storage.
    """

    try:
        # Create a GCS client using the credentials from the environment variable
        storage_client = storage.Client()

        # Get a reference to the bucket by name
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object — represents the file at the destination path in GCS
        blob = bucket.blob(destination_blob_name)

        # Upload the local file to GCS at the blob path
        blob.upload_from_filename(local_file_path)

        # Print success message with file details
        print("Upload successful")
        print(f"Local file: {local_file_path}")
        print(f"GCS path: gs://{bucket_name}/{destination_blob_name}")

    except FileNotFoundError:
        # Triggered if the local file path does not exist
        print(f"Error: Local file not found: {local_file_path}")

    except Exception as error:
        # Catches any other unexpected errors like auth issues, bucket not found etc.
        print("Upload failed")
        print(f"Error details: {error}")


# Entry point: only runs when script is executed directly, not when imported
if __name__ == "__main__":
    # Call the upload function with the configured values
    upload_file_to_gcs(BUCKET_NAME, LOCAL_FILE_PATH, DESTINATION_BLOB_NAME)
