# drive_uploader.py

import os
import shutil
import tarfile
import zipfile
import logging
from pathlib import Path
from typing import Literal, Optional, List

# Make sure to install the required libraries:
# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# --- Configuration ---
# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# The scopes required for the Google Drive API.
# If you modify these scopes, delete the file token.json.
SCOPES: List[str] = ['https://www.googleapis.com/auth/drive.file']

# --- Core Functions ---

def compress_directory(
    source_dir: str,
    output_path: str,
    compress_format: Literal['zip', 'tar.gz']
) -> Optional[str]:
    """
    Compresses a directory into a .zip or .tar.gz file.

    Args:
        source_dir (str): The path to the directory to compress.
        output_path (str): The base path for the output archive (without extension).
        compress_format (Literal['zip', 'tar.gz']): The compression format.

    Returns:
        Optional[str]: The full path to the created archive, or None if an error occurred.
    """
    source_path = Path(source_dir)
    if not source_path.is_dir():
        logging.error(f"Source directory not found: {source_dir}")
        return None

    archive_path = f"{output_path}.{compress_format}"
    logging.info(f"Starting compression of '{source_dir}' to '{archive_path}'...")

    try:
        if compress_format == 'zip':
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(source_dir):
                    for file in files:
                        file_path = Path(root) / file
                        arcname = file_path.relative_to(source_path)
                        zipf.write(file_path, arcname)
        elif compress_format == 'tar.gz':
            with tarfile.open(archive_path, 'w:gz') as tar:
                tar.add(source_dir, arcname=source_path.name)
        else:
            logging.error(f"Unsupported compression format: {compress_format}")
            return None

        logging.info(f"Successfully compressed directory to {archive_path}")
        return archive_path
    except Exception as e:
        logging.error(f"Failed to compress directory: {e}")
        return None

def get_gdrive_service(credentials_path: str = 'credentials.json') -> Optional[object]:
    """
    Authenticates with the Google Drive API and returns a service object.

    Handles the OAuth2 flow by creating a token.json file.

    Args:
        credentials_path (str): Path to the Google Cloud credentials.json file.

    Returns:
        Optional[object]: An authenticated Google Drive API service object, or None.
    """
    creds = None
    token_path = Path('token.json')
    credentials_path = Path(credentials_path)

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logging.error(f"Could not refresh token: {e}. Please re-authenticate.")
                token_path.unlink() # Delete bad token
                return get_gdrive_service(str(credentials_path)) # Retry
        else:
            if not credentials_path.exists():
                logging.error(f"Credentials file not found at '{credentials_path}'.")
                logging.error("Please download it from the Google Cloud Console and place it here.")
                return None
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('drive', 'v3', credentials=creds)
        logging.info("Google Drive service created successfully.")
        return service
    except Exception as e:
        logging.error(f"Failed to build Google Drive service: {e}")
        return None


def upload_to_drive(
    service: object,
    local_path: str,
    folder_id: Optional[str] = None
) -> Optional[str]:
    """
    Uploads a local file to a specified Google Drive folder.

    Args:
        service (object): The authenticated Google Drive API service object.
        local_path (str): The path of the local file to upload.
        folder_id (Optional[str]): The ID of the destination folder in Google Drive.
                                   If None, uploads to the root "My Drive".

    Returns:
        Optional[str]: The file ID of the uploaded file on Google Drive, or None.
    """
    file_path = Path(local_path)
    if not file_path.exists():
        logging.error(f"Local file not found for upload: {local_path}")
        return None

    logging.info(f"Uploading '{file_path.name}' to Google Drive...")
    file_metadata = {'name': file_path.name}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    media = MediaFileUpload(str(file_path), resumable=True)

    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        logging.info(f"File '{file_path.name}' uploaded successfully. File ID: {file.get('id')}")
        return file.get('id')
    except HttpError as error:
        logging.error(f'An error occurred during upload: {error}')
        return None

def compress_and_upload(
    source_dir: str,
    compress_format: Literal['zip', 'tar.gz'],
    credentials_path: str = 'credentials.json',
    drive_folder_id: Optional[str] = None,
    cleanup: bool = True
) -> None:
    """
    Orchestrates the compression and upload process.

    Args:
        source_dir (str): Directory to compress and upload.
        compress_format (Literal['zip', 'tar.gz']): Compression format.
        credentials_path (str): Path to Google Cloud credentials.json.
        drive_folder_id (Optional[str]): ID of the target Google Drive folder.
        cleanup (bool): If True, deletes the local archive after successful upload.
    """
    output_filename = Path(source_dir).name
    archive_path = compress_directory(source_dir, output_filename, compress_format)

    if not archive_path:
        logging.error("Aborting upload due to compression failure.")
        return

    gdrive_service = get_gdrive_service(credentials_path)
    if not gdrive_service:
        logging.error("Aborting upload due to Google Drive authentication failure.")
        return

    file_id = upload_to_drive(gdrive_service, archive_path, drive_folder_id)

    if file_id and cleanup:
        try:
            os.remove(archive_path)
            logging.info(f"Cleaned up local archive: {archive_path}")
        except OSError as e:
            logging.error(f"Error removing local archive file: {e}")


# --- Example Usage ---
if __name__ == '__main__':
    # --- PLEASE CONFIGURE THE VARIABLES BELOW ---

    # 1. The directory you want to back up.
    # For this example, let's create a dummy directory.
    if not os.path.exists("my_important_data"):
        os.makedirs("my_important_data/folder1")
        with open("my_important_data/file1.txt", "w") as f:
            f.write("This is some important data.")
        with open("my_important_data/folder1/file2.txt", "w") as f:
            f.write("This is some more data in a subfolder.")

    DIRECTORY_TO_BACKUP = "my_important_data"

    # 2. The ID of the Google Drive folder to upload to.
    # To get the Folder ID, open the folder in Google Drive in your browser.
    # The URL will be like: https://drive.google.com/drive/folders/THIS_IS_THE_ID
    # Set to None to upload to the root "My Drive".
    GDRIVE_FOLDER_ID = None  # <-- IMPORTANT: Change this to your folder ID or leave as None

    # 3. Path to your credentials file.
    CREDENTIALS_FILE = 'credentials.json'

    # --- Run the backup process ---
    print("--- Starting ZIP backup example ---")
    compress_and_upload(
        source_dir=DIRECTORY_TO_BACKUP,
        compress_format='zip',
        credentials_path=CREDENTIALS_FILE,
        drive_folder_id=GDRIVE_FOLDER_ID,
        cleanup=True
    )

    print("\n--- Starting TAR.GZ backup example ---")
    compress_and_upload(
        source_dir=DIRECTORY_TO_BACKUP,
        compress_format='tar.gz',
        credentials_path=CREDENTIALS_FILE,
        drive_folder_id=GDRIVE_FOLDER_ID,
        cleanup=True
    )

    # Clean up the dummy directory
    # shutil.rmtree(DIRECTORY_TO_BACKUP)
