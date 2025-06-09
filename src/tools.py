import os
import stat
import datetime
import logging

from pathlib import Path

from PIL import Image
from PIL.ExifTags import TAGS

from tinytag import TinyTag

logger = logging.getLogger(__name__)

class ToolException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)

def calculate_file_hash(filepath: Path, hash_algorithm: str = "sha256") -> str:
    """Calculates the hash of a file."""
    h = hashlib.new(hash_algorithm)
    try:
        with open(filepath, "rb") as f:
            while chunk := f.read(8192): # Read in 8KB chunks
                h.update(chunk)
        return h.hexdigest()
    except IOError as e:
        logging.error(f"Could not read file {filepath} for hashing: {e}")
        return "" # Return empty string or raise exception


def get_file_metadata(file_path: Path) -> dict[str, Any]:
    """
    Retrieves metadata for a given file_path.

    Args:
        file_path (Path): absolute pathlib.Path of a file

    Returns:
        dict: A dictionary containing file metadata, or None if an error occurs.
    """
    metadata = {}
    try:
        # Get file stats
        stat_info = os.stat(file_path)

        metadata['file_path'] = file_path
        metadata['size_bytes'] = stat_info.st_size
        metadata['permissions'] = stat.filemode(stat_info.st_mode) # Human-readable permissions
        metadata['owner_uid'] = stat_info.st_uid
        metadata['group_gid'] = stat_info.st_gid
        metadata['last_access_time'] = datetime.datetime.fromtimestamp(stat_info.st_atime).strftime('%Y-%m-%d %H:%M:%S')
        metadata['last_modification_time'] = datetime.datetime.fromtimestamp(stat_info.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        metadata['last_metadata_change_time'] = datetime.datetime.fromtimestamp(stat_info.st_ctime).strftime('%Y-%m-%d %H:%M:%S')
        metadata['inode_number'] = stat_info.st_ino
        metadata['device_id'] = stat_info.st_dev
        metadata['num_hard_links'] = stat_info.st_nlink

        # You can also get simplified time information using os.path
        metadata['alt_last_modification_time'] = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
        metadata['alt_creation_time_or_metadata_change'] = datetime.datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d %H:%M:%S') # Note: on Unix, ctime is metadata change time
        metadata['alt_size_bytes'] = os.path.getsize(file_path)

        return metadata

    except FileNotFoundError as e:
        message = f"tools.get_file_metadata: {e.__class__.__name__} - {file_path}")
        raise ToolException(message)
    except OSError as e:
        message = f"tools.get_file_metadata: {e.__class__.__name__} - {file_path}")
        raise ToolException(message)


def get_image_metadata(file_path: Path) -> dict[str, Any]:
    metadata = {}
    try:
        img = Image.open(file_path)
        exif_data = img._getexif() # Returns a dictionary of EXIF tags
        if exif_data:
            for tag_id, value in exif_data.items():
                tag_name = TAGS.get(tag_id, tag_id)
                metadata[tag_name] = value
        return metadata
    except Exception as e:
        message = f"tools.get_image_metadata: {e.__class__.__name__} - {file_path}")
        raise ToolException(message)

# exif_info = get_image_exif("your_image.jpg")
# if exif_info:
#     for tag, value in exif_info.items():
#         print(f"{tag}: {value}")

def get_audio_metadata(file_path: Path) -> dict[str, Any]:
    try:
        tag = TinyTag.get(file_path)
        return {
            "title": tag.title,
            "artist": tag.artist,
            "album": tag.album,
            "year": tag.year,
            "duration_seconds": tag.duration,
            "bitrate_kps": tag.bitrate,
            # Add more as needed
        }
    except Exception as e:
        message = f"tools.get_audio_metadata: {e.__class__.__name__} - {file_path}")
        raise ToolException(message)

# https://pypi.org/project/tinytag/
# audio_info = get_audio_metadata("your_song.mp3")
# if audio_info:
#     for key, value in audio_info.items():
#         print(f"{key.capitalize()}: {value}")

