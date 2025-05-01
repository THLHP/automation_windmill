import os
import shutil
import time
import zipfile
from datetime import datetime, timedelta

def log(level, message):
    """Simple logger function to print messages to stdout with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} [{level}] {message}")

def clean_target_directory(target_dir, dry_run):
    """Removes and recreates the target directory."""
    log("INFO", f"Preparing target directory: {target_dir}")
    if os.path.exists(target_dir):
        if os.path.isdir(target_dir):
            log("INFO", f"Target directory '{target_dir}' exists. Cleaning...")
            if dry_run:
                log("DRYRUN", f"Would remove directory: {target_dir}")
            else:
                try:
                    shutil.rmtree(target_dir)
                    log("INFO", f"Successfully removed directory: {target_dir}")
                except OSError as e:
                    log("ERROR", f"Failed to remove directory '{target_dir}': {e}")
                    return False # Indicate failure
        else:
            log("ERROR", f"Target path '{target_dir}' exists but is not a directory. Aborting.")
            return False # Indicate failure
    else:
        log("INFO", f"Target directory '{target_dir}' does not exist. Will create it.")

    # Recreate the directory (only if not in dry run)
    if not dry_run:
        try:
            os.makedirs(target_dir, exist_ok=True) # exist_ok=True handles race condition if created between check and make
            log("INFO", f"Successfully created directory: {target_dir}")
        except OSError as e:
            log("ERROR", f"Failed to create directory '{target_dir}': {e}")
            return False # Indicate failure

    return True # Indicate success

def find_recent_zip_files(source_dir, hours):
    """Recursively finds zip files modified within the specified number of hours."""
    log("INFO", f"Searching for zip files in '{source_dir}' modified in the last {hours} hours...")
    recent_files = []
    now = time.time()
    time_threshold = now - (hours * 60 * 60) # Calculate the cutoff time

    if not os.path.isdir(source_dir):
        log("ERROR", f"Source directory '{source_dir}' not found or is not a directory.")
        return recent_files # Return empty list

    try:
        for root, _, files in os.walk(source_dir):
            for filename in files:
                if filename.lower().endswith(".zip"):
                    file_path = os.path.join(root, filename)
                    try:
                        # Use modification time (mtime) as it often reflects when the file was finalized
                        file_mtime = os.path.getmtime(file_path)
                        if file_mtime >= time_threshold:
                            log("DEBUG", f"Found recent zip file: {file_path} (Modified: {datetime.fromtimestamp(file_mtime)})")
                            recent_files.append(file_path)
                        else:
                            log("DEBUG", f"Skipping older zip file: {file_path} (Modified: {datetime.fromtimestamp(file_mtime)})")
                    except FileNotFoundError:
                         # Handle rare case where file disappears between os.walk and getmtime
                         log("WARN", f"File disappeared before its modification time could be checked: {file_path}")
                    except OSError as e:
                         log("WARN", f"Could not get modification time for '{file_path}': {e}")

    except Exception as e:
        log("ERROR", f"An error occurred during directory traversal of '{source_dir}': {e}")

    log("INFO", f"Found {len(recent_files)} recent zip files.")
    return recent_files

def extract_zip_file(zip_path, target_dir, dry_run):
    """Extracts a single zip file to the target directory."""
    log("INFO", f"Processing file: {zip_path}")
    if not zipfile.is_zipfile(zip_path):
        log("WARN", f"Skipping non-zip or corrupted file: {zip_path}")
        return

    if dry_run:
        log("DRYRUN", f"Would extract '{zip_path}' to '{target_dir}'")
        # Optionally list contents in dry run:
        # try:
        #     with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        #         log("DRYRUN", f"  Contents:")
        #         for member in zip_ref.namelist():
        #              log("DRYRUN", f"    - {member}")
        # except Exception as e:
        #      log("DRYRUN", f"  Could not list contents due to error: {e}")
    else:
        log("INFO", f"Extracting '{zip_path}' to '{target_dir}'...")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(target_dir)
            log("INFO", f"Successfully extracted: {zip_path}")
        except zipfile.BadZipFile:
            log("ERROR", f"Failed to extract '{zip_path}'. File is corrupted or not a valid zip.")
        except PermissionError:
            log("ERROR", f"Permission denied when trying to extract '{zip_path}' to '{target_dir}'.")
        except FileNotFoundError:
             log("ERROR", f"File not found during extraction attempt (should not happen if find worked): {zip_path}")
        except Exception as e:
            log("ERROR", f"An unexpected error occurred while extracting '{zip_path}': {e}")

def main(source_dir = '/dicoms/download_complete', target_dir = '/dicoms/download_complete_recent', dry_run = True, time_window = 48 ):

    # 1. Validate Source Directory
    if not os.path.isdir(source_dir):
        log("ERROR", f"Source directory '{source_dir}' does not exist or is not a directory. Aborting.")
        return

    # 2. Clean Target Directory
    if not clean_target_directory(target_dir, dry_run):
        log("ERROR", "Failed to prepare target directory. Aborting.")
        return # Stop if cleaning/creation failed

    # 3. Find recent zip files
    zip_files_to_extract = find_recent_zip_files(source_dir, HOURS_TO_CHECK)

    # 4. Extract found files
    if not zip_files_to_extract:
        log("INFO", "No recent zip files found to extract.")
    else:
        log("INFO", f"Starting extraction of {len(zip_files_to_extract)} files...")
        for zip_file in zip_files_to_extract:
            extract_zip_file(zip_file, target_dir, dry_run)
        log("INFO", "Extraction process complete.")

    log("INFO", "--- Zip Extraction Script Finished ---")


if __name__ == "__main__":
    # Call the main function with configured parameters
    # You can change the values here directly if needed,
    # or override them when calling the script from elsewhere.
    main(
        source_dir=DEFAULT_SOURCE_DIR,
        target_dir=DEFAULT_TARGET_DIR,
        dry_run=DEFAULT_DRY_RUN
    )