import os
import tempfile
import zipfile
import shutil
import re
import logging
from pathlib import Path
from pprint import PrettyPrinter
import wmill

pp = PrettyPrinter()
pp.indent = 4

def clean_name(name):
    """
    Clean a filename to contain only alphanumeric, dot, underscore, or hyphen characters.
    Replaces invalid characters with underscores.
    
    Args:
        name (str): Original filename
        
    Returns:
        str: Cleaned filename
    """
    # Replace any character that isn't alphanumeric, dot, underscore, or hyphen with underscore
    cleaned = re.sub(r'[^a-zA-Z0-9._-]', '', name)
    return cleaned

def rename_invalid_directories(base_path):
    """
    Recursively rename directories containing invalid characters.
    Returns a mapping of old to new paths for all renamed directories.
    
    Args:
        base_path (Path): Base directory path to start renaming from
        
    Returns:
        dict: Mapping of original paths to renamed paths
    """
    path_mapping = {}
    
    # Walk bottom-up to handle nested directories correctly
    for root, dirs, _ in os.walk(base_path, topdown=False):
        for dir_name in dirs:
            dir_path = Path(root) / dir_name
            if not is_valid_dirname(dir_name):
                new_name = clean_name(dir_name)
                new_path = dir_path.parent / new_name
                
                # Handle case where cleaned name is empty
                if not new_name:
                    new_name = "unnamed_directory"
                    new_path = dir_path.parent / new_name
                
                # Handle case where new path already exists
                counter = 1
                while new_path.exists():
                    new_path = dir_path.parent / f"{new_name}_{counter}"
                    counter += 1
                
                try:
                    dir_path.rename(new_path)
                    logging.info(f"Renamed directory: {dir_path} -> {new_path}")
                    path_mapping[str(dir_path)] = str(new_path)
                except Exception as e:
                    logging.error(f"Failed to rename directory {dir_path}: {e}")
    
    return path_mapping

def find_zip_files_with_non_alphanumeric_chars(directory_path):
    """
    Returns full paths to all .zip files in the given directory that contain any character 
    which is not alphanumeric, dot, underscore or hyphen.

    Args:
        directory_path (str): Path to the directory to search.

    Returns:
        list: List of .zip file paths containing non-alphanumeric characters.
    """
    pattern = re.compile(r'[^a-zA-Z0-9._-]')
    zip_files_with_non_alphanumeric_chars = []

    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.zip'):
                if pattern.search(file):
                    zip_files_with_non_alphanumeric_chars.append(os.path.join(root, file))

    return zip_files_with_non_alphanumeric_chars

def setup_logging():
    """Configure logging format and level"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def is_valid_dirname(dirname):
    """Check if directory name contains only allowed characters"""
    return bool(re.match(r'^[a-zA-Z0-9._-]+$', dirname))

def is_valid_filename(filename):
    """Check if filename contains only allowed characters"""
    return bool(re.match(r'^[a-zA-Z0-9._-]+$', filename))

def get_dry_run_filename(original_path):
    """Generate a new filename for dry run output"""
    path = Path(original_path)
    clean_stem = clean_name(path.stem)
    new_name = f"{clean_stem}_processed{path.suffix}"
    return path.parent / new_name

def get_cleaned_filename(original_path):
    """Generate a cleaned filename for the zip file"""
    path = Path(original_path)
    clean_stem = clean_name(path.stem)
    return path.parent / f"{clean_stem}{path.suffix}"

def process_zip_file(zip_path, dry_run=False):
    """
    Process zip file and handle directory names
    
    Returns:
        dict: Status information including path and processing result
    """
    status = {
        "path": str(zip_path),
        "status": "unknown",
        "message": ""
    }
    
    zip_path = Path(zip_path)
    if not zip_path.exists():
        status.update({
            "status": "error",
            "message": "Zip file not found"
        })
        logging.error(f"Zip file not found: {zip_path}")
        return status

    # Check if zip filename needs cleaning
    zip_filename_needs_cleaning = not is_valid_filename(zip_path.name)
    if zip_filename_needs_cleaning:
        logging.info(f"Zip filename contains invalid characters: {zip_path.name}")

    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        logging.info(f"Created temporary directory: {temp_dir}")

        # Extract zip file
        logging.info(f"Extracting {zip_path} to temporary directory")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir_path)
        except Exception as e:
            status.update({
                "status": "error",
                "message": f"Failed to extract zip file: {str(e)}"
            })
            logging.error(f"Failed to extract zip file: {e}")
            return status

        # Check for and rename invalid directories
        path_mapping = rename_invalid_directories(temp_dir_path)
        
        invalid_dirs_found = bool(path_mapping)
        if invalid_dirs_found:
            logging.info("Directory renames performed:")
            for old_path, new_path in path_mapping.items():
                logging.info(f"  {old_path} -> {new_path}")

        if not invalid_dirs_found and not zip_filename_needs_cleaning:
            status.update({
                "status": "success",
                "message": "No changes needed - all names are valid"
            })
            logging.info("No changes needed - all names are valid")
            return status

        # Determine the output path based on dry run status and filename validity
        if dry_run:
            output_path = get_dry_run_filename(zip_path)
        else:
            output_path = get_cleaned_filename(zip_path) if zip_filename_needs_cleaning else zip_path

        temp_zip_path = temp_dir_path / "temp.zip"
        logging.info(f"Creating new zip file{' (dry run)' if dry_run else ''}")
        try:
            with zipfile.ZipFile(temp_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(temp_dir_path):
                    rel_path = Path(root).relative_to(temp_dir_path)
                    for file in files:
                        if file == "temp.zip":
                            continue
                        file_path = Path(root) / file
                        arc_path = rel_path / file
                        logging.debug(f"Adding to zip: {arc_path}")
                        zipf.write(file_path, arc_path)
        except Exception as e:
            status.update({
                "status": "error",
                "message": f"Failed to create new zip file: {str(e)}"
            })
            logging.error(f"Failed to create new zip file: {e}")
            return status

        # Move the processed file to its final location
        try:
            shutil.move(temp_zip_path, output_path)
            logging.info(f"Successfully saved processed zip file to: {output_path}")
            
            # Delete the original zip file if this is not a dry run and the operation was successful
            if not dry_run and output_path != zip_path:
                try:
                    zip_path.unlink()
                    logging.info(f"Successfully deleted original zip file: {zip_path}")
                except Exception as e:
                    status.update({
                        "status": "partial_success",
                        "message": f"Processed successfully but failed to delete original: {str(e)}"
                    })
                    logging.error(f"Failed to delete original zip file: {e}")
                    return status
            
            status.update({
                "status": "success",
                "message": f"Successfully processed and saved to: {output_path}"
            })
                
        except Exception as e:
            status.update({
                "status": "error",
                "message": f"Failed to save processed zip file: {str(e)}"
            })
            logging.error(f"Failed to save processed zip file: {e}")
            return status

        return status

def main(files_path):
    
    print(f"Files path: {files_path}")
    offending_zip_files = find_zip_files_with_non_alphanumeric_chars(files_path)
    #pp.pprint(offending_zip_files)

    setup_logging()

    total_zip_files = len(offending_zip_files)
    index = 1
    
    results = []
    for zip_file in offending_zip_files:
        current_progress = index / total_zip_files * 100
        wmill.set_progress(int(current_progress))
        index +=1

        result = process_zip_file(zip_file, False)
        results.append(result)
    
    logging.info("Processing complete. Results:")
    
    # Check if any failures occurred
    if any(result["status"] == "error" for result in results):
        logging.error("Some files failed processing")
        exit(1)
    else:
        logging.info("All files processed successfully")
    
    return results