import os
import zipfile
import random
from typing import Optional, Tuple
import pydicom
import io
import psycopg2
import wmill
import json
import glob
import logging
import tempfile
import shutil

db_credentials = json.loads(wmill.get_variable("f/kobo/vultr_db"))
db_credentials = db_credentials["db_settings"]

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=db_credentials['dbname'],
    user=db_credentials['username'],
    password=db_credentials['password'],
    host=db_credentials['host'],
    port=db_credentials['port']
)
cur = conn.cursor()

# Query all series with download status 'complete' grouped by seriesdescription
cur.execute("""
select
	s2.patient_id,
	s.seriesinstanceuid,
	(
	select
		string_agg(value,
		'.'
	order by
		idx)
	from
		unnest(string_to_array(s.seriesinstanceuid,
		'.')) with ordinality as t(value,
		idx)
	where
		idx > 6
        ) as seriesuid,
	pc.corrected_patient_id,
	pc.correct_patient_sex,
	pc.corrected_patient_name
from
	fieldsite.series s
left join fieldsite.studies s2 on
	s.studyinstanceuid = s2.studyinstanceuid
inner join fieldsite.patientid_corrections pc on
	s2.patient_id = pc.original_patient
where
	s.keep_status = 'keep'
	and (s.extract_status = ''
		or s.extract_status is null) order by s.series_datetime desc
    limit 500;
""")

def update_extract_status(patient_id, seriesuid, status):
    """Update the validation status in the PostgreSQL database."""
    cur.execute("""
        UPDATE fieldsite.series
        SET extract_status = %s, date_modified = CURRENT_TIMESTAMP
        WHERE seriesinstanceuid = %s AND studyid IN (
            SELECT studyid FROM fieldsite.studies
            WHERE patient_id = %s
        )
    """, (status, seriesuid, patient_id))
    conn.commit()

series_list = cur.fetchall()

result = []

def update_dicom_tags(dicom_path, patient_id, corrected_patient_id, corrected_patient_sex, corrected_patient_name):
    """
    Updates the 'patient_id', 'patient_sex' and 'patient_name' fields in a DICOM file.
    Args:
    dicom_path (str): Path to the DICOM file.
    corrected_patient_id (str): Corrected patient ID.
    corrected_patient_sex (str): Corrected patient sex.
    corrected_patient_name (str): Corrected patient name.
    Returns:
    bool: Whether the update was successful.
    """
    try:
        # Read the DICOM file with write mode
        ds = pydicom.dcmread(dicom_path, force=True)

        effective_patient_id = corrected_patient_id if corrected_patient_id is not None and corrected_patient_id.strip() else patient_id

        logger.info(f'Updating: {effective_patient_id}, {corrected_patient_name}, {corrected_patient_sex}')
        
        # Update tags if corrections are not empty strings or None
        if corrected_patient_id is not None and corrected_patient_id.strip():
            ds.PatientID = corrected_patient_id
        
        if corrected_patient_sex is not None and corrected_patient_sex.strip():
            ds.PatientSex = corrected_patient_sex
        
        if corrected_patient_name is not None and corrected_patient_name.strip():
            ds.PatientName = corrected_patient_name
        
        logger.info(f'Saving at {dicom_path}')
        # Write the updated DICOM file
        ds.save_as(dicom_path)
        
        return True
    except Exception as e:
        logger.error(f"Error updating DICOM tags: {str(e)}")
        return False

def main(
        dicoms_dir = '/dicoms/download_complete',
        validation_dir = '/dicoms/validated_scans'
):
    index = 1
    total_loop = len(series_list)
    for series_info in series_list:
        
        wmill.set_progress(int(index / total_loop * 100))
        index +=1

        patient_id, fullseriesuid, seriesuid, corrected_patient_id, corrected_patient_sex, corrected_patient_name = series_info

        resultant = {'seriesuid': seriesuid, 'status': 'failed'}
        logger.info(f"Working on: {series_info}")
        
        # Use glob to find directories that end with the series name
        pattern = os.path.join(dicoms_dir, patient_id, f'*___{seriesuid}.zip')
        matching_dirs = glob.glob(pattern)
        logger.info(f"Matching dirs: {matching_dirs}")

        if matching_dirs:
            try:
                series_dir = matching_dirs[0]
                if not series_dir:
                    continue
                logger.info("Extracting zip file")
                
                # Create a temporary directory to extract the zip file
                temp_dir = tempfile.mkdtemp()
                
                # Extract the zip file to the temporary directory
                shutil.unpack_archive(series_dir, temp_dir)

                logger.info(f'Temp dir location {temp_dir}')
                
                # Get the list of DICOM files in the extracted directory
                dicom_files = glob.glob(os.path.join(temp_dir, '**', '*.dcm'), recursive=True)
    
                # Update the DICOM tags for each file
                update_success = True
                for dicom_file in dicom_files:
                    logger.info(f'Updating file {dicom_file}')
                    if not update_dicom_tags(dicom_file, patient_id, corrected_patient_id, corrected_patient_sex, corrected_patient_name):
                        update_success = False
                        break

                # Rename directories with patient_id to corrected_patient_id
                if corrected_patient_id is not None:
                    for root, dirs, files in os.walk(temp_dir):
                        for dir in dirs:
                            if dir == patient_id:
                                new_path = os.path.join(root, corrected_patient_id)
                                old_path = os.path.join(root, dir)
                                try:
                                    logger.info(f'Renaming directory {old_path} to {new_path}')
                                    os.rename(old_path, new_path)
                                except Exception as e:
                                    logger.error(f'Failed to rename directory: {str(e)}')
                else:
                    logger.info(f"Keeping original directory names for patient {patient_id} - corrected ID is None")
                
                if update_success:
                    logger.info("Moving extracted files to validated scans directory")
                    
                        # Move all DICOM files from temp_dir to validation_dir, preserving subdirectories
                    for root, dirs, files in os.walk(temp_dir):
                        rel_path = os.path.relpath(root, start=temp_dir)
                        
                        if rel_path != '.':
                            dest_dir = os.path.join(validation_dir, rel_path)
                            os.makedirs(dest_dir, exist_ok=True)  # Create destination directory if it doesn't exist
                        
                        for file in files:
                            src_file = os.path.join(root, file)
                            if src_file.endswith('.dcm'):
                                dest_file = os.path.join(validation_dir, rel_path, file) if rel_path != '.' else os.path.join(validation_dir, file)
                                shutil.move(src_file, dest_file)

                    # Remove the temporary directory (now empty)
                    shutil.rmtree(temp_dir)
                    resultant['status'] = 'complete'
                    update_extract_status(patient_id, fullseriesuid, 'complete')

                else:
                    logger.error("Failed to update DICOM tags. Removing temporary directory.")
                    shutil.rmtree(temp_dir)
                    update_extract_status(patient_id, fullseriesuid, 'failed')
            except Exception as e:
                logger.error(f"Error processing scan {series_info}: {str(e)}")
                update_extract_status(patient_id, fullseriesuid, 'failed')
                continue
        result.append(resultant)
    
    print("Done")
    return result