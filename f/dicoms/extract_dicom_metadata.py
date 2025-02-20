import os
import zipfile
import json
from typing import Optional, Tuple
import pydicom
import io
import psycopg2
import wmill
import glob

db_credentials = json.loads(wmill.get_variable("f/kobo/vultr_db"))
db_credentials = db_credentials["db_settings"]

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=db_credentials['dbname'],
    user=db_credentials['username'],
    password=db_credentials['password'],
    host=db_credentials['host'],
    port=db_credentials['port']
)
cur = conn.cursor()

def read_random_dicom_from_zip(zip_path: str) -> Optional[Tuple[pydicom.dataset.FileDataset, str, str]]:
    """Read a random DICOM file directly from the zip archive without extracting and return its SeriesInstanceUID."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Filter for DICOM files (assuming .dcm extension)
            dicom_files = [f for f in zip_ref.namelist() if f.lower().endswith('.dcm')]
            if not dicom_files:
                return None

            # Select random DICOM file
            random_dicom = dicom_files[0]

            # Read the DICOM file directly from ZIP
            with zip_ref.open(random_dicom) as dicom_file:
                # Read bytes into memory
                dicom_bytes = io.BytesIO(dicom_file.read())
                # Parse DICOM from bytes
                dicom_dataset = pydicom.dcmread(dicom_bytes)

                # Check if SeriesInstanceUID is present in the DICOM dataset
                series_instance_uid = getattr(dicom_dataset, 'SeriesInstanceUID', None)
                return dicom_dataset, random_dicom, series_instance_uid
    except Exception as e:
        print(f"Error reading DICOM from zip {zip_path}: {str(e)}")
        return None

def dicom_to_json(ds):
    """
    Convert a DICOM dataset to a JSON-compatible dictionary using official DICOM keywords as keys.
    For private or unknown tags, use the numerical representation.

    Args:
        ds: A pydicom dataset object (returned from dcmread)

    Returns:
        dict: A JSON-compatible dictionary containing the DICOM data
    """
    def _convert_value(value):
        """Helper function to convert DICOM values to JSON-serializable format"""
        if hasattr(value, 'value'):  # For sequence elements
            return _convert_value(value.value)
        elif hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
            return [_convert_value(v) for v in value]
        elif isinstance(value, bytes):
            return value.hex()
        else:
            return str(value)

    def _process_dataset(dataset):
        """Recursively process DICOM dataset"""
        result = {}
        for elem in dataset:
            # Get the official keyword if it exists, otherwise use numerical tag
            if pydicom.datadict.keyword_for_tag(elem.tag):
                key = pydicom.datadict.keyword_for_tag(elem.tag)
            else:
                key = f"{elem.tag.group:04X}_{elem.tag.element:04X}"

            # Exclude Pixel Data
            if elem.tag == (0x7FE0, 0x0010):
                continue

            # Handle sequences (which can contain nested datasets)
            if elem.VR == "SQ":
                result[key] = [_process_dataset(item) for item in elem]
            else:
                result[key] = _convert_value(elem.value)

        return result

    json_dict = _process_dataset(ds)
    return json_dict

def update_file_metadata(seriesuid, metadata):
    """Update the file metadata status in the PostgreSQL database."""
    print("About to update", seriesuid)
    cur.execute("""
        UPDATE fieldsite.series
        SET file_metadata = %s, date_modified = CURRENT_TIMESTAMP
        WHERE seriesinstanceuid = %s
    """, (json.dumps(metadata), seriesuid))
    conn.commit()

def main(
        dicoms_dir: str = '/dicoms/download_complete',
        limit: int = 1,
):
    cur.execute("""
        select
            st.patient_id,
            s.seriesinstanceuid,
            (
                select string_agg(value, '.' order by idx)
                from unnest(string_to_array(s.seriesinstanceuid, '.')) with ordinality as t(value, idx)
                where idx > 6
            ) as seriesuid
        from
            fieldsite.series s
        join fieldsite.studies st on
            s.studyid = st.studyid
        where
            s.file_metadata is null
        order by random() limit %s;
    """, (limit,))

    series_list = cur.fetchall()

    index = 1
    total_loop = len(series_list)
    result = []
    for series_info in series_list:
        wmill.set_progress(int(index / total_loop * 100))
        index += 1

        patient_id, fullseriesuid, seriesuid = series_info

        resultant = {'seriesuid': seriesuid, 'status': 'failed'}
        print(f"Working on: {series_info}")
        # Use glob to find directories that end with the series name
        pattern = os.path.join(dicoms_dir, patient_id, f'*___{seriesuid}.zip')
        matching_dirs = glob.glob(pattern)
        print(f"Matching dirs: {matching_dirs}")

        if matching_dirs:
            try:
                series_dir = matching_dirs[0]
                if not series_dir:
                    continue
                print("Getting dicom data from zip file")
                dicom_dataset, _, _ = read_random_dicom_from_zip(series_dir)
                if not dicom_dataset:
                    continue
                print("Extracting metadata")
                metadata = dicom_to_json(dicom_dataset)
                print("Updating SQL database")
                update_file_metadata(fullseriesuid, metadata)
                resultant['status'] = 'complete'
                print("All done")
            except Exception as e:
                print(f"Error processing scan {series_info}: {str(e)}")
                continue
        result.append(resultant)

    print("Done")
    return result
