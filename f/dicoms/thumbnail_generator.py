import os
import zipfile
import random
from typing import Optional, Tuple
import pydicom
from minio import Minio
import io
from PIL import Image
import certifi
import urllib3
import ssl
import psycopg2
import wmill
import json
import glob
import numpy

db_credentials = json.loads(wmill.get_variable("f/kobo/vultr_db"))
db_credentials = db_credentials["db_settings"]
s3_credentials = wmill.get_resource("f/dicoms/minio")

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
        s.compression_status = 'complete' and (s.image_url = '' or s.image_url is null)
    order by random() limit 10000;
""")

series_list = cur.fetchall()

result = []

def read_random_dicom_from_zip(zip_path: str) -> Optional[Tuple[pydicom.dataset.FileDataset, str, str]]:
    """Read a random DICOM file directly from the zip archive without extracting and return its SeriesInstanceUID."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Filter for DICOM files (assuming .dcm extension)
            dicom_files = [f for f in zip_ref.namelist() if f.lower().endswith('.dcm')]
            if not dicom_files:
                return None
            
            # Select random DICOM file
            random_dicom = random.choice(dicom_files)
            
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


def create_thumbnail(dicom_dataset: pydicom.dataset.FileDataset) -> Optional[bytes]:
    """Create a thumbnail from DICOM image."""
    try:
        # Convert DICOM to PIL Image
        image = Image.fromarray(dicom_dataset.pixel_array)
        
        if image.mode != 'RGB':
            image = image.convert('RGB')
        
        # Save to bytes
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='JPEG', quality=85)
        return img_byte_arr.getvalue()
    except Exception as e:
        print(f"Error creating thumbnail: {str(e)}")
        return None
    
def update_image_url(url, seriesuid ):
    """Update the validation status in the PostgreSQL database."""
    print("About to update", seriesuid, url)
    cur.execute("""
        UPDATE fieldsite.series
        SET image_url = %s, date_modified = CURRENT_TIMESTAMP
        WHERE seriesinstanceuid = %s
    """, (url, seriesuid))
    conn.commit()

def upload_to_objectstorage(thumbnail_data, dicom_filename, dicom_series_instanceuid):
    minio_client = Minio(
        endpoint=f"{s3_credentials['endPoint']}:{s3_credentials['port']}",
        access_key=s3_credentials['accessKey'],
        secret_key=s3_credentials['secretKey'],
        secure=s3_credentials['useSSL'],
        http_client=urllib3.PoolManager(
            cert_reqs='CERT_NONE',  # Don't verify SSL certificate
            ssl_version=ssl.PROTOCOL_TLS,
            maxsize=10,
            retries=urllib3.Retry(
                total=3,
                backoff_factor=0.2,
            )
        )
    )

    try:
        print(f"dicom_series_instanceuid: {dicom_series_instanceuid}")
        print(f"dicom_filename: {dicom_filename}")
        # Create object name
        object_name = f"scan_images/{dicom_series_instanceuid}/{dicom_filename}.jpg"

        # Upload to MinIO
        minio_client.put_object(
            bucket_name=s3_credentials['bucket'],
            object_name=object_name,
            data=io.BytesIO(thumbnail_data),
            length=len(thumbnail_data),
            content_type='image/jpeg'
        )
        
        # Generate URL
        url = f"s3://{s3_credentials['bucket']}/{object_name}"
        return url
    except Exception as e:
        print(f"Error uploading to MinIO: {str(e)}")
        return None


def main(
        dicoms_dir = '/dicoms/download_complete',
):
    index = 1
    total_loop = len(series_list)
    for series_info in series_list:
        
        wmill.set_progress(int(index / total_loop * 100))
        index +=1

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
                dicom_dataset, dicom_filename, dicom_series_instanceuid = read_random_dicom_from_zip(series_dir)
                if not dicom_dataset:
                    continue
                print("Generating thumbnail")
                thumbnail_data = create_thumbnail(dicom_dataset)
                if not thumbnail_data:
                    continue

                print("Uploading to object storage")
                thumbnail_url = upload_to_objectstorage(thumbnail_data, os.path.basename(dicom_filename), dicom_series_instanceuid)
                if not thumbnail_url:
                    continue
                print("Updating SQL database")
                update_image_url(thumbnail_url, fullseriesuid)
                resultant['status'] = 'complete'
                print("All done")
            except Exception as e:
                print(f"Error processing scan {series_info}: {str(e)}")
                continue
        result.append(resultant)
    
    print("Done")
    return result