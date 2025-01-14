import json
import psycopg2
from psycopg2 import sql, extras
from tqdm import tqdm
from pynetdicom import AE, debug_logger
from pydicom.dataset import Dataset
from datetime import datetime
import os
import wmill
from wmill import task
import os
import json
import psycopg2
from psycopg2 import sql
from tqdm import tqdm
from pynetdicom import AE, evt, build_role, debug_logger
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelGet,
    StudyRootQueryRetrieveInformationModelFind,
    CTImageStorage,
    MRImageStorage,
    SecondaryCaptureImageStorage,
)
from pydicom.dataset import Dataset
from datetime import datetime
from contextlib import contextmanager
import time
from multiprocessing import Pool

pacs_credentials = wmill.get_resource("f/dicoms/trinidad_pacs")
db_credentials = json.loads(wmill.get_variable("f/kobo/vultr_db"))
db_credentials = db_credentials["db_settings"]

PACS_IP = pacs_credentials['ip']
PACS_PORT = pacs_credentials['port']
PACS_AET = pacs_credentials['aet']
LOCAL_AET = pacs_credentials['local_aet']

storage_dir = ''

@contextmanager
def get_db_connection():
    """Create a fresh database connection for each process"""
    conn = psycopg2.connect(
        dbname=db_credentials['dbname'],
        user=db_credentials['username'],
        password=db_credentials['password'],
        host=db_credentials['host'],
        port=db_credentials['port']
    )
    try:
        yield conn
    finally:
        conn.close()


def current_timestamp():
    """Return the current timestamp as a human-readable string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Define a handler for incoming C-STORE requests
def handle_store(event):
    """Handle a C-STORE request event."""
    ds = event.dataset
    ds.file_meta = event.file_meta
    
    # Define the filename and save the dataset
    patient_id = ds.PatientID
    series_instance_uid = ds.SeriesInstanceUID
    series_instance_uid = '.'.join(series_instance_uid.split('.')[-2:])
    sop_instance_uid = ds.SOPInstanceUID
    series_name = ds.SeriesDescription if 'SeriesDescription' in ds else 'Unknown_Series'
    series_name = series_name.replace(' ', '_')

    series_dir = os.path.join(storage_dir, patient_id, series_name + '___' + series_instance_uid)
    os.makedirs(series_dir, exist_ok=True)

    filename = os.path.join(series_dir, f"{sop_instance_uid}.dcm")
    ds.save_as(filename, write_like_original=False)
    #print(f"Saved file to {filename}")
    return 0x0000

# Function to download series data for a given series instance UID
def download_series(ae, pacs_address, pacs_port, called_aet, local_aet, patient_id, 
                   study_instance_uid, series_instance_uid, series_name, conn,
                   max_retries=20, wait_time=30):
    handlers = [(evt.EVT_C_STORE, handle_store)]
    storage_sop_classes = [CTImageStorage, MRImageStorage, SecondaryCaptureImageStorage]
    roles = [build_role(sop_class, scp_role=True) for sop_class in storage_sop_classes]
    
    ds = Dataset()
    ds.QueryRetrieveLevel = 'SERIES'
    ds.PatientID = patient_id
    ds.StudyInstanceUID = study_instance_uid
    ds.SeriesInstanceUID = series_instance_uid

    retries = 0
    while retries < max_retries:
        try:
            assoc = ae.associate(pacs_address, pacs_port, ae_title=called_aet, 
                               ext_neg=roles, evt_handlers=handlers)

            if assoc.is_established:
                update_download_status(conn, series_instance_uid, 'in_progress')

                responses = assoc.send_c_get(ds, PatientRootQueryRetrieveInformationModelGet)
                
                for (status, identifier) in responses:
                    if status and hasattr(status, 'Status') and status.Status == 0x0000:
                        break
                    elif status and hasattr(status, 'Status') and status.Status not in (0xFF00, 0xFF01):
                        print(f"Failed to retrieve series {series_instance_uid}: 0x{status.Status:04X}")

                assoc.release()
                update_download_status(conn, series_instance_uid, 'complete')
                break

            else:
                print(f"Association rejected, aborted or never connected for SeriesInstanceUID: {series_instance_uid}")

        except AttributeError as e:
            print(f"AttributeError: {e}")
            retries += 1
            if retries < max_retries:
                print(f"Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"Failed to download series {series_instance_uid} after {max_retries} attempts")
                break

def setup_ae():
    """Initialize and setup the Application Entity"""
    ae = AE()
    ae.acse_timeout = 3000
    ae.dimse_timeout = 3000
    ae.network_timeout = 3000
    
    ae.add_requested_context(PatientRootQueryRetrieveInformationModelFind)
    ae.add_requested_context(PatientRootQueryRetrieveInformationModelGet)
    ae.add_requested_context(CTImageStorage)
    ae.add_requested_context(MRImageStorage)
    ae.add_requested_context(SecondaryCaptureImageStorage)
    
    return ae

def worker_process():
    """Worker function for each process with improved error handling and retries"""
    ae = setup_ae()
    worker_id = os.getpid()
    print(f"Starting worker with pid: {worker_id}")
    
    while True:
        try:
            with get_db_connection() as conn:
                series_info = fetch_next_series(conn)
                
                if not series_info:
                    print(f"Worker {worker_id} found no more work, exiting...")
                    return
                
                series_instance_uid, series_name, patient_id, study_instance_uid, numimages = series_info
                print(f"{current_timestamp()} Worker {worker_id} START: {patient_id} - {series_name} - {numimages}")
                
                try:
                    download_series(
                        ae, PACS_IP, PACS_PORT, PACS_AET, LOCAL_AET,
                        patient_id, study_instance_uid, series_instance_uid, 
                        series_name, conn
                    )
                except Exception as e:
                    print(f"Error downloading series {series_instance_uid}: {e}")
                    # Update status to failed so it can be retried later
                    update_download_status(conn, series_instance_uid, 'failed')
                    continue
                
                print(f"{current_timestamp()} Worker {worker_id} END: {patient_id} - {series_name} - {numimages}")
                
        except Exception as e:
            print(f"Critical error in worker {worker_id}: {str(e)}")
            time.sleep(5)  # Add delay before retry
            continue


def update_download_status(conn, series_instance_uid, status):
    """Update the download status in the database."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE fieldsite.series
            SET download_status = %s, date_modified = CURRENT_TIMESTAMP
            WHERE seriesinstanceuid = %s
        """, (status, series_instance_uid))
        conn.commit()


def fetch_next_series(conn):
    """Fetch the next available series from the database with proper transaction handling"""
    with conn.cursor() as cur:
        try:
            # Use SKIP LOCKED to allow multiple workers to fetch different rows
            cur.execute("""
                WITH next_series AS (
                    SELECT 
                        s.seriesinstanceuid,
                        s.seriesdescription,
                        p.patient_id,
                        st.studyinstanceuid,
                        s.numberofimages
                    FROM fieldsite.series s
                    JOIN fieldsite.studies st ON s.studyinstanceuid = st.studyinstanceuid
                    JOIN fieldsite.patients p ON st.patient_id = p.patient_id
                    WHERE (s.download_status IS NULL OR s.download_status = '')
                    ORDER BY s.seriesinstanceuid
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE fieldsite.series s
                SET 
                    download_status = 'pending',
                    date_modified = CURRENT_TIMESTAMP
                FROM next_series ns
                WHERE s.seriesinstanceuid = ns.seriesinstanceuid
                RETURNING 
                    ns.seriesinstanceuid,
                    ns.seriesdescription,
                    ns.patient_id,
                    ns.studyinstanceuid,
                    ns.numberofimages;
            """)
            
            result = cur.fetchone()
            conn.commit()
            return result
            
        except Exception as e:
            conn.rollback()
            print(f"Error in fetch_next_series: {e}")
            return None

def main(
        storage_path = '/mnt/blockstorage/debug',
        num_threads = 3,
):
    global storage_dir
    storage_dir = storage_path

    print(f"Starting download process with {num_threads} workers...")
    
    with Pool(processes=num_threads) as pool:
        try:
            results = [pool.apply_async(worker_process) for _ in range(num_threads)]
            
            # Wait for all workers to complete
            for result in results:
                try:
                    result.get()
                except Exception as e:
                    print(f"Worker failed with error: {e}")
                    
        except KeyboardInterrupt:
            print("\nReceived keyboard interrupt, stopping workers...")
            pool.terminate()
        finally:
            pool.close()
            pool.join()

    print("All workers completed!")