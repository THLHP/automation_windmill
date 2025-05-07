import json
import psycopg2
from psycopg2 import sql, extras
from tqdm import tqdm
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind
)
from pydicom.dataset import Dataset
from datetime import datetime
import os
import wmill
import f.dicoms.upload_file as uploader
import csv
from io import StringIO


# Super complex script for detecting patients that are in THLHP cohort
def detect_thlhp_patient(patient, custom_id):
    if len(patient) > 5 and patient[4] == '-' and int(patient[:4]) < 5000:
        return True
    if patient in custom_id:
        print("Adding custom patient:", patient)
        return True
    return False


pacs_credentials = wmill.get_resource("f/dicoms/trinidad_pacs")
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

def main(
        bucket: uploader.s3,
        upload_to_s3: bool,
        custom_patient_ids = [],
):
    if upload_to_s3:
        print("Uploading results to s3")
    print("Running with:", custom_patient_ids)
    cur = conn.cursor()

    # Initialize the Application Entity (AE)
    ae = AE()

    # Add requested presentation context for C-FIND operation
    ae.add_requested_context(PatientRootQueryRetrieveInformationModelFind)

    patient_export_data = []  # List to hold all patient info with THLHP flag

    # Define the PACS server details
    PACS_IP = pacs_credentials['ip']
    PACS_PORT = pacs_credentials['port']
    PACS_AET = pacs_credentials['aet']
    LOCAL_AET = pacs_credentials['local_aet']

    # Define the query dataset
    ds = Dataset()
    ds.QueryRetrieveLevel = 'PATIENT'
    ds.PatientID = ''
    ds.PatientName = ''
    ds.PatientSex = ''

    # Perform the association with the PACS
    assoc = ae.associate(PACS_IP, PACS_PORT, ae_title=PACS_AET)

    if assoc.is_established:
        # Send the C-FIND request
        responses = assoc.send_c_find(ds, PatientRootQueryRetrieveInformationModelFind)
        
        batch_data = []
        batch_size = 100  # Adjust batch size as needed

        for (status, identifier) in responses:
            if status.Status in (0xFF00, 0xFF01):
                patient_id = identifier.PatientID if 'PatientID' in identifier else None
                patient_name = str(identifier.PatientName) if 'PatientName' in identifier else None
                patient_sex = identifier.PatientSex if 'PatientSex' in identifier else None

                if patient_id:
                    is_thlhp = detect_thlhp_patient(patient_id, custom_patient_ids)
                    patient_export_data.append({
                        "patient_id": patient_id,
                        "patient_name": patient_name,
                        "patient_sex": patient_sex,
                        "is_thlhp": is_thlhp
                    })
                    if is_thlhp:
                        batch_data.append((patient_id, patient_name, patient_sex))

                        if len(batch_data) >= batch_size:
                            extras.execute_values(cur, sql.SQL("""
                                INSERT INTO fieldsite.patients (patient_id, patient_name, patient_sex)
                                VALUES %s
                                ON CONFLICT (patient_id) DO UPDATE
                                SET patient_name = EXCLUDED.patient_name,
                                    patient_sex = EXCLUDED.patient_sex,
                                    date_modified = CURRENT_TIMESTAMP;
                            """), batch_data)
                            conn.commit()
                            batch_data = []

        # Insert any remaining data in the batch
        if batch_data:
            extras.execute_values(cur, sql.SQL("""
                INSERT INTO fieldsite.patients (patient_id, patient_name, patient_sex)
                VALUES %s
                ON CONFLICT (patient_id) DO UPDATE
                SET patient_name = EXCLUDED.patient_name,
                    patient_sex = EXCLUDED.patient_sex,
                    date_modified = CURRENT_TIMESTAMP;
            """), batch_data)
            conn.commit()

        # Release the association
        assoc.release()
    else:
        return "Association rejected, aborted or never connected"



    if upload_to_s3:
        # Upload full patient list with THLHP detection results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"patient_export_{timestamp}.csv"
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=["patient_id", "patient_name", "patient_sex", "is_thlhp"])
        writer.writeheader()
        writer.writerows(patient_export_data)

        csv_bytes = output.getvalue().encode("utf-8")
        uploader.main(
            input_file=csv_bytes,
            bucket=bucket,
            file_name=filename
        )
    # Close the database connection
    cur.close()
    conn.close()

    return patient_export_data
    return "Patient data has been populated."
