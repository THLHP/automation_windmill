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
        custom_patient_ids = []
):
    print("Running with:", custom_patient_ids)
    cur = conn.cursor()

    # Initialize the Application Entity (AE)
    ae = AE()

    # Add requested presentation context for C-FIND operation
    ae.add_requested_context(PatientRootQueryRetrieveInformationModelFind)

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

                if patient_id and detect_thlhp_patient(patient_id, custom_patient_ids):
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
        print("Association rejected, aborted or never connected")

    # Close the database connection
    cur.close()
    conn.close()

    print("Patient data has been populated.")
