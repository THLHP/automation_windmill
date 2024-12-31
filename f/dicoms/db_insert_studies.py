import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from tqdm import tqdm
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelFind
)
from pydicom.dataset import Dataset
from datetime import datetime
import os
import wmill
import pandas as pd

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

# Define the PACS server details
PACS_IP = pacs_credentials['ip']
PACS_PORT = pacs_credentials['port']
PACS_AET = pacs_credentials['aet']
LOCAL_AET = pacs_credentials['local_aet']

cur = conn.cursor()
print("Getting patients from DB")
# Query all patients from the patients table
cur.execute("SELECT patient_id FROM fieldsite.patients")
patients = cur.fetchall()

patient_ids = [patient[0] for patient in patients]

# Initialize the Application Entity (AE)
ae = AE()

# Add requested presentation context for C-FIND operation
ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

# List to hold study data for batch insertion
studies_data = []

def main():
    # Calculate total number of steps (number of patient IDs)
    total_patients = len(patient_ids)
    index = 1



    print("Iterating over patients")

    for patient_id in patient_ids:
        index += 1
        # Calculate the current progress percentage and update the progress bar
        current_progress = index / total_patients * 100
        wmill.set_progress(int(current_progress))
    
        # Define the query dataset
        ds = Dataset()
        ds.QueryRetrieveLevel = 'STUDY'
        ds.PatientID = patient_id
        ds.StudyInstanceUID = ''
        ds.StudyDate = ''
        ds.StudyTime = ''
        ds.StudyID = ''
        ds.AccessionNumber = ''

        # Perform the association with the PACS
        assoc = ae.associate(PACS_IP, PACS_PORT, ae_title=PACS_AET)

        if assoc.is_established:
            # Send the C-FIND request
            responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
            
            for (status, identifier) in responses:
                if status.Status in (0xFF00, 0xFF01):
                    study_id = identifier.StudyID if 'StudyID' in identifier else None
                    study_instance_uid = identifier.StudyInstanceUID if 'StudyInstanceUID' in identifier else None
                    accession_number = identifier.AccessionNumber if 'AccessionNumber' in identifier else None
                    study_date = identifier.StudyDate if 'StudyDate' in identifier else None
                    study_time = identifier.StudyTime if 'StudyTime' in identifier else None

                    if study_date and study_time:
                        study_datetime = f"{study_date} {study_time}"
                    elif study_date:
                        study_datetime = study_date
                    elif study_time:
                        study_datetime = study_time
                    else:
                        study_datetime = None

                    if study_id and patient_id and study_instance_uid:
                        studies_data.append(
                            (study_id, patient_id, study_datetime, study_instance_uid, accession_number)
                        )

            # Release the association
            assoc.release()
        else:
            print(f"Association rejected, aborted or never connected for PatientID: {patient_id}")

    # Convert studies data to a DataFrame
    studies_df = pd.DataFrame(studies_data, columns=['StudyID', 'PatientID', 'StudyDatetime', 'StudyInstanceUID', 'AccessionNumber'])

    # Insert studies data in batch
    if studies_data:
        insert_query = """
            INSERT INTO fieldsite.studies (studyid, patient_id, study_datetime, studyinstanceuid, accession_number)
            VALUES %s
            ON CONFLICT (studyinstanceuid) DO UPDATE
            SET patient_id = EXCLUDED.patient_id,
                study_datetime = EXCLUDED.study_datetime,
                studyinstanceuid = EXCLUDED.studyinstanceuid,
                accession_number = EXCLUDED.accession_number,
                date_modified = CURRENT_TIMESTAMP;
        """
        execute_values(cur, insert_query, studies_data)
        conn.commit()

    # Close the database connection
    cur.close()
    conn.close()

    print("Studies data has been populated.")

    return studies_df
