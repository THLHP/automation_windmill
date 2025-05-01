import json
import psycopg2
from psycopg2 import sql, extras
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

# List to hold series data for batch insertion
series_data = []

cur = conn.cursor()

# Query all studies from the studies table
cur.execute("SELECT studyid, studyinstanceuid FROM fieldsite.studies WHERE date_created >= CURRENT_DATE - INTERVAL '5 day'")
studies = cur.fetchall()

study_map = {study[1]: study[0] for study in studies}  # Map studyinstanceuid to studyid
study_map_size = len(study_map)

# Initialize the Application Entity (AE)
ae = AE()

# Add requested presentation context for C-FIND operation
ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

def main():
    index = 1
    for studyinstanceuid, studyid in study_map.items():
        index +=1
        current_progress = int(index / study_map_size * 100)
        wmill.set_progress(current_progress)

        # Define the query dataset
        ds = Dataset()
        ds.QueryRetrieveLevel = 'SERIES'
        ds.StudyInstanceUID = studyinstanceuid
        ds.SeriesInstanceUID = ''
        ds.SeriesDate = ''
        ds.SeriesTime = ''
        ds.SeriesNumber = ''
        ds.Modality = ''
        ds.InstitutionName = ''
        ds.InstitutionalDepartmentName = ''
        ds.SeriesDescription = ''
        ds.BodyPartExamined = ''
        ds.NumberOfSeriesRelatedInstances = ''
        ds.add_new((0x0040, 0x0310), 'ST', '')
        ds.add_new((0x0018, 0x1210), 'SH', '')
        ds.add_new((0x0018, 0x1030), 'LO', '')
        ds.add_new((0x0018, 0x0050), 'DS', '')
        ds.add_new((0x0054, 0x0081), 'US', '')
        ds.add_new((0x0018, 0x0088), 'DS', '')
        ds.add_new((0x0018, 0x0060), 'DS', '')
        ds.add_new((0x0018, 0x7005), 'CS', '')
        ds.add_new((0x1092, 0x7005), 'CS', '')
        ds.add_new((0x100B, 0x7005), 'CS', '')
        ds.add_new((0x0010, 0x4000), 'LT', '')
        ds.add_new((0x0018, 0x0022), 'CS', '')
        ds.add_new((0x1011, 0x7005), 'UN', '')

        # Perform the association with the PACS
        assoc = ae.associate(PACS_IP, PACS_PORT, ae_title=PACS_AET)

        if assoc.is_established:
            # Send the C-FIND request
            responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
            
            for (status, identifier) in responses:
                if status.Status in (0xFF00, 0xFF01):
                    series_instance_uid = identifier.SeriesInstanceUID if 'SeriesInstanceUID' in identifier else None
                    series_number = identifier.SeriesNumber if 'SeriesNumber' in identifier else None
                    modality = identifier.Modality if 'Modality' in identifier else None
                    institution_name = identifier.InstitutionName if 'InstitutionName' in identifier else None
                    institutional_department_name = identifier.InstitutionalDepartmentName if 'InstitutionalDepartmentName' in identifier else None
                    series_description = identifier.SeriesDescription if 'SeriesDescription' in identifier else None
                    body_part_examined = identifier.BodyPartExamined if 'BodyPartExamined' in identifier else None
                    number_of_images = identifier.NumberOfSeriesRelatedInstances if 'NumberOfSeriesRelatedInstances' in identifier else None
                    series_date = identifier.SeriesDate if 'SeriesDate' in identifier else None
                    series_time = identifier.SeriesTime if 'SeriesTime' in identifier else None
                    comments_on_radiation_dose = identifier[(0x0040, 0x0310)].value if (0x0040, 0x0310) in identifier else None
                    convolution_kernel = identifier[(0x0018, 0x1210)].value if (0x0018, 0x1210) in identifier else None
                    protocol_name = identifier[(0x0018, 0x1030)].value if (0x0018, 0x1030) in identifier else None
                    slice_thickness = identifier[(0x0018, 0x0050)].value if (0x0018, 0x0050) in identifier else None
                    number_of_slices = identifier[(0x0054, 0x0081)].value if (0x0054, 0x0081) in identifier else None
                    spacing_between_slices = identifier[(0x0018, 0x0088)].value if (0x0018, 0x0088) in identifier else None
                    kvp = identifier[(0x0018, 0x0060)].value if (0x0018, 0x0060) in identifier else None
                    detector_configuration = identifier[(0x0018, 0x7005)].value if (0x0018, 0x7005) in identifier else None
                    aice = identifier[(0x1092, 0x7005)].value if (0x1092, 0x7005) in identifier else None
                    aidr_3d_estd = identifier[(0x100B, 0x7005)].value if (0x100B, 0x7005) in identifier else None
                    patient_comments = identifier[(0x0010, 0x4000)].value if (0x0010, 0x4000) in identifier else None
                    scan_options = identifier[(0x0018, 0x0022)].value if (0x0018, 0x0022) in identifier else None
                    vol = identifier[(0x1011, 0x7005)].value if (0x1011, 0x7005) in identifier else None

                    if series_date and series_time:
                        series_datetime = f"{series_date} {series_time}"
                    elif series_date:
                        series_datetime = series_date
                    elif series_time:
                        series_datetime = series_time
                    else:
                        series_datetime = None

                    if series_instance_uid:
                        series_data.append(
                            (studyid, series_instance_uid, series_datetime, series_number, modality,
                            institution_name, institutional_department_name, series_description,
                            body_part_examined, number_of_images, comments_on_radiation_dose,
                            convolution_kernel, protocol_name, slice_thickness, number_of_slices,
                            spacing_between_slices, kvp, detector_configuration, aice, aidr_3d_estd,
                            patient_comments, scan_options, vol, studyinstanceuid)
                        )

            # Release the association
            assoc.release()
        else:
            print(f"Association rejected, aborted or never connected for StudyInstanceUID: {studyinstanceuid}")

    # Insert series data in batch
    if series_data:
        insert_query = """
            INSERT INTO fieldsite.series (studyid, seriesinstanceuid, series_datetime, seriesnumber, modality,
                                        institutionname, institutionaldepartmentname, seriesdescription,
                                        bodypartexamined, numberofimages, comments_on_radiation_dose, 
                                        convolution_kernel, protocol_name, slice_thickness, number_of_slices, 
                                        spacing_between_slices, kvp, detector_configuration, aice, 
                                        aidr_3d_estd, patient_comments, scan_options, vol, studyinstanceuid)
            VALUES %s
            ON CONFLICT (seriesinstanceuid) DO UPDATE
            SET studyid = EXCLUDED.studyid,
                series_datetime = EXCLUDED.series_datetime,
                seriesnumber = EXCLUDED.seriesnumber,
                modality = EXCLUDED.modality,
                institutionname = EXCLUDED.institutionname,
                institutionaldepartmentname = EXCLUDED.institutionaldepartmentname,
                seriesdescription = EXCLUDED.seriesdescription,
                bodypartexamined = EXCLUDED.bodypartexamined,
                numberofimages = EXCLUDED.numberofimages,
                comments_on_radiation_dose = EXCLUDED.comments_on_radiation_dose,
                convolution_kernel = EXCLUDED.convolution_kernel,
                protocol_name = EXCLUDED.protocol_name,
                slice_thickness = EXCLUDED.slice_thickness,
                number_of_slices = EXCLUDED.number_of_slices,
                spacing_between_slices = EXCLUDED.spacing_between_slices,
                kvp = EXCLUDED.kvp,
                detector_configuration = EXCLUDED.detector_configuration,
                aice = EXCLUDED.aice,
                aidr_3d_estd = EXCLUDED.aidr_3d_estd,
                patient_comments = EXCLUDED.patient_comments,
                scan_options = EXCLUDED.scan_options,
                vol = EXCLUDED.vol,
                studyinstanceuid = EXCLUDED.studyinstanceuid,
                date_modified = CURRENT_TIMESTAMP;
        """
        extras.execute_values(cur, insert_query, series_data)
        conn.commit()

    # Close the database connection
    cur.close()
    conn.close()

    print("Series data has been populated.")

    return series_data
