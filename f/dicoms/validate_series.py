import os
import json
import psycopg2
from psycopg2 import sql
from pydicom import dcmread
import wmill
import glob

pacs_credentials = wmill.get_resource("f/dicoms/trinidad_pacs")
db_credentials = json.loads(wmill.get_variable("f/kobo/vultr_db"))
db_credentials = db_credentials["db_settings"]

PACS_IP = pacs_credentials['ip']
PACS_PORT = pacs_credentials['port']
PACS_AET = pacs_credentials['aet']
LOCAL_AET = pacs_credentials['local_aet']

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=db_credentials['dbname'],
    user=db_credentials['username'],
    password=db_credentials['password'],
    host=db_credentials['host'],
    port=db_credentials['port']
)
cur = conn.cursor()

def get_downloaded_images_count(series_dir):
    """Count the number of slices in the given series directory."""
    count = 0
    for filename in os.listdir(series_dir):
        if filename.endswith('.dcm'):
            filepath = os.path.join(series_dir, filename)
            ds = dcmread(filepath)
            if hasattr(ds, 'NumberOfFrames'):
                count += ds.NumberOfFrames
            else:
                count += 1
    return count

def update_validation_status(patient_id, seriesuid, status):
    """Update the validation status in the PostgreSQL database."""
    cur.execute("""
        UPDATE fieldsite.series
        SET validation = %s, date_modified = CURRENT_TIMESTAMP, download_status = %s
        WHERE seriesinstanceuid LIKE %s AND studyid IN (
            SELECT studyid FROM fieldsite.studies
            WHERE patient_id = %s
        )
    """, (status, status, f"%{seriesuid}", patient_id))

    if status == 'failed':
        cur.execute("""
        UPDATE fieldsite.series
        SET validation = '', date_modified = CURRENT_TIMESTAMP, download_status = %s
        WHERE seriesinstanceuid LIKE %s AND studyid IN (
            SELECT studyid FROM fieldsite.studies
            WHERE patient_id = %s
        )
        """, (status, f"%{seriesuid}", patient_id))
    conn.commit()

# Query all series with download status 'complete' grouped by seriesdescription
cur.execute("""
    select
        p.patient_id,
        s.seriesdescription,
        (
        select string_agg(value, '.' order by idx)
        from unnest(string_to_array(s.seriesinstanceuid, '.')) with ordinality as t(value, idx)
        where idx > 6
        ) as seriesuid,
        s.numberofimages
    from
        fieldsite.series s
    join fieldsite.studies st on
        s.studyid = st.studyid
    join fieldsite.patients p on
        st.patient_id = p.patient_id
    where
        s.download_status = 'complete'
        and (s.validation = ''
            or s.validation is null or s.validation = 'failed') order by s.series_datetime desc;
""")

series_list = cur.fetchall()
slices_report = []


# Iterate through each series and compare the downloaded images count with the expected number of images
def main(
        storage_dir = '/blockstorage/dicoms_inprogress'
):
    index = 1
    total_loop = len(series_list)
    for series_info in series_list:
        patient_id, series_name, seriesuid, expected_num_images = series_info
        print(f"Working on: {series_info}")
        # Use glob to find directories that end with the series name
        pattern = os.path.join(storage_dir, patient_id, f'*___{seriesuid}')
        matching_dirs = glob.glob(pattern)
        print(f"Matching dirs: {matching_dirs}")


        if not matching_dirs:
            print(f"Directory does not exist for series: {patient_id} - {series_name} - {seriesuid}")
            update_validation_status(patient_id, seriesuid, 'failed')
            wmill.set_progress(int(index / total_loop * 100))
            index +=1
            slices_report.append({
                "patient_id": patient_id,
                "series_name": series_name,
                "series_uid": seriesuid,
                "expected_num_images": expected_num_images,
                "downloaded_num_images": 0,
                "status": "no_directory"
            })
            continue

        series_dir = matching_dirs[0]
        downloaded_num_images = get_downloaded_images_count(series_dir)

        if downloaded_num_images < expected_num_images:
            slices_report.append({
                "patient_id": patient_id,
                "series_name": series_name,
                "series_uid": seriesuid,
                "expected_num_images": expected_num_images,
                "downloaded_num_images": downloaded_num_images,
                "status": "incomplete_download"
            })
            print(f"Failed images count {series_info}")
            update_validation_status(patient_id, seriesuid, 'failed')
        else:
            slices_report.append({
                "patient_id": patient_id,
                "series_name": series_name,
                "series_uid": seriesuid,
                "expected_num_images": expected_num_images,
                "downloaded_num_images": downloaded_num_images,
                "status": "success"
            })
            print(f"Succeeded on {series_info}")
            update_validation_status(patient_id, seriesuid, 'complete')
        
        wmill.set_progress(int(index / total_loop * 100))
        index +=1

    # Print the report
    #for report in missing_slices_report:
    #    print(f"PatientID: {report['patient_id']}, SeriesName: {report['series_name']}, "
    #        f"Expected: {report['expected_num_images']}, Downloaded: {report['downloaded_num_images']}")

    # Close the database connection
    cur.close()
    conn.close()
    wmill.set_progress(100)
    return slices_report
