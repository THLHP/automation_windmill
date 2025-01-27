import json
import wmill
import psycopg2
import os
import glob
import zipfile

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

# Query all series with download status 'complete' grouped by seriesdescription
cur.execute("""
    select
        p.patient_id,
        s.seriesdescription,
        (
            select string_agg(value, '.' order by idx)
            from unnest(string_to_array(s.seriesinstanceuid, '.')) with ordinality as t(value, idx)
            where idx > 6
        ) as seriesuid
    from
        fieldsite.series s
    join fieldsite.studies st on
        s.studyid = st.studyid
    join fieldsite.patients p on
        st.patient_id = p.patient_id
    where
        s.validation = 'complete'
        and (s.compression_status = ''
            or s.compression_status is null
            or s.compression_status = 'failed')
    order by s.series_datetime desc limit 100;
""")

series_list = cur.fetchall()
compression_report = []

def update_validation_status(patient_id, seriesuid, status):
    """Update the validation status in the PostgreSQL database."""
    print("About to update", status, seriesuid, patient_id)
    cur.execute("""
        UPDATE fieldsite.series
        SET compression_status = %s, date_modified = CURRENT_TIMESTAMP
        WHERE seriesinstanceuid LIKE %s AND studyid IN (
            SELECT studyid FROM fieldsite.studies
            WHERE patient_id = %s
        )
    """, (status, f"%{seriesuid}", patient_id))
    conn.commit()
        

def main(
        inprogress_dir = '/blockstorage/dicoms_inprogress',
        destination_dir = '/blockstorage/dicoms_complete'
):
    index = 1
    total_loop = len(series_list)
    for series_info in series_list:
        patient_id, series_name, seriesuid = series_info
        print(f"Working on: {series_info}")
        # Use glob to find directories that end with the series name
        pattern = os.path.join(inprogress_dir, patient_id, f'*___{seriesuid}')
        matching_dirs = glob.glob(pattern)
        print(f"Matching dirs: {matching_dirs}")

        report = {
            "patient_id": patient_id, 
            "series_name": series_name, 
            "seriesuid": seriesuid,
            "status": "unknown"
            }
        
        if matching_dirs:
            series_dir = matching_dirs[0]
            # Get the directory path for the destination zip file
            dest_path = os.path.join(destination_dir, patient_id)
            os.makedirs(dest_path, exist_ok=True)

            # Create the zip file name by getting the base name of the series dir and adding '.zip'
            series_base_name = os.path.basename(series_dir)
            zip_file_name = f"{series_base_name}.zip"
            dest_zip_file = os.path.join(dest_path, zip_file_name)

            report["src_dir"] = series_dir
            report["dst_zip"] = dest_zip_file

            print(f"Compressing {series_dir} to {dest_zip_file}")
            
            # Compress the series directory
            with zipfile.ZipFile(dest_zip_file, 'w') as zipf:
                for root, dirs, files in os.walk(series_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        rel_path = os.path.relpath(file_path, start=inprogress_dir)
                        zipf.write(file_path, rel_path)

            print(f"Compression of {series_dir} complete")
            report["status"] = "complete"
            update_validation_status(patient_id, seriesuid, "complete")
        else:
            print(f"No matching directory found for {series_info}")
            report["status"] = "failed"
            update_validation_status(patient_id, seriesuid, "failed")

        compression_report.append(report)
        wmill.set_progress(int(index / total_loop * 100))
        index +=1


    # Close the database connection
    cur.close()
    conn.close()
    wmill.set_progress(100)
    return compression_report
