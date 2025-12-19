-- https://www.windmill.dev/docs/getting_started/scripts_quickstart/sql#result-collection
-- result_collection=legacy

SELECT * 
    FROM fieldsite.patients p
    WHERE p.patient_id NOT IN (SELECT original_patient FROM fieldsite.patientid_corrections)