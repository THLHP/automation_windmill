-- https://www.windmill.dev/docs/getting_started/scripts_quickstart/sql#result-collection
-- result_collection=legacy

SELECT 
    COUNT(CASE WHEN c.original_patient IS NULL THEN p.patient_id END) AS unvalidated_patients,
    COUNT(CASE WHEN c.original_patient IS NOT NULL THEN p.patient_id END) AS validated_patients
FROM 
    fieldsite.patients p
    LEFT JOIN fieldsite.patientid_corrections c ON p.patient_id = c.original_patient;