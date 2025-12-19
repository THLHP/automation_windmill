-- https://www.windmill.dev/docs/getting_started/scripts_quickstart/sql#result-collection
-- result_collection=legacy

-- to only return the result of the last query use '--return_last_result'
-- $1 original_patient_id
-- $2 corrected_patient_id
-- $3 corrected_patient_name
-- $4 correct_patient_sex
-- $5 authored_by
-- $6 notes
INSERT INTO fieldsite.patientid_corrections 
(original_patient,
corrected_patient_id,
corrected_patient_name,
correct_patient_sex,
authored_by,
notes)
VALUES ($1::TEXT, $2::TEXT, $3::TEXT, $4::TEXT, $5::TEXT, $6::TEXT) RETURNING *;
