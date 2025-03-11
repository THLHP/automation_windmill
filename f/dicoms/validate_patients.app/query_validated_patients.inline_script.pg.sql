SELECT * 
    FROM fieldsite.patients p
    WHERE p.patient_id NOT IN (SELECT original_patient FROM fieldsite.patientid_corrections)