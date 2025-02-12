select
    s.*,
    st.*,
    pc.*,
    coalesce(nullif(pc.corrected_patient_id, ''), p.patient_id) as correct_patient_id,
    coalesce(nullif(pc.corrected_patient_name, ''), p.patient_name) as correct_patient_name,
    coalesce(nullif(pc.correct_patient_sex, ''), p.patient_sex) as correct_patient_sex
from
    fieldsite.series s
left join 
    fieldsite.studies st on
    s.studyinstanceuid = st.studyinstanceuid
inner join 
    fieldsite.patientid_corrections pc on
    pc.original_patient = st.patient_id
inner join 
    fieldsite.patients p on
    p.patient_id = st.patient_id
where 
    not exists (
        select 1 
        from fieldsite.validate_series vs 
        where vs.series_id = s.series_id
    )
order by
    random()
limit 100;