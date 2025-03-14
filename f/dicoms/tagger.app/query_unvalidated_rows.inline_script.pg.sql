select
    s.series_id,
    s.studyid,
    s.seriesinstanceuid,
    s.series_datetime,
    s.seriesnumber,
    s.modality,
    s.institutionaldepartmentname ,
    s.seriesdescription ,
    s.bodypartexamined ,
    s.numberofimages ,
    s.comments_on_radiation_dose ,
    s.convolution_kernel ,
    s.protocol_name ,
    s.slice_thickness ,
    s.number_of_slices ,
    s.spacing_between_slices ,
    s.kvp,
    s.aice,
    s.aidr_3d_estd ,
    s.scan_options ,
    s.vol, 
    s.studyinstanceuid ,
    s.image_url ,
    st.*,
    pc.*,
    coalesce(nullif(pc.corrected_patient_id, ''), p.patient_id) as correct_patient_id,
    coalesce(nullif(pc.corrected_patient_name, ''), p.patient_name) as correct_patient_name,
    coalesce(nullif(pc.correct_patient_sex, ''), p.patient_sex) as correct_patient_sex,
    s.file_metadata->>'WindowCenter' AS window_center,
    s.file_metadata->>'WindowWidth' AS window_width
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
    ) and s.file_metadata IS NOT NULL 
  AND s.seriesnumber NOT IN (9000) 
  AND s.seriesdescription NOT IN ('0.5')
  and not (
  (s.file_metadata->>'WindowCenter')::integer = 300 AND (s.file_metadata->>'WindowWidth')::integer = -2700
  OR (s.file_metadata->>'WindowCenter')::integer = 40 AND (s.file_metadata->>'WindowWidth')::integer = 400
  OR (s.file_metadata->>'WindowCenter')::integer = -550 AND (s.file_metadata->>'WindowWidth')::integer = 1600
  OR (s.file_metadata->>'WindowCenter')::integer = 43 AND (s.file_metadata->>'WindowWidth')::integer = 80
  OR (s.file_metadata->>'WindowCenter')::integer = 40 AND (s.file_metadata->>'WindowWidth')::integer = 120
  OR (s.file_metadata->>'WindowCenter')::integer = 30 AND (s.file_metadata->>'WindowWidth')::integer = 320
)
limit 100;