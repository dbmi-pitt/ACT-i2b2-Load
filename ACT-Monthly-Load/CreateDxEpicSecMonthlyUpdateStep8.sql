define prev_month = 'SEP17';
define curr_month = 'OCT17';
define curr_start_date = '''01-OCT-17''';
define upload = 20171210;
define default_instance_num = 1;
define text_value_type = '''T''';

drop table i2b2_obs_fact_dx_esec_STG;
create table i2b2_obs_fact_dx_esec_STG as   -- 3 min
SELECT
    /*+ PARALLEL(4) */
    e.encounter_num as encounter_num,
    p.patient_num as patient_num,
    d.dx_type || ':' || d.dx_code as concept_cd,
    d.provider_id as provider_id,
    z.start_date as start_date,
    z.end_date as end_date,
    '@' as modifier_cd, --@ 'ACT|DIAG_PRIORTY:S' when new disk space
    &default_instance_num as instance_num,
    &text_value_type as valtype_cd,
    z.hospital_service_cd as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    neptune_pitt.diagnosis_stg@neptune_read d,
    i2b2_encounter_mapping_STG e, 
    i2b2_patient_mapping p, 
    epic_enc_eq_mars_vis_STG z
WHERE
    e.patient_ide = d.person_id
    and z.encounter_id = d.visit_id
    and e.encounter_ide = z.visit_id
    and p.patient_ide = d.person_id
    and d.primary_dx_ind = 'N'
    and d.source_id = 'EPIC';
commit;

-- add sec diagnosis for encounters that are in inpatient visits
insert into i2b2_obs_fact_dx_esec_STG   -- 3 min
SELECT
    /*+ PARALLEL(4) */
    e.encounter_num as encounter_num,
    p.patient_num as patient_num,
    d.dx_type || ':' || d.dx_code as concept_cd,
    d.provider_id as provider_id,
    z.start_date as start_date,
    z.end_date as end_date,
    '@' as modifier_cd, --@
    &default_instance_num as instance_num,
    &text_value_type as valtype_cd,
    z.hospital_service_cd as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    neptune_pitt.diagnosis_stg@neptune_read d,
    i2b2_encounter_mapping_STG e, 
    i2b2_patient_mapping p, 
    epic_enc_in_mars_vis_STG z
WHERE
    e.patient_ide = d.person_id
    and z.encounter_id = d.visit_id
    and e.encounter_ide = z.visit_id
    and p.patient_ide = d.person_id
    and d.primary_dx_ind = 'N'
    and d.source_id = 'EPIC';
commit;

-- add primary diagnosis for encounters epic only
insert into i2b2_obs_fact_dx_esec_STG   -- 3 min
SELECT
    /*+ PARALLEL(4) */
    e.encounter_num as encounter_num,
    p.patient_num as patient_num,
    d.dx_type || ':' || d.dx_code as concept_cd,
    d.provider_id as provider_id,
    z.start_date as start_date,
    z.end_date as end_date,
    '@' as modifier_cd, --@
    &default_instance_num as instance_num,
    &text_value_type as valtype_cd,
    z.hospital_service_cd as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number ) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    neptune_pitt.diagnosis_stg@neptune_read d,
    i2b2_encounter_mapping_STG e, 
    i2b2_patient_mapping p, 
    epic_enc_only_vis_STG z
WHERE
    e.patient_ide = d.person_id
    and z.encounter_id = d.visit_id
    and e.encounter_ide = z.visit_id
    and p.patient_ide = d.person_id
    and d.primary_dx_ind = 'N'
    and d.source_id = 'EPIC';

commit;
