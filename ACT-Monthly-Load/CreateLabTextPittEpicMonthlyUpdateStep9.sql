define prev_month = 'SEP17';
define curr_month = 'OCT17';
define curr_start_date = '''01-OCT-17''';
define upload = 20171210;
define default_instance_num = 1;
define  text_value_type = '''T''';
define  number_value_type = '''N''';
define operator_value = '''E''';

drop table i2b2_obs_fact_labt_stg;
create table i2b2_obs_fact_labt_STG as 
SELECT
    v.encounter_num as encounter_num,
    v.patient_num as patient_num,
    'LOINC:' || m.loinc_cd as concept_cd,
    'PROVIDER NEEDS MAPPED' as provider_id,
    l.specimen_collected_date as start_date,
    l.specimen_collected_date as end_date, -- the dates need fixed
    '@' as modifier_cd, --@ 
    &text_value_type as valtype_cd,
    v.location_cd as location_cd,
    l.ord_value as tval_char,
    cast(null as number) as nval_num,
    l.result_flag_cd as valueflag_cd,
    l.reference_unit as units_cd,
    empty_blob() as observation_blob,
    &default_instance_num AS instance_num,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    neptune_pitt.LAB_RESULT_STG@neptune_read l, -- epic only
    i2b2_vis_dim_STG_xtra v,
    epic_enc_in_mars_vis_STG z,
    component_loinc_mapping m
where l.component_id = m.component_id
    and z.encounter_id = l.visit_id
    and v.visit_id = z.visit_id
    and v.research_id = l.person_id
    and l.ord_num_value is null;
 commit;
 
--ADD MORE LAB TEXT FACTS
insert into i2b2_obs_fact_labt_STG
SELECT
    v.encounter_num as encounter_num,
    v.patient_num as patient_num,
    'LOINC:' || m.loinc_cd as concept_cd,
    'PROVIDER NEEDS MAPPED' as provider_id,
    l.specimen_collected_date as start_date,
    l.specimen_collected_date as end_date, -- the dates need fixed
    '@' as modifier_cd, --@ 
    &text_value_type as valtype_cd,
    v.location_cd as location_cd,
    l.ord_value as tval_char,
    cast(null as number) as nval_num,
    l.result_flag_cd as valueflag_cd,
    l.reference_unit as units_cd,
    empty_blob() as observation_blob,
    &default_instance_num AS instance_num,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    neptune_pitt.LAB_RESULT_STG@neptune_read l, -- epic only
    i2b2_vis_dim_STG_xtra v,
    epic_enc_eq_mars_vis_STG z,
    component_loinc_mapping m
where l.component_id = m.component_id
    and z.encounter_id = l.visit_id
    and v.visit_id = z.visit_id
    and v.research_id = l.person_id
    and l.ord_num_value is null;
 commit;
   
select count(*) from I2B2_OBS_FACT_labt_STG; 

insert into i2b2_obs_fact_labt_STG
SELECT
    v.encounter_num as encounter_num,
    v.patient_num as patient_num,
    'LOINC:' || m.loinc_cd as concept_cd,
    'PROVIDER NEEDS MAPPED' as provider_id,
    l.specimen_collected_date as start_date,
    l.specimen_collected_date as end_date, -- the dates need fixed
    '@' as modifier_cd, --@ 
    &text_value_type as valtype_cd,
    v.location_cd as location_cd,
    l.ord_value as tval_char,
    cast(null as number) as nval_num,
    l.result_flag_cd as valueflag_cd,
    l.reference_unit as units_cd,
    empty_blob() as observation_blob,
    &default_instance_num AS instance_num,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    neptune_pitt.LAB_RESULT_STG@neptune_read l, -- epic only
    i2b2_vis_dim_STG_xtra v,
    epic_enc_only_vis_STG z,
    component_loinc_mapping m
where l.component_id = m.component_id
    and z.encounter_id = l.visit_id
    and v.visit_id = z.visit_id
    and v.research_id = l.person_id
    and l.ord_num_value is null;
 commit;
select count(*) from I2B2_OBS_FACT_labt_STG; --sep1041390



