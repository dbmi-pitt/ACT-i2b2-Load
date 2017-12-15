-- CLEAN OUT LAST run
drop table observation_fact_STG;
commit;
define undefined_provider = '''-1''';


-- SET UP SOME INDEXES INCASE THEY GOT DROPPED
create table act_loincs as --286 loincs
select concept_cd from ncats2i2b2demodata.concept_dimension where concept_cd like 'LOINC%'; -- THESE ARE ONLY ONTOLOGY LOINCS TO SAVE SPACE
create index act_loincs_conc on act_loincs(concept_cd);
commit;

-- CREATE OBSERVATION_FACT STAGING TABLE
-- PRIMARY DIAGNOSIS FROM EPIC
create table observation_fact_STG
as select * from i2b2_obs_fact_dx_epri_STG;
commit;

-- EXPAND COLUMNS JUST IN CASE LARGEST DATA LOADED WAS SMALLER ON THE FIRST VALUES ADDED
alter table observation_fact_STG modify tval_char varchar2(255);
alter table observation_fact_STG modify units_cd varchar2(255);
commit;

-- ADD FACTS TO OBSERVATION_FACT STAGING TABLE
-- OTHER DIAGNOSIS FROM EPIC
-- THESE FACTS WILL NEED DUPLICATED IN FINAL RELEASE,
-- ONE WITH MODIFIER ONE WITH @
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
 i2b2_obs_fact_dx_esec_STG;

commit;

-- MARS PRIMARY DIAGNOSIS
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_dx_mpri_STG;

commit;

-- MARS OTHER DIAGNOSIS
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_dx_msec_STG;

commit;

-- MARS PROCEDURES
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_px_mars_STG;

commit;

-- EPIC PROCEDURES
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_px_epic_STG;

commit;

--LAB NUMBER VALUES
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_labn_STG;

commit;

-- LAB TEXT VALUES
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_labt_STG;

commit;

-- MED ORDERS
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_med_ord_STG;
commit;
-- renamed
--i2b2_obs_fact_med_epic_STG;
commit;
-- renamed

--MED DISPENSE
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_med_disp_STG;
commit;
-- rename this table

-- DEMOGRAPHIC FACTS
-- HISPANIC
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_hisp_mars_STG;
commit;
-- does this exist

--SEX
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_hisp_epic_STG;

commit;

insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_sex_STG;

commit;

--RACE
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_race_STG;

commit;

--VITAL STATUS
insert /*+ APPEND */ into observation_fact_STG
SELECT
    encounter_num,
    patient_num,
    concept_cd,
    &undefined_provider provider_id,
    start_date,
    end_date,
    modifier_cd,
    instance_num,
    valtype_cd,
    location_cd,
    tval_char,
    nval_num,
    valueflag_cd,
    units_cd,
    observation_blob,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id
FROM
i2b2_obs_fact_vs_STG;

commit;

-- THIS MAY BE IRRELEVANT
update observation_fact_STG set concept_cd = replace(concept_cd, 'ICD9Proc%', 'ICD9:') 
where concept_cd like 'ICD9Proc%';


-- GRANT PERMISSIONS TO THE i2b2 schema
grant all on observation_fact_STG to ncats2i2b2demodata;
grant all on i2b2_patient_mapping_STG to ncats2i2b2demodata;
grant all on i2b2_encounter_mapping_stg to ncats2i2b2demodata;
grant all on i2b2_visit_dimension_stg to ncats2i2b2demodata;
commit;

select count(*) from observation_fact_STG; --jul 4,680,744
grant all on observation_fact_STG to ncats2i2b2demodata;
grant all on i2b2_patient_mapping_STG to ncats2i2b2demodata;
grant all on i2b2_patient_dimension_stg to ncats2i2b2demodata;
grant all on i2b2_encounter_mapping_stg to ncats2i2b2demodata;
grant all on i2b2_visit_dimension_stg to ncats2i2b2demodata;
commit;
update observation_fact_STG set concept_cd = replace(concept_cd, 'ICD9Proc%', 'ICD9:') 
where concept_cd like 'ICD9Proc%';

commit;

/* spot check
select * from observation_fact_stg where concept_cd like 'LOINC:%';
select * from observation_fact_stg where concept_cd like 'ICD9:%'; -- none
select * from observation_fact_stg where concept_cd like 'RXNORM:%';
select * from observation_fact_stg where concept_cd like 'ICD10:%';
select * from observation_fact_stg where concept_cd like 'CPT%';
select * from observation_fact_STG where concept_cd like 'ICD9Proc%';
select * from i2b2_obs_fact_icd9_proc;
*/
