define prev_month = 'SEP17';
define curr_month = 'OCT17';
define curr_start_date = '''01-OCT-17''';
define upload = 20171210;
 
-- STILL NEED A GOOD CONSISTENT WAY TO FIND NEW PATIENTS
CREATE TABLE NEW_PATIENTS_&curr_month AS 
SELECT * FROM  NEPTUNE_PITT.PATIENT_DEMOGRAPHIC_GOLD@NEPTUNE_READ T1 
WHERE LAST_ENCOUNTER_DATE >= &curr_start_date AND NOT EXISTS
( SELECT NULL FROM ACT_PAT_GOLD T2 WHERE T2.PERSON_ID = T1.PERSON_ID )
ORDER BY BIRTH_DATE DESC;  --3083634
--1967 BABIES BORN IN SEPTEMBER IN UPMC HEALTH SYSTEM


--backup last month
rename ACT_PATIENT_DEM_DIST_STG to ACT_PATIENT_DEM_DIST_&prev_month;
create table ACT_PATIENT_DEM_DIST_STG as 
 SELECT *
  FROM(SELECT    
    d.source_id,
    d.person_id as research_id,
    d.gender_cd,
    d.birth_date,
    d.death_date,
    d.death_source,
    d.ethnic_group_cd,
    'field_changed_nov' as pat_status_cd,
    d.language_cd,
    d.primary_fc_cd,
    d.pt_zip,
    d.state_abbr,
    d.county_cd,
    d.pat_marital_status_cd,
    d.load_date
    , ROW_NUMBER()
                OVER (PARTITION BY d.person_id
                          ORDER BY d.source_id) AS rownumber
         FROM new_patients_&curr_month d 
         WHERE not exists ( select 1 from NCATS2I2B2DEMODATA.patient_mapping m
         where m.patient_ide = d.person_id))
 WHERE rownumber = 1;
 commit;
drop index IDX_ACT_PT_DM_SRC_STG;
create index IDX_ACT_PT_DM_SRC_STG on ACT_PATIENT_DEM_DIST_STG(SOURCE_ID);

 
-- create patient_mapping
--CREATE SEQUENCE  i2b2_patient_num_seq
--MINVALUE 1
--MAXVALUE 99999999999999999999999999
--INCREMENT BY 1 START WITH 800 
--CACHE 20 NOORDER  NOCYCLE  NOPARTITION;
select count(*) from new_patients_&curr_month; --30355

rename i2b2_patient_mapping_STG to i2b2_patient_mapping_&prev_month;
drop table i2b2_patient_mapping_STG;
create table i2b2_patient_mapping_STG as 
SELECT
    research_id as patient_ide,
    source_id as patient_ide_source,
    i2b2_patient_num_seq.nextval as patient_num,
    'ACTIVE' as patient_ide_status,
    'ACT' as project_id,
    sysdate as upload_date,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM ACT_PATIENT_DEM_DIST_STG;
commit;

-- create encounter_mapping
--CREATE SEQUENCE  i2b2_visit_num_seq
--MINVALUE 1
--MAXVALUE 99999999999999999999999999
--INCREMENT BY 1 START WITH 800 
--CACHE 20 NOORDER  NOCYCLE  NOPARTITION;


--->>>>>>>START HERE AFTER CREATING ENCOUNTER / VISITS
rename i2b2_encounter_mapping_STG to i2b2_encounter_mapping_&prev_month;
create table i2b2_encounter_mapping_STG as 
SELECT
    visit_id as encounter_ide,
    'MARS' as encounter_ide_source,
    research_id as patient_ide,
    source_id as patient_ide_source,
    i2b2_visit_num_seq.nextval as encounter_num,
    'ACTIVE' as encounter_ide_status,
    'ACT' as project_id,
    sysdate as upload_date,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM mars_enc_w_enddt_STG;


insert into i2b2_encounter_mapping_STG  
SELECT
    visit_id as encounter_ide,
    'MARS' as encounter_ide_source,
    research_id as patient_ide,
    source_id as patient_ide_source,
    i2b2_visit_num_seq.nextval as encounter_num,
    'ACTIVE' as encounter_ide_status,
    'ACT' as project_id,
    sysdate as upload_date,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM mars_enc_no_enddt_STG;

select count(*) from i2b2_encounter_mapping_STG; --235599 --aug 232182 --sep 218273
commit;
--fix this add to the create and insert above
alter table i2b2_encounter_mapping_STG add rownumber number; 
alter table i2b2_encounter_mapping_STG modify encounter_ide varchar2(20); 

commit;

insert into i2b2_encounter_mapping_STG -- aug 1,636,232
(encounter_num,
encounter_ide,
encounter_ide_source,
patient_ide,
patient_ide_source,
encounter_ide_status,
project_id,
upload_date,
update_date,
download_date,
import_date,
sourcesystem_cd,
upload_id,
rownumber)
select 
i2b2_visit_num_seq.nextval as encounter_num,
encounter_ide,
encounter_ide_source,
patient_ide,
patient_ide_source,
encounter_ide_status,
project_id,
upload_date,
update_date,
download_date,
import_date,
sourcesystem_cd,
upload_id,
rownumber
from (
    SELECT
    visit_id as encounter_ide,
    'EPIC' as encounter_ide_source,
    research_id as patient_ide,
    source_id as patient_ide_source,
    'ACTIVE' as encounter_ide_status,
    'ACT' as project_id,
    sysdate as upload_date,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
    , ROW_NUMBER()
                OVER (PARTITION BY visit_id
                          ORDER BY encounter_id) AS rownumber
         FROM epic_enc_only_vis_STG)
 WHERE rownumber = 1;
commit;

--not step 2 below this line?
--START HERE
-- create 1st visit
select count(distinct research_id) from ACT_PATIENT_DEM_DIST_STG;
--select * from ACT_PATIENT_DEM_DIST_STG;
create table first_patient_visit_STG as
select v.research_id,
    v.visit_id,
    v.source_id,
    v.encounter_num,
    v.patient_num,
    v.active_status_cd,
    v.start_date,
    v.end_date,
    v.inout_cd,
    v.location_cd,
    v.location_path,
    v.length_of_stay,
    v.update_date,
    v.download_date,
    v.import_date,
    v.sourcesystem_cd,
    v.upload_id
    from ( 
SELECT
    research_id,
    visit_id,
    source_id,
    encounter_num,
    patient_num,
    active_status_cd,
    start_date,
    end_date,
    inout_cd,
    location_cd,
    location_path,
    length_of_stay,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    upload_id,
    ROW_NUMBER()
                OVER (PARTITION BY patient_num
                          ORDER BY start_date) AS rownumber
FROM
    i2b2_vis_dim_STG_xtra ) v,
ACT_PATIENT_DEM_DIST_STG p
WHERE v.rownumber = 1
and p.research_id = v.research_id;
commit;


-- this is no longer in use
--select * from i2b2_vis_dim_STG_xtra;
--->>>START HERE WHERE IS THIS TABLE CREATED
--select * from i2b2_vis_dim_0517_xtra;

--select * from i2b2_vis_dim_STG_xtra;

drop table FIRST_PATIENT_VISIT_STG;
select count(*) from FIRST_PATIENT_VISIT_STG;--20319
--select * from FIRST_PATIENT_VISIT_STG;

commit;
--temporary fix - translate later because I have to move mapping tables from the UPMC box 
create table i2b2_patient_dimension1 as 
SELECT
    source_id,
    m.patient_num as patient_num,
    PAT_STATUS_CD as vital_status_cd,
    d.birth_date as birth_date,
    d.death_date as death_date,
    g.code_namespace || g.act_code as sex_cd,
    0 as age_in_years_num,
    cast( null as varchar2(200) ) language_cd,
    g.code_namespace || r.act_code as race_cd,
    d.pat_marital_status_cd as marital_status_cd,
    d.PT_ZIP as zip_cd,
    d.STATE_CD as statecityzip_path,
    d.PRIMARY_FC_CD as income_cd,
    --cast( null as blob ) patient_blob,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
   ACT_PATIENT_DEM_DIST_STG d
   left join i2b2_patient_mapping_STG m on m.patient_ide = d.person_id
   left join act_patient_gender_distinct g on g.research_id = d.person_id
   left join act_patient_race_distinct r on r.research_id = d.person_id;
