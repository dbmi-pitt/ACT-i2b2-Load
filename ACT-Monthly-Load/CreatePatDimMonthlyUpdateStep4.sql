define prev_month = 'SEP17';
define curr_month = 'OCT17';
define curr_start_date = '''01-OCT-17''';
define upload = 20171210;
define default_instance_num = 1;
define text_value_type = '''T''';

DROP TABLE PAT_DEM_ACT_VENEER;
CREATE TABLE PAT_DEM_ACT_VENEER AS 
SELECT * FROM (
WITH
HAS_RACE AS (
select person_id, race_cd, act_race FROM (
select person_id, race_cd, line_num, decode(line_num, 1, tmp_act_race, 'DEM|RACE:M') as act_race 
from (
select person_id, race_cd, line_num, code_namespace||act_code as tmp_act_race, 
 ROW_NUMBER()
   OVER (PARTITION BY person_id
     ORDER BY load_date desc, LINE_NUM desc) AS rownumber
     from
     NEPTUNE_PITT.PATIENT_RACE@NEPTUNE_READ d, 
    act_code_relationships r 
    where r.data_source = d.source_id
    and r.rel_type_code like 'ACT_%_Race'
    and r.code = d.race_cd )
 WHERE rownumber = 1)),
  
HAS_SEX AS (
select person_id, gender_cd, act_gender 
from (
select person_id, gender_cd, code_namespace||act_code as act_gender, 
 ROW_NUMBER()
   OVER (PARTITION BY person_id
     ORDER BY load_date desc) AS rownumber
     from
     new_patients_stg d, 
    act_code_relationships r 
    where r.data_source = d.source_id
    and r.rel_type_code like 'ACT_%_Gender'
    and r.code = d.gender_cd )
 WHERE rownumber = 1),
 
HAS_VITAL_STATUS AS (
select person_id, vital_status_cd, act_vital_status 
from (
select person_id, pat_status_cd as vital_status_cd, code_namespace||act_code as act_vital_status, 
 ROW_NUMBER()
   OVER (PARTITION BY person_id
     ORDER BY load_date desc) AS rownumber
     from
     new_patients_stg d, 
    act_code_relationships r 
    where r.data_source = d.source_id
    and r.rel_type_code like 'ACT_%_vital status'
    and r.code = d.pat_status_cd )
 WHERE rownumber = 1),
 
mars_hispanic as (
  select person_id, race_cd, mars_act_hispanic from (
    select person_id, race_cd,  code_namespace||act_code as mars_act_hispanic, 
    ROW_NUMBER()
   OVER (PARTITION BY person_id
     ORDER BY load_date desc, LINE_NUM desc) AS rownumber
     from
     NEPTUNE_PITT.PATIENT_RACE@NEPTUNE_READ d, 
    act_code_relationships r 
    where d.source_id = 'MARS'
    and r.data_source = d.source_id
    and r.rel_type_code like 'ACT_%_hispanic'
    and r.code = d.race_cd )
 WHERE rownumber = 1),
 
 epic_hispanic aS (
 select person_id, ethnic_group_cd,  epic_act_hispanic from (
    select person_id, ethnic_group_cd,  code_namespace||act_code as epic_act_hispanic, 
 ROW_NUMBER()
   OVER (PARTITION BY person_id
     ORDER BY load_date desc) AS rownumber
     from
     NEPTUNE_PITT.PATIENT_DEMOGRAPHIC@NEPTUNE_READ d, 
    act_code_relationships r 
    where d.source_id = 'EPIC'
    and r.data_source = d.source_id
    and r.rel_type_code like 'ACT_%_hispanic'
    and r.code = d.ethnic_group_cd )
 WHERE rownumber = 1),
 
 ALL_HISPANIC AS 
 (SELECT  e.person_id, e.ethnic_group_cd,  e.epic_act_hispanic,  m.race_cd mars_race_cd, m.mars_act_hispanic
 FROM EPIC_HISPANIC E
  FULL OUTER JOIN MARS_HISPANIC M
  ON E.PERSON_ID = M.PERSON_ID),

HAS_HISPANIC AS(
    select PERSON_ID, NVL(EPIC_ACT_HISPANIC, MARS_ACT_HISPANIC) AS ACT_HISPANIC 
    from ALL_HISPANIC)
SELECT P.SOURCE_ID,
P.PERSON_ID,
P.BIRTH_DATE,
P.DEATH_DATE,
P.DEATH_SOURCE,
P.LANGUAGE_CD,
P.PRIMARY_FC_CD,
P.PT_ZIP,
P.STATE_ABBR,
P.COUNTY_CD,
p.COUNTRY_CD,
P.PCP_PROV_ID,
P.PAT_MARITAL_STATUS_CD,
P.LOAD_DATE,
H.ACT_HISPANIC, 
R.RACE_CD, 
R.ACT_RACE, 
S.GENDER_CD, 
S.ACT_GENDER, 
V.VITAL_STATUS_CD,
V.ACT_VITAL_STATUS
FROM new_patients_stg p
left outer join HAS_HISPANIC H on H.PERSON_ID = P.PERSON_ID
left outer join HAS_RACE R on R.PERSON_ID = P.PERSON_ID
left outer join HAS_SEX S on S.PERSON_ID = P.PERSON_ID
left outer join HAS_VITAL_STATUS V on V.PERSON_ID = P.PERSON_ID
where  
EXISTS (select 1 from VISIT_FIRST v WHERE START_DATE >= &curr_start_date
and p.person_id = v.person_id)); --23628
commit;

drop table I2B2_PATIENT_DIMENSION_STG;
CREATE TABLE I2B2_PATIENT_DIMENSION_STG AS
SELECT
    m.patient_num,
    v.birth_date birth_date,
    v.death_date death_date,
    v.ACT_RACE race_cd,
    v.ACT_GENDER sex_cd,
    v.ACT_VITAL_STATUS vital_status_cd,
    v.ACT_HISPANIC,
    trunc((sysdate - v.birth_date)/365) as age_in_years_num,
    v.language_cd,
    v.PAT_MARITAL_STATUS_CD marital_status_cd,
    cast(null as varchar(20)) as  religion_cd,
   v.pt_zip as zip_cd,
    v.state_abbr as statecityzip_path,
    v.primary_fc_cd as income_cd,
    empty_blob() as patient_blob,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
from PAT_DEM_ACT_VENEER V, 
I2B2_PATIENT_MAPPING_STG M
where V.PERSON_ID = M.PATIENT_IDE;
commit;

-- create demographic observation facts GENDER/RACE/HISP/VITAL_STATUS/
drop table i2b2_obs_fact_hisp_STG;
create table i2b2_obs_fact_hisp_STG as 
SELECT
    E.encounter_num as encounter_num,
    P.patient_num as patient_num,
    D.ACT_HISPANIC as concept_cd,
    F.provider_id as provider_id,
    F.start_date as start_date,
    '@' as modifier_cd,
    &default_instance_num as instance_num,
    &text_value_type as valtype_cd,
    F.start_date as end_date,
    F.dept_facility_id as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number ) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob, -- in future put raw values in here
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    I2B2_PATIENT_DIMENSION_STG d, 
    VISIT_FIRST f,
    I2B2_ENCOUNTER_MAPPING_STG E,
    I2B2_PATIENT_MAPPING_STG P
WHERE
    E.ENCOUNTER_IDE = F.VISIT_ID
    AND F.PERSON_ID = P.PATIENT_IDE
    AND P.PATIENT_NUM = D.PATIENT_NUM
    AND D.ACT_HISPANIC IS NOT NULL;
    
 
drop table i2b2_obs_fact_sex_STG;
create table i2b2_obs_fact_sex_STG as 
SELECT
    E.encounter_num as encounter_num,
    P.patient_num as patient_num,
    D.SEX_CD as concept_cd,
    F.provider_id as provider_id,
    F.start_date as start_date,
    '@' as modifier_cd,
    &default_instance_num as instance_num,
    &text_value_type as valtype_cd,
    F.start_date as end_date,
    F.dept_facility_id as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number ) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob, -- in future put raw values in here
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    I2B2_PATIENT_DIMENSION_STG d, 
    VISIT_FIRST f,
    I2B2_ENCOUNTER_MAPPING_STG E,
    I2B2_PATIENT_MAPPING_STG P
WHERE
    E.ENCOUNTER_IDE = F.VISIT_ID
    AND F.PERSON_ID = P.PATIENT_IDE
    AND P.PATIENT_NUM = D.PATIENT_NUM
    AND D.SEX_CD IS NOT NULL;
COMMIT;

drop table i2b2_obs_fact_race_STG;
create table i2b2_obs_fact_race_STG as 
SELECT
    E.encounter_num as encounter_num,
    P.patient_num as patient_num,
    D.RACE_CD as concept_cd,
    F.provider_id as provider_id,
    F.start_date as start_date,
    '@' as modifier_cd,
    &default_instance_num as instance_num,
    &text_value_type as valtype_cd,
    F.start_date as end_date,
    F.dept_facility_id as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number ) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob, -- in future put raw values in here
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    I2B2_PATIENT_DIMENSION_STG d, 
    VISIT_FIRST f,
    I2B2_ENCOUNTER_MAPPING_STG E,
    I2B2_PATIENT_MAPPING_STG P
WHERE
    E.ENCOUNTER_IDE = F.VISIT_ID
    AND F.PERSON_ID = P.PATIENT_IDE
    AND P.PATIENT_NUM = D.PATIENT_NUM
    AND D.RACE_CD IS NOT NULL;
COMMIT;

drop table i2b2_obs_fact_vs_STG;
create table i2b2_obs_fact_vs_STG as 
SELECT
    E.encounter_num as encounter_num,
    P.patient_num as patient_num,
    D.VITAL_STATUS_CD as concept_cd,
    F.provider_id as provider_id,
    F.start_date as start_date,
    '@' as modifier_cd,
    &default_instance_num as instance_num,
    &text_value_type as valtype_cd,
    F.start_date as end_date,
    F.dept_facility_id as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number ) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob, -- in future put raw values in here
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id
FROM
    I2B2_PATIENT_DIMENSION_STG d, 
    VISIT_FIRST f,
    I2B2_ENCOUNTER_MAPPING_STG E,
    I2B2_PATIENT_MAPPING_STG P
WHERE
    E.ENCOUNTER_IDE = F.VISIT_ID
    AND F.PERSON_ID = P.PATIENT_IDE
    AND P.PATIENT_NUM = D.PATIENT_NUM
    AND D.VITAL_STATUS_CD IS NOT NULL;
COMMIT;

select * from I2B2_OBS_FACT_VS_STG;
select * from I2B2_OBS_FACT_HISP_STG;
select * from I2B2_OBS_FACT_RACE_STG;
select * from I2B2_OBS_FACT_SEX_STG;
select * from I2B2_PATIENT_DIMENSION_STG;
