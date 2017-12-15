--Run in acts_etl schema
define prev_month = 'SEP17';
define curr_month = 'OCT17';
define curr_start_date = '''01-OCT-17''';
define upload = 20171210;

--Create new patient mapping - start with production patient_mapping
drop table acts_etl.i2b2_patient_mapping;
create table acts_etl.i2b2_patient_mapping as select * from NCATS2I2B2DEMODATA.patient_mapping;
--Add patients new to the system this month
insert into acts_etl.i2b2_patient_mapping select * from i2b2_patient_mapping_STG;
commit;

-- Create Visit Dimension Current Month
drop table acts_etl.i2b2_visit_dimension_STG;
create table acts_etl.i2b2_visit_dimension_STG as 
SELECT
    v.source_id as source_id,
    e.encounter_num as encounter_num,
    p.patient_num as patient_num,
    'ACTIVE' as active_status_cd,
    v.start_date as start_date,
    v.end_date as end_date,
    v.enc_type_cd as inout_cd, -- translate
    v.dept_facility_id as location_cd,
    v.hospital_service_cd as location_path,
    decode(v.end_date,null,0,trunc((v.end_date - v.start_date))) as length_of_stay,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id,
    cast( null as number ) as  rownumber 
FROM  
   mars_enc_w_enddt_STG v
   join i2b2_patient_mapping p on p.patient_ide = v.research_id
   join i2b2_encounter_mapping_STG e on e.encounter_ide = v.visit_id;
commit;

-- add mars no end date visits
insert into i2b2_visit_dimension_STG
SELECT
    v.source_id as source_id,
    e.encounter_num as encounter_num,
    p.patient_num as patient_num,
    'ACTIVE' as active_status_cd,
    v.start_date as start_date,
    v.end_date as end_date,
    v.enc_type_cd as inout_cd, -- translate
    v.dept_facility_id as location_cd,
    v.hospital_service_cd as location_path,
    decode(v.end_date,null,0,trunc((v.end_date - v.start_date))) as length_of_stay,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id,
    cast( null as number ) as  rownumber 
FROM  
   mars_enc_no_enddt_STG v
   join i2b2_patient_mapping p on p.patient_ide = v.research_id
   join i2b2_encounter_mapping_STG e on e.encounter_ide = v.visit_id;
commit;

-- Are there any overlapping visits
create table tmp_overlap_vis as 
select * from i2b2_visit_dimension_STG 
where encounter_num in ( 
select encounter_num from i2b2_visit_dimension_STG
group by encounter_num
having count(*) > 1)
order by encounter_num;

--Delete any overlaping duplicate visits
delete from i2b2_visit_dimension_STG
where encounter_num in 
( select encounter_num from tmp_overlap_vis);

commit;


--->>> START HERE
-- prune encounters to one per visit
drop table acts_etl.tmp_epic_vis_STG;
create table acts_etl.tmp_epic_vis_STG as
select * from (SELECT
    v.source_id as source_id,
    v.research_id,
    v.visit_id,
    v.encounter_id,
     'ACTIVE' as active_status_cd,
    v.start_date as start_date,
    v.end_date as end_date,
    v.enc_type_cd as inout_cd, -- translate to out if epic only - is there defined I or ER type in epic
    v.dept_facility_id as location_cd,
    v.hospital_service_cd as location_path,
    decode(v.end_date,null,0,trunc((v.end_date - v.start_date))) as length_of_stay,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id, 
    ROW_NUMBER()
   OVER (PARTITION BY visit_id
     ORDER BY encounter_id) AS rownumber
     FROM epic_enc_only_vis_STG v)
 WHERE rownumber = 1; --2 min
 commit;

--add in linked visits to patient_ide and encounter_num from encounter_mapping and patient_mapping
insert into i2b2_visit_dimension_STG
select 
   v.source_id as source_id,
   e.encounter_num as encounter_num,
   p.patient_num as patient_num,
    'ACTIVE' as active_status_cd,
    v.start_date as start_date,
    v.end_date as end_date,
    'O' as inout_cd, -- translate
    v.location_cd as location_cd,
    v.location_path as location_path,
    decode(v.end_date,null,0,trunc((v.end_date - v.start_date))) as length_of_stay,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id,
    v.rownumber as rownumber
from
 TMP_EPIC_VIS_STG v
 join i2b2_patient_mapping p on p.patient_ide = v.research_id
 join i2b2_encounter_mapping_STG e on e.encounter_ide = v.visit_id;
 commit;

-- Set the visit type - TODO: once mapping tables set could potentially add more detail
update  i2b2_visit_dimension_STG set inout_cd = 'O' where inout_cd not in ('I', 'O', 'E'); --1,544,235
commit;


