-- MED DISPENSE - THERE IS SOME WRONG WITH THE SOURCE DATA
define prev_month = 'SEP17';
define curr_month = 'OCT17';
define curr_start_date = '''01-OCT-17''';
define upload = 20171210;
define default_instance_num = 1;
define  text_value_type = '''T''';
define  number_value_type = '''N''';
define operator_value = '''E''';

drop table i2b2_obs_fact_med_mars;
create table i2b2_obs_fact_med_mars as
SELECT
    /*+ PARALLEL 4 */
    v.encounter_num as encounter_num,
    v.patient_num as patient_num,
    'RXNORM:' || n.rxcui as concept_cd,
    'MED UNIT NOT SET' as provider_id,
    m.dispense_date as start_date,
    m.dispense_date as end_date, -- the dates need fixed
    '@' as modifier_cd, --@ 
    &text_value_type as valtype_cd,
    v.location_cd as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number ) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id,
    &default_instance_num AS instance_num
FROM
    NEPTUNE_PITT.MED_DISPENSE_STG@neptune_read m, -- mars only
    I2B2_VISIT_DIMENSION_STG v,
    I2B2_ENCOUNTER_MAPPING_STG e,
    ndc_to_rxnorm_map n
WHERE
    m.dispense_date between v.start_date and v.end_date
    and e.patient_ide = m.person_id
    and v.encounter_num = e.encounter_num
    and n.ndc = m.drug_id_str
    and n.rxcui is not null;
    commit;
    
-- THERE ARE NO EPIC MED DISPENSE IN STG
insert into i2b2_obs_fact_med_epic_STG
SELECT
    /*+ PARALLEL 4 */
    v.encounter_num as encounter_num,
    v.patient_num as patient_num,
    'RXNORM:' || m.rxnorm_code as concept_cd,
    m.order_provider_id as provider_id,
    v.start_date as start_date,
    v.start_date as end_date, -- the dates need fixed
    '@' as modifier_cd, --@ 
    &text_value_type as valtype_cd,
    v.location_cd as location_cd,
    cast( null as varchar2(50) ) as tval_char,
    cast( null as number ) as nval_num,
    cast( null as varchar2(50) ) as valueflag_cd,
    cast( null as varchar2(50) ) as units_cd,
    empty_blob() as observation_blob,
    sysdate as update_date,
    sysdate as download_date,
    sysdate as import_date,
    'NEPTUNE' as sourcesystem_cd,
    &upload as upload_id,
    &default_instance_num AS instance_num
FROM
    MED_ORDER_WITH_RXNORM_STG m, -- mars only
    i2b2_vis_dim_STG_xtra v,
    epic_enc_in_mars_vis_STG z
WHERE
    z.encounter_id = m.visit_id
    and v.visit_id = z.visit_id
    and v.research_id = m.person_id;

commit;


select count(*) from i2b2_obs_fact_med_epic_STG;
 
