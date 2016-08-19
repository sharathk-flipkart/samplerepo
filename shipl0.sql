set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 100000000;

INSERT OVERWRITE TABLE shipment_l0_90_fact
SELECT /*+ MAPJOIN(FKL_MH, Rev_FKL_MH,geo_src,geo_dest) */  
S_current.entityid as shipment_id,
If(size(S_current.`data`.associated_shipment_ids)=1,`data`.associated_shipment_ids[0],NULL) as first_associated_shipment_id,
S_current.`data`.vendor_tracking_id AS vendor_tracking_id,
b2CLogisticsRequest.merchant_reference_id AS merchant_reference_id,
b2CLogisticsRequest.merchant_id AS merchant_id,
S_current.`data`.shipment_items[0].seller_id as seller_id,
S_current.`data`.status as shipment_current_status,
S_current.`data`.payment_type as payment_type,
S_current.`data`.payment.payment_details.mode[0] as payment_mode,
S_current.`data`.payment.payment_details[0].device_id as pos_id,
S_current.`data`.payment.payment_details[0].transaction_id as transaction_id,
S_current.`data`.payment.payment_details[0].agent_id as agent_id,
S_current.`data`.payment.amount_collected.value as amount_collected,
If(lower(S_current.`data`.source_address.type) = 'warehouse',
	If(lower(S_current.`data`.shipment_items[0].seller_id) in ('wsr', 'd591418b408940a0'),'WSR',
	if(lower(S_current.`data`.shipment_items[0].seller_id) = 'myn','MYN',
	if(upper(S_current.`data`.shipment_items[0].seller_id) = 'JBN_209','JBN','FA'))),
if(lower(S_current.`data`.source_address.type) = 'mp_non_fbf_seller','Non-FA',
if(lower(S_current.`data`.source_address.type) = 'customer',
If(lower(S_current.`data`.shipment_items[0].seller_id) = 'myn','MYN',
If(lower(S_current.`data`.shipment_items[0].seller_id) in ('wsr', 'd591418b408940a0'),'WSR',
if(upper(S_current.`data`.shipment_items[0].seller_id) = 'JBN_209','JBN',	
If(lower(S_current.`data`.destination_address.type)='warehouse','FA','Non-FA')))),null))) AS seller_type,
If(concat_ws("-",S_current.`data`.attributes) like '%dangerous%',1,0) as shipment_dg_flag,
If(upper(concat_ws("-",S_current.`data`.attributes)) like '%FLASH%',1,0) as shipment_flash_flag,
If(concat_ws("-",S_current.`data`.attributes) like '%ragile%',1,0) as shipment_fragile_flag,
If(b2CLogisticsRequest.shipment_priority_flag IS NULL,'Normal',b2CLogisticsRequest.shipment_priority_flag) AS shipment_priority_flag,
b2CLogisticsRequest.service_tier as service_tier,
b2CLogisticsRequest.surface_mandatory_flag as surface_mandatory_flag,
S_current.`data`.size as shipment_size_flag,
size(S_current.`data`.shipment_items) as item_quantity,
S_current.`data`.shipping_category as shipping_category,
IF(T1.associated_id IS NOT NULL AND S_current.`data`.current_address.type NOT IN ('PL_FACILITY','PICKUP_HUB'),'merchant_return',S_current.`data`.shipment_type) as ekl_shipment_type,
If(S_current.`data`.shipment_type = 'rvp',If(b2CLogisticsRequest.vas_type = 'Product Exchange','PREXO',if(b2CLogisticsRequest.vas_type = 'Replacement','Replacement','Pickup_Only')),NULL) AS reverse_shipment_type,
IF((Upper(geo_src.src_lt) = Upper(geo_dest.dest_lt) OR Upper(geo_src.src_city)=Upper(geo_dest.dest_city)), "INTRACITY", IF(Upper(geo_src.src_zone) = Upper(geo_dest.dest_zone), "INTRAZONE", IF(Upper(geo_src.src_zone) <> Upper(geo_dest.dest_zone), "INTERZONE", "Missing"))) as ekl_fin_zone,
--IF(Upper(geo_src.src_lt) = Upper(geo_dest.dest_lt), "INTRACITY", IF(Upper(geo_src.src_zone) = Upper(geo_dest.dest_zone), "INTRAZONE", IF(Upper(geo_src.src_zone) <> Upper(geo_dest.dest_zone), "INTERZONE", "Missing"))) as ekl_fin_zone,
IF((Upper(geo_src.src_lt) = Upper(geo_dest.dest_lt) OR Upper(geo_src.src_city)=Upper(geo_dest.dest_city)), "LOCAL", 
IF(Upper(geo_src.src_zone) = Upper(geo_dest.dest_zone),
"ZONAL",
IF(Upper(geo_src.src_city) in ('CHENNAI','MUMBAI','NEW DELHI','KOLKATA','BANGALORE','HYDERABAD','AHMEDABAD','PUNE') 
and 
UPPER(geo_dest.dest_city) in ('CHENNAI','MUMBAI','NEW DELHI','KOLKATA','BANGALORE','HYDERABAD','AHMEDABAD','PUNE')
and
geo_src.src_city <> geo_dest.dest_city,
"METRO",
IF(Upper(geo_dest.dest_state) in ('SIKKIM','ASSAM','MANIPUR','MEGHALAYA','MIZORAM','ARUNACHAL PRADESH','NAGALAND','TRIPURA','JAMMU AND KASHMIR'),
"JK_NE",
"ROI"
)
)
)
)
as ekart_lzn_flag,
If(S_current.`data`.source_address.type like '%NON_FBF%',0,If(upper(S_current.`data`.source_address.type)='CUSTOMER' AND lower(S_current.`data`.destination_address.type)='warehouse',1,0)) as shipment_fa_flag,
S_current.`data`.vendor_id AS vendor_id,
If(S_current.`data`.vendor_tracking_id = 'not_assigned' OR S_current.`data`.vendor_tracking_id IS NULL,'VNF',If(S_current.`data`.vendor_id = 200 OR S_current.`data`.vendor_id =207, 'FSD','3PL')) as shipment_carrier,
If(lower(S_current.`data`.status) IN ('lost','reshipped','not_received','delivered','delivery_update','received_by_merchant','returned_to_seller','damaged','pickup_leg_completed','pickup_leg_complete'),0,1) as shipment_pending_flag,
S_group.out_for_pickup_attempts as shipment_num_of_pk_attempt,
S_group.fsd_number_of_ofd_attempts as fsd_number_of_ofd_attempts,
S_group.shipment_rvp_pk_number_of_attempts as shipment_rvp_pk_number_of_attempts,
S_current.`data`.shipment_weight.physical as shipment_weight,
S_current.`data`.sender_weight.physical as sender_weight,
S_current.`data`.system_weight.physical as system_weight,
NULL as volumetric_weight_source,
if(S_current.`data`.vendor_id in (198,204,202,196),(Vol.volumetric_weight *6/5),Vol.volumetric_weight )  AS volumetric_weight,
If(cast(if(S_current.`data`.vendor_id in (198,204,202,196),(Vol.volumetric_weight *6/5),Vol.volumetric_weight ) as int) > CAST(If(S_current.`data`.shipment_weight.physical IS NULL,0,S_current.`data`.shipment_weight.physical) AS int),if(S_current.`data`.vendor_id in (198,204,202,196),(Vol.volumetric_weight *6/5),Vol.volumetric_weight ),S_current.`data`.shipment_weight.physical) AS billable_weight,
If(cast(if(S_current.`data`.vendor_id in (198,204,202,196),(Vol.volumetric_weight *6/5),Vol.volumetric_weight ) as int) > CAST(If(S_current.`data`.shipment_weight.physical IS NULL,0,S_current.`data`.shipment_weight.physical) AS int),'Volumetric','Physical') AS billable_weight_type,
b2CLogisticsRequest.cost_of_breach AS cost_of_breach,
S_current.`data`.value.value as shipment_value,
S_current.`data`.amount_to_collect.value as cod_amount_to_collect,
S_current.`data`.shipment_charge.total_charge.value as shipment_charge,
S_current.`data`.source_address.pincode as source_address_pincode,
S_current.`data`.destination_address.pincode as destination_address_pincode,
if(S_current.`data`.shipment_type NOT IN ('rvp'),S_current.`data`.destination_address.id,S_current.`data`.source_address.id) as customer_address_id,
--S_current.`data`.destination_address.id as customer_address_id,
--if(size(S_current.`data`.notes)>0,
--concat_ws("-",if(S_current.`data`.notes[0].type = 'CS Notes',S_current.`data`.notes[0].flag,
--if(S_current.`data`.notes[1].type = 'CS Notes',S_current.`data`.notes[1].flag,
--if(S_current.`data`.notes[2].type = 'CS Notes',S_current.`data`.notes[2].flag,NULL))),
--if(S_current.`data`.notes[1].type = 'CS Notes',S_current.`data`.notes[1].flag,
--if(S_current.`data`.notes[2].type = 'CS Notes',S_current.`data`.notes[2].flag,NULL)),
--if(S_current.`data`.notes[2].type = 'CS Notes',S_current.`data`.notes[2].flag,NULL)),NULL) AS cs_notes,
--if(size(S_current.`data`.notes)>0,
--concat_ws("-",if(S_current.`data`.notes[0].type = 'Hub Notes',S_current.`data`.notes[0].flag,
--if(S_current.`data`.notes[1].type = 'Hub Notes',S_current.`data`.notes[1].flag,
--if(S_current.`data`.notes[2].type = 'Hub Notes',S_current.`data`.notes[2].flag,NULL))),
--if(S_current.`data`.notes[1].type = 'Hub Notes',S_current.`data`.notes[1].flag,
--if(S_current.`data`.notes[2].type = 'Hub Notes',S_current.`data`.notes[2].flag,NULL)),
--if(S_current.`data`.notes[2].type = 'Hub Notes',S_current.`data`.notes[2].flag,NULL)),NULL) AS hub_notes,
notes.cs_notes as cs_notes,
notes.hub_notes as hub_notes,
S_current.`data`.assigned_address.id as fsd_assigned_hub_id,
if(S_current.`data`.shipment_type='rvp',-1,NULL) AS reverse_pickup_hub_id,
S_current.`data`.current_address.id as shipment_current_hub_id,
FKL_MH.mh_facility_id as shipment_origin_mh_facility_id,
If(lower(S_current.`data`.status) IN ('received_by_ekl','returned_to_ekl','received_by_merchant','returned_to_seller'),-1, Rev_FKL_MH.mh_facility_id) as shipment_destination_mh_facility_id,
--if(S_current.`data`.source_address.id is not null and S_current.`data`.source_address.id<>0,S_current.`data`.source_address.id,if(sourceFacility.hub_type='FKL_FACILITY',sourceFacility.source_facility_id,NULL)) as shipment_origin_facility_id,
S_current.`data`.source_address.id as shipment_origin_facility_id,
if(S_current.`data`.shipment_type IN ('rvp'),S_current.`data`.destination_address.id,NULL) as shipment_destination_facility_id,
--S_current.`data`.destination_address.id as shipment_destination_facility_id,
S_current.`data`.current_address.type as shipment_current_hub_type,
S_current.`data`.created_at AS shipment_created_at_datetime,
If(S_current.`data`.source_address.type like '%NON_FBF%',CAST(s_group.received_at_source_facility AS TIMESTAMP),CAST(S_current.`data`.created_at AS TIMESTAMP)) AS shipment_dispatch_datetime,
CAST(S_group.dispatched_to_vendor_time AS TIMESTAMP) AS vendor_dispatch_datetime,
CAST(S_current.updatedat AS TIMESTAMP) AS shipment_current_status_datetime,
CAST(S_group.received_time AS TIMESTAMP) AS shipment_first_received_at_datetime,
CAST(S_group.delivered_time AS TIMESTAMP) AS shipment_delivered_at_datetime,
CAST(S_group.last_delivered_time as TIMESTAMP) as shipment_last_delivered_at_datetime,
CAST(S_group.first_delivery_update_time as TIMESTAMP) as shipment_first_delivery_update_datetime,
CAST(S_group.last_delivery_update_time as TIMESTAMP) as shipment_last_delivery_update_datetime,
CAST(S_group.first_dispatched_to_merchant_time AS TIMESTAMP) as shipment_first_dispatched_to_merchant_datetime,
S_current.`data`.design_sla as logistics_promise_datetime,
S_current.`data`.actual_sla as shipment_actual_sla_datetime,
S_current.`data`.customer_sla as customer_promise_datetime,
S_current.`data`.design_sla as new_logistics_promise_datetime,
S_current.`data`.customer_sla as new_customer_promise_datetime,
CAST(S_group.received_last_time AS TIMESTAMP) AS shipment_last_receive_datetime,
CAST(S_group.dh_receive_time AS TIMESTAMP) AS fsd_first_dh_received_datetime,
CAST(S_group.last_dh_receive_time AS TIMESTAMP) AS fsd_last_dh_received_datetime,
CAST(S_group.first_received_pc_time AS TIMESTAMP) as shipment_first_received_pc_datetime,
CAST(S_group.last_received_pc_time AS TIMESTAMP) as shipment_last_received_pc_datetime,
CAST(S_group.first_ofd_time AS TIMESTAMP) AS fsd_first_ofd_datetime,
CAST(S_group.last_ofd_time AS TIMESTAMP) AS fsd_last_ofd_datetime,
CAST(S_group.first_rfp_time AS TIMESTAMP) as shipment_first_rfp_datetime,
CAST(S_group.last_rfp_time AS TIMESTAMP) as shipment_last_rfp_datetime,
CAST(S_group.first_picksheet_creation_time AS TIMESTAMP) as shipment_first_picksheet_creation_time,
CAST(S_group.last_picksheet_creation_time AS TIMESTAMP) as shipment_last_picksheet_creation_time,
CAST(S_group.first_rvp_pickup_time AS TIMESTAMP) AS shipment_first_rvp_pickup_time,
CAST(S_group.last_rvp_pickup_time AS TIMESTAMP) AS shipment_last_rvp_pickup_time,
CAST(S_group.rto_first_received_time AS TIMESTAMP) AS rto_first_received_time,
CAST(S_group.received_at_origin_facility AS TIMESTAMP) AS received_at_origin_facility_datetime,
CAST(rto_create_time as TIMESTAMP) AS rto_create_datetime,
CAST(rto_complete_time as TIMESTAMP) AS rto_complete_datetime,
CAST(3pl_first_ofd_time as TIMESTAMP) AS tpl_first_ofd_datetime,
CAST(3pl_last_ofd_time as TIMESTAMP) AS tpl_last_ofd_datetime,
CAST(first_mh_tc_receive_time as TIMESTAMP) AS first_mh_tc_receive_datetime,
CAST(last_mh_tc_receive_time as TIMESTAMP) AS last_mh_tc_receive_datetime,
NULL AS first_mh_tc_outscan_datetime,
NULL AS last_mh_tc_outscan_datetime,
NULL AS first_dh_outscan_datetime,
NULL AS last_dh_outscan_datetime,
S_current.`data`.shipment_weight.updated_by AS profiler_flag,
null AS profiled_hub_id
FROM
bigfoot_snapshot.dart_wsr_scp_ekl_shipment_4_view S_current  
inner join (select entityid AS shipment_id,
min(If(lower(`data`.status) = 'dispatched_to_vendor',updatedat,NULL)) as dispatched_to_vendor_time, 
min(If(lower(`data`.status) IN ('delivered','delivery_update'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as delivered_time,
max(If(lower(`data`.status) IN ('delivered','delivery_update'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as last_delivered_time,
min(If(lower(`data`.status) IN ('delivery_update'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as first_delivery_update_time,
max(If(lower(`data`.status) IN ('delivery_update'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as last_delivery_update_time,
min(If(lower(`data`.status) IN ('dispatched_to_merchant'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as first_dispatched_to_merchant_time,
min(If(lower(`data`.status) = 'undelivered_attempted' AND `data`.vendor_id NOT IN (200,207),`data`.updated_at,NULL)) as 3pl_first_ofd_time,
max(If(lower(`data`.status) = 'undelivered_attempted' AND `data`.vendor_id NOT IN (200,207),`data`.updated_at,NULL)) as 3pl_last_ofd_time,
min(If(lower(`data`.status) = 'out_for_delivery',updatedat,NULL)) as first_ofd_time,
max(If(lower(`data`.status) = 'out_for_delivery',updatedat,NULL)) as last_ofd_time,
min(If(lower(`data`.status) = 'ready_for_pickup',updatedat,NULL)) as first_rfp_time,
max(If(lower(`data`.status) = 'ready_for_pickup',updatedat,NULL)) as last_rfp_time,
min(If(lower(`data`.status) = 'pickup_addedtopickupsheet',updatedat,NULL)) as first_picksheet_creation_time,
max(If(lower(`data`.status) = 'pickup_addedtopickupsheet',updatedat,NULL)) as last_picksheet_creation_time,
min(If(lower(`data`.status) = 'pickup_out_for_pickup',updatedat,NULL)) as first_rvp_pickup_time,
max(If(lower(`data`.status) = 'pickup_out_for_pickup',updatedat,NULL)) as last_rvp_pickup_time,
min(If(lower(`data`.status) IN ('received','undelivered_not_attended','error'),updatedat,NULL)) as received_time,
min(If(lower(`data`.status) IN ('received','undelivered_not_attended','error') and `data`.current_address.type in ('DELIVERY_HUB','BULK_HUB'),updatedat,NULL)) as dh_receive_time,
max(If(lower(`data`.status) IN ('received','undelivered_not_attended','error') and `data`.current_address.type in ('DELIVERY_HUB','BULK_HUB'),updatedat,NULL)) as last_dh_receive_time,

min(If(lower(`data`.status) IN ('received','undelivered_not_attended','error') and `data`.current_address.type in ('PICKUP_CENTER'),updatedat,NULL)) as first_received_pc_time,
max(If(lower(`data`.status) IN ('received','undelivered_not_attended','error') and `data`.current_address.type in ('PICKUP_CENTER'),updatedat,NULL)) as last_received_pc_time,
min(if(lower(`data`.status) IN ('received') and `data`.shipment_type IN ('approved_rto','unapproved_rto'),updatedat,null)) as rto_first_received_time,
min(If(`data`.status IN ('Received','Undelivered_Not_Attended','Error') and `data`.current_address.type = 'MOTHER_HUB',updatedat,NULL)) as first_mh_tc_receive_time,
---min(If(lower(`data`.status) IN ('received','undelivered_not_attended','error') and `data`.current_address.type = 'MOTHER_HUB',updatedat,NULL)) as first_mh_tc_receive_time,
max(If(lower(`data`.status) IN ('received','undelivered_not_attended','error') and `data`.current_address.type = 'MOTHER_HUB',updatedat,NULL)) as last_mh_tc_receive_time,
-- min(If(lower(`data`.status) = 'sent' and `data`.current_address.type = 'MOTHER_HUB',updatedat,NULL)) as first_mh_tc_outscan_time,
-- max(If(lower(`data`.status) = 'sent' and `data`.current_address.type = 'MOTHER_HUB',updatedat,NULL)) as last_mh_tc_outscan_time,
-- min(If(lower(`data`.status) = 'sent' and `data`.current_address.type IN ('DELIVERY_HUB','BULK_HUB'),updatedat,NULL)) as first_dh_outscan_time,
-- max(If(lower(`data`.status) = 'sent' and `data`.current_address.type IN ('DELIVERY_HUB','BULK_HUB'),updatedat,NULL)) as last_dh_outscan_time,
max(If(lower(`data`.status) IN ('received','error','undelivered_not_attended'),updatedat,NULL)) as received_last_time,
sum(if(`data`.status in ('Out_For_Delivery','out_for_delivery'),1,0)) as fsd_number_of_ofd_attempts,
sum(If(`data`.status = 'pickup_out_for_pickup' and `data`.source_address.type like '%NON_FBF%',1,0)) as out_for_pickup_attempts,
min(If(`data`.shipment_type like '%rto',updatedat,null)) as rto_create_time,
min(If(lower(`data`.status) in ('returned_to_seller','received_by_merchant','delivered','delivery_update'),updatedat,null)) as rto_complete_time,
sum(if(`data`.status = 'PICKUP_Out_For_Pickup',1,0)) as shipment_rvp_pk_number_of_attempts,
min(If(`data`.status = 'expected',updatedat,null)) as received_at_source_facility,
min(If(`data`.status = 'Expected' and `data`.current_address.type = 'DELIVERY_HUB',updatedat,NULL)) AS reverse_pickup_hub_time,
min(If(`data`.status = 'pickup_complete',updatedat,null)) as received_at_origin_facility
from bigfoot_journal.dart_wsr_scp_ekl_shipment_4
group by entityid
) S_group
on (S_group.shipment_id=S_current.entityid)
LEFT OUTER JOIN (SELECT `data`.pincode as src_pincode, `data`.zone as src_zone, `data`.local_territory as src_lt,`data`.city as src_city,`data`.state as src_state from bigfoot_snapshot.dart_fki_scp_ekl_geo_1_1_view_total) geo_src ON If(S_current.`data`.shipment_type = 'rvp',S_current.`data`.destination_address.pincode,S_current.`data`.source_address.pincode) = geo_src.src_pincode
LEFT OUTER JOIN (SELECT `data`.pincode as dest_pincode, `data`.zone as dest_zone, `data`.local_territory as dest_lt,`data`.city as dest_city,`data`.state as dest_state from bigfoot_snapshot.dart_fki_scp_ekl_geo_1_1_view_total) geo_dest ON If(S_current.`data`.shipment_type = 'rvp',S_current.`data`.source_address.pincode,S_current.`data`.destination_address.pincode) = geo_dest.dest_pincode

--LEFT OUTER JOIN (SELECT `data`.pincode as src_pincode, `data`.zone as src_zone, `data`.local_territory as src_lt,`data`.city as src_city from bigfoot_snapshot.dart_fki_scp_ekl_geo_1_1_view_total) geo_src ON S_current.`data`.source_address.pincode = geo_src.src_pincode
--LEFT OUTER JOIN (SELECT `data`.pincode as dest_pincode, `data`.zone as dest_zone, `data`.local_territory as dest_lt,`data`.city as dest_city from bigfoot_snapshot.dart_fki_scp_ekl_geo_1_1_view_total) geo_dest ON (S_current.`data`.destination_address.pincode = geo_dest.dest_pincode)
LEFT OUTER JOIN 
-- (SELECT vendor_tracking_id, volumetric_weight as vol_weight 
-- 	FROM bigfoot_external_neo.scp_ekl__fc_profiler_volumetric_estimate_final_hive_fact--bigfoot_external_neo.scp_ekl__fsn_shipment_weight_volume_l1_fact 
-- 	WHERE vendor_tracking_id <> 'not_assigned' AND vendor_tracking_id IS NOT NULL
--     --Group By vendor_tracking_id
--     ) Vol ON S_current.`data`.vendor_tracking_id = Vol.vendor_tracking_id
-- COMMENTED FOR DUP ISSUE
--bigfoot_external_neo.scp_ekl__fc_profiler_volumetric_estimate_final_hive_fact Vol on (S_current.entityid = Vol.shipment_id)
-- CREATED TEMP TABLE FOR DUP ISSUE
(SELECT shipment_id,volumetric_weight FROM bigfoot_external_neo.scp_ekl__fc_profiler_volumetric_estimate_final_hive_fact
group by shipment_id,volumetric_weight) 
Vol on (S_current.entityid = Vol.shipment_id)
-- END
left outer join
bigfoot_common.ekl_fkl_facility_mother_hub_mapping FKL_MH ON S_current.`data`.source_address.id = FKL_MH.fkl_facility_id
left outer join
bigfoot_common.ekl_fkl_facility_mother_hub_mapping Rev_FKL_MH ON S_current.`data`.destination_address.id = Rev_FKL_MH.fkl_facility_id
LEFT OUTER JOIN
(SELECT refid AS shipment_id,
`data`.merchant_id AS merchant_id,
`data`.vas_ids[0] AS vas_type,
max(`data`.merchant_reference_id) AS merchant_reference_id,
`data`.cost_of_breach.value AS cost_of_breach,
IF(size(`data`.logistics_service_offering)>=2,`data`.logistics_service_offering[1],NULL) as service_tier,
IF(size(`data`.logistics_service_offering)>=3,`data`.logistics_service_offering[2],NULL) as surface_mandatory_flag,
IF(`data`.cost_of_breach.value >= 20000, "SDD", IF(`data`.cost_of_breach.value >= 15000, 'SDD_Pilot', IF(`data`.cost_of_breach.value >= 1000, 'NDD', IF(`data`.cost_of_breach.value >= 100, 'NDD_Pilot', 'Normal')))) AS shipment_priority_flag
FROM bigfoot_snapshot.dart_wsr_scp_ekl_b2clogisticsrequest_1_1_view LATERAL VIEW explode(`data`.ekl_reference_ids) reference_id AS refid
group by refid,
`data`.merchant_id,
`data`.vas_ids,
`data`.cost_of_breach.value,
`data`.logistics_service_offering) b2CLogisticsRequest ON S_current.entityid = b2CLogisticsRequest.shipment_id
LEFT OUTER JOIN
bigfoot_external_neo.scp_ekl__ekl_notes_fact notes ON (S_current.`data`.vendor_tracking_id=notes.vendor_tracking_id AND notes.vendor_tracking_id NOT IN ('not_assigned'))
LEFT OUTER JOIN 
(select associated_id
from bigfoot_snapshot.dart_wsr_scp_ekl_shipment_4_view LATERAL VIEW explode(`data`.associated_shipment_ids) associated_shipment_id AS associated_id
where `data`.shipment_type IN ('approved_rto','unapproved_rto','rvp') and lower(`data`.status) NOT IN ('reshipped')) T1 ON S_current.entityid=T1.associated_id
WHERE
(FKL_MH.serviceability <> 'Large' OR FKL_MH.serviceability IS NULL) AND
(Rev_FKL_MH.serviceability <> 'Large' OR Rev_FKL_MH.serviceability IS NULL) AND
S_current.`data`.current_address.type <> 'BULK_HUB';