create table jb4gov.hd1103xfm as
select allhd.*,
case when allhd.result = 'Canvassed' and allhd.jbid is null then 'did not say' end as no,
row_number() over () as nrum
from (
  (Select vanid, client_id, campaign_id, phone_number, call_id, date::date as datecanvassed, case when status = 'Human' then 'Canvassed' else status end as result, jbid, ev, null as absent, null as yard, null as ride from jb4gov.raw_15593) union
(Select vanid, client_id, campaign_id, phone_number, call_id, date::date as datecanvassed, case when status = 'Human' then 'Canvassed' else status end as result, jbid, ev, null as absent, null as yard, null as ride from jb4gov.raw_15594) union
(Select vanid, client_id, campaign_id, phone_number, call_id, date::date as datecanvassed, case when status = 'Human' then 'Canvassed' else status end as result, jbid, ev, null as absent, null as yard, null as ride from jb4gov.raw_15598) union
(Select vanid, client_id, campaign_id, phone_number, call_id, date::date as datecanvassed, case when status = 'Human' then 'Canvassed' else status end as result, jbid, ev, null as absent, null as yard, null as ride from jb4gov.raw_15600) union
(Select vanid, client_id, campaign_id, phone_number, call_id, date::date as datecanvassed, case when status = 'Human' then 'Canvassed' else status end as result, jbid, ev, null as absent, null as yard, null as ride from jb4gov.raw_14563) union
(Select vanid, client_id, campaign_id, phone_number, call_id, date::date as datecanvassed, case when status = 'Human' then 'Canvassed' else status end as result, jbid, null as ev, absent, null as yard, null as ride from jb4gov.raw_15087) union
(Select vanid, client_id, campaign_id, phone_number, call_id, date::date as datecanvassed, case when status = 'Human' then 'Canvassed' else status end as result, jbid, null as ev, absent, yard, ride from jb4gov.raw_15083) union
(Select vanid, client_id, campaign_id, phone_number, call_id, date::date as datecanvassed, case when status = 'Human' then 'Canvassed' else status end as result, jbid,  ev, null as absent, yard, ride from jb4gov.raw_14558)) allhd
