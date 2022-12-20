with journey as (
select *, 
lag(data.location.lat) OVER(PARTITION BY data.id ORDER BY data.location.at) AS end_lat, 
lag(data.location.lng) OVER(PARTITION BY data.id ORDER BY data.location.at) AS end_lng,
FROM `fif-fpay-reconciliation-test.pay_recon_cl.cars_events_table` 
WHERE event= 'update'),

operation_period as(
  select data.id, data.start, data.finish, organization_id from `fif-fpay-reconciliation-test.pay_recon_cl.cars_events_table` tb where tb.on = 'operating_period'
),

final_data as (
select data.id id_vehicle, data.location.at, op.start, op.finish, op.id,
ST_DISTANCE(ST_GEOGPOINT(CAST(data.location.lng AS FLOAT64), CAST(data.location.lat AS FLOAT64)),
            ST_GEOGPOINT(CAST(end_lng AS FLOAT64), CAST(end_lat AS FLOAT64))) dist_recorrida 
            from journey tb
inner join operation_period op on tb.organization_id = op.organization_id
where 
data.location.at between start and finish 
order by op.id)

select id operation_id, id_vehicle, sum(dist_recorrida) dist_total from final_data
group by id_vehicle, id;
