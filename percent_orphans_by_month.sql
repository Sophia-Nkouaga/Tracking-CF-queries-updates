/* pick the recent sales made in the last 60 days */

with recent_sales_name as (
		  select  distinct sa.sa_name,
		  		  min(sa.first_sale_date) as first_sale_date,
		    	  max(sa.last_sale_date) as last_sale_date
		  from (
			 /* sale_agent = application by in admin + alms  */
			  select nvl(cd.application_by_contractor_id, cd.assigned_to_contractor_id) as sa_id,
			  		 lower(nvl(cd.application_by_contractor_name, cd.assigned_to_contractor_name)) as sa_name,
					 min(date(soi.handover_at_utc)) as first_sale_date,
					 max(date(soi.handover_at_utc)) as last_sale_date
					 
			  from powerhub_reporting.eea_loan_account_details ld
			  left join powerhub_reporting.eea_customer_details cd ON cd.customer_id = ld.customer_id
			  left join powerhub_reporting.eea_sales_order_items soi ON soi.customer_id = ld.customer_id 
			  where  soi.account_type = 'CREDIT' 
			  		and ld.state like 'active' 
			  		and soi.sale_type like 'FIRST SALE' 
			  		and soi.sales_order_submitted_by_type not like 'User'
			  group by 1,2
	  
			/* sale_agent in fenixdb, missing in admin + alms  */			
			  union all
						
			  select aa.powerhub_entry_id as sa_id,
			   		 lower(sd.associator_name) as sa_name,
					 min(date(sd.date_fulfilled_utc)) as first_sale_date,
					 max(date(sd.date_fulfilled_utc)) AS last_sale_date   		 	   	
			  from powerhub_reporting.reporting_sales_details sd 
			  left join (select *,
								row_number() over (partition by powerhub_entry_id order by inserted_at_utc) rn 
						 from powerhub_reporting.eea_fenixdb_links 
						 where  powerhub_entry_type = 'Contractor' 
						   ) as aa on aa.fenixdb_entry_id = sd.associator_id and aa.rn = 1  -- links sales_agent_id from fenixdb to powerhub		
			  -- where sd.account_type = 'CREDIT'
			  group by 1,2
		
			  ) as sa 
			  
			group by 1 ),


recent_sales_ID as ( 
       select 
       		cd.application_by_contractor_id as associator_id,
        	min(date(soi.handover_at_utc)) as first_sale_at,
        	max(date(soi.handover_at_utc)) as last_sale_at     	
       from powerhub_reporting.eea_sales_order_items soi
       left join powerhub_reporting.eea_customer_details cd on cd.customer_id = soi.customer_id   
       where soi.account_type = 'CREDIT' and  soi.master_loan_item = 1
       group by 1  ),

          
					
/* reassigned contractor */	
	
reassigned as  (select reas.reassigned_agent_name,
					   reassigned_agent_id,
					   reas.customer_id,
					   reas.reassigned_agent_role,
					   min(reas.first_sale_date) as first_sale_at,
					   max(reas.last_sale_date) as last_sale_at
					 
								  
			    from (	select cd.adopted_by_contractor_id as reassigned_agent_id,
					  		   oc.to_contractor_name as reassigned_agent_name,
					  		   oc.to_contractor_role_name as reassigned_agent_role,
					  		   cd.customer_id,
					  		   coalesce(rs.first_sale_date, rsd.first_sale_at) as first_sale_date,
					  		   coalesce(rs.last_sale_date, rsd.last_sale_at) as last_sale_date
									   
						from powerhub_reporting.eea_customer_details cd
						left join powerhub_reporting.eea_orphaned_customers oc on oc.to_contractor_id = cd.adopted_by_contractor_id and oc.customer_id = cd.customer_id
						left join recent_sales_name rs on rs.sa_name = lower(oc.to_contractor_name ) 
						left join recent_sales_ID rsd on rsd.associator_id = cd.adopted_by_contractor_id
										
					) as reas 
			group by 1,2,3,4
				)


SELECT *

FROM

( 

/* fenixdb */

select
	/* sale info */
	monthly.report_date as report_date, --1	
	'fenixdb' as source, --2
	'paygo' loan_type,  --3
    'in_repayment' as loan_state, --4
    ctx.iso_3_abbreviation country, --5
 
 	case 
 		 when fpa.loan_id is not null and fpa.person_responsible not like 'Collection_Champions' then 'non_orphan'
 		 when oc.customer_id is not null then 'non_orphan'
 		 when rs.last_sale_date  >= monthly.report_date - interval '60 Day' then 'non_orphan'
 	else 'orphan' end as "orphaned_status", --6
	
	/* portfolio info */
	(round(monthly.days_elapsed/30.5,0)) as months_on_book, --7
	
	count(distinct monthly.loan_id::varchar) as  count_loan_id,
    count(distinct monthly.customer_id::varchar) as count_customer_id	

from sensitive.finance_global_finance_report_monthly monthly
	
left join (select distinct customer_id, created_at_utc from powerhub_reporting.eea_orphaned_customers) oc on oc.customer_id = monthly.customer_id and date(oc.created_at_utc) <= monthly.report_date

left join powerhub_reporting.reporting_sales_details sd on monthly.account_id = sd.account_id and monthly.loan_id = sd.loan_id

left join (select distinct loan_id,person_responsible from analysts_inputs.market_sources_ug_portfolio) fpa on monthly.loan_id = fpa.loan_id 


left join recent_sales_name rs on lower(rs.sa_name) = lower(sd.associator_name)



left join powerhub_reporting.eea_sales_order_items so on sd.fulfillment_source = 'Powerhub' 
								   					   and monthly.loan_id::VARCHAR(20) = so.loan_account_id
								   					   and so.master_loan_item = 1

/* get the customer details */
left join powerhub_reporting.eea_customer_details cd on cd.customer_id = monthly.customer_id
left join sensitive.customers_sensitive cs on cs.customer_id = cd.customer_id
left join reassigned rea on rea.customer_id = cd.customer_id
left join powerhub_reporting.eea_countries ctx on sd.country_of_sale = ctx.name

    ---- some filters ----
where monthly.active like 'active'  and country_of_sale not in ('Senegal', 'Kenya')  and monthly.report_date >= CURRENT_DATE - interval '365 Day'
group by 1,2,3,4,5,6,7


UNION ALL

/* admin */

select

	/* sale info */
	monthly.snapshot_at as report_date, --1
	'admin' as source, --2
	lad.loan_type, --3
	monthly.state as loan_state, --4
    ctx.iso_3_abbreviation as country, --5
    
 	case 
 		 when fpa.loan_id is not null and fpa.person_responsible not like 'Collection_Champions' then 'non_orphan'
 		 when oc.customer_id is not null then 'non_orphan'
 		 when NVL(rs.last_sale_date, rsd.last_sale_at) >= monthly.snapshot_at - interval '60 Day' then 'non_orphan'
 	else 'orphan' end as "orphaned_status", --6
	
	(monthly.months_on_book) as months_on_book, --7
	count(distinct monthly.loan_account_id) as count_loan_id,
    count(distinct monthly.customer_id) as count_customer_id

	
	
from analysts_inputs.credit_loan_accounts_monthly monthly

left join (select distinct customer_id, created_at_utc from powerhub_reporting.eea_orphaned_customers) oc on oc.customer_id = monthly.customer_id and date(oc.created_at_utc) <= monthly.snapshot_at

left join (select distinct loan_id,person_responsible from analysts_inputs.market_sources_ug_portfolio) fpa on monthly.loan_account_id = fpa.loan_id 

left join powerhub_reporting.eea_loan_account_details lad on lad.loan_account_id = monthly.loan_account_id 
										 and lad.customer_id = monthly.customer_id
										 and lad.last_benchmark_date = monthly.snapshot_at
										 
left join powerhub_reporting.eea_sales_order_items so on so.customer_id = monthly.customer_id and so.loan_account_id = monthly.loan_account_id 
left join powerhub_reporting.eea_sales_cases sc on sc.sales_case_id = so.sales_case_id and sc.finalized = 1 and so.master_loan_item = 1

left join analysts_inputs.vw_global_sales_channel_region_mapping crm on so.sales_order_item_id = crm.order_item_id 
														and NVL(so.loan_portfolio_id,'') = crm.account_id 
														and NVL(so.loan_account_id,'') = crm.loan_id 
														and crm.source = 'Solarhub'
 
left join powerhub_reporting.eea_countries ctx ON so.country = ctx.iso_2_abbreviation
left join powerhub_reporting.eea_customer_details cd on cd.customer_id = lad.customer_id

left join (select * 
		   from powerhub_reporting.eea_loan_reschedule_requests  
		   where resultant_loan_account_id is not null
		   ) resch on resch.resultant_loan_account_id = lad.loan_account_id   
 
left join sensitive.customers_sensitive cs on lad.customer_id = cs.customer_id 

left join recent_sales_name rs on rs.sa_name = NVL(lower(crm.sales_agent_name),lower(cd.application_by_contractor_name))

left join recent_sales_ID rsd on rsd.associator_id = cd.application_by_contractor_id

left join reassigned rea on rea.customer_id = cd.customer_id

where monthly.state in ('maintained','exceeded_grace_period','exceeded_loan_period','in_grace_period')
	  -- and lad.source = 'admin'
	  and ctx.iso_2_abbreviation not like 'DE'
	  and monthly.snapshot_at >= CURRENT_DATE - interval '365 Day'

group by 1,2,3,4,5,6,7
	  
	  	  
UNION ALL


/* alms */

select

	/* sale info */
	monthly.report_date, --1
	'alms' as source, --2
	'paygo' as loan_type, --3
    monthly.loan_state as loan_state,  --4
    ctx.iso_3_abbreviation country, --5
    
   case 
 		 when fpa.loan_id is not null and fpa.person_responsible not like 'Collection_Champions' then 'non_orphan'
 		 when oc.customer_id is not null then 'non_orphan'
 		 when NVL(rs.last_sale_date, rsd.last_sale_at) >= monthly.report_date - interval '60 Day' then 'non_orphan'
 	else 'orphan' end as "orphaned_status", --6
	
	/* portfolio info */
	(round(monthly.days_elapsed/30.5,0)) as months_on_book, --7
  	
	count(distinct monthly.loan_id) as count_loan_id,
    count(distinct monthly.customer_id) as count_customer_id
	
	
from sensitive.finance_global_finance_report_monthly_alms as monthly

left join (select distinct customer_id, created_at_utc from powerhub_reporting.eea_orphaned_customers) oc on oc.customer_id = monthly.customer_id and date(oc.created_at_utc) <= monthly.report_date

left join (select distinct loan_id,person_responsible from analysts_inputs.market_sources_ug_portfolio) fpa on monthly.loan_id = fpa.loan_id 
												   
left join powerhub_reporting.eea_loan_account_details lcd on lcd.loan_account_id = monthly.loan_id
left join powerhub_reporting.eea_customer_details cd on cd.customer_id = monthly.customer_id
left join powerhub_reporting.eea_sales_order_items so on so.customer_id = monthly.customer_id and so.loan_account_id = monthly.loan_id
								   
inner join powerhub_reporting.eea_sales_cases sc on sc.sales_case_id = so.sales_case_id 
							  					 and sc.finalized = 1 
							  					 and so.master_loan_item = 1


left join powerhub_reporting.eea_countries ctx ON so.country = ctx.iso_2_abbreviation

left join sensitive.finance_global_finance_calculations_monthly_alms c on monthly.account_id= c.account_id 
														   			   and monthly.loan_id = c.loan_id 
														   			   and c.report_date = monthly.report_date 
        						
left join sensitive.customers_sensitive cs on cs.customer_id = monthly.customer_id
left join reassigned rea on rea.customer_id = cd.customer_id

left join analysts_inputs.vw_global_sales_channel_region_mapping crm on so.sales_order_item_id = crm.order_item_id 
																	   and NVL(so.loan_portfolio_id,'') = crm.account_id 
																	   and NVL(so.loan_account_id,'') = crm.loan_id 
																	   and crm.source = 'Solarhub'


left join recent_sales_name rs on lower(rs.sa_name) = lower(coalesce(cd.application_by_contractor_name, crm.sales_agent_name, sales_order_submitted_by_name))
left join recent_sales_ID rsd on rsd.associator_id = cd.application_by_contractor_id


    ---- some filters ----
where monthly.loan_state like 'active'
      and lcd.initial_amount > 0
      --- and lcd.source = 'alms'
      and monthly.report_date >= CURRENT_DATE - interval '365 Day'
     
group by 1,2,3,4,5,6,7  

) as main
