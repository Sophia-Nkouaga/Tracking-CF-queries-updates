WITH current_month AS (
/*
WITH RECURSIVE date_series (generated_date) AS (
    SELECT date_trunc('month', current_date)::date AS generated_date
    UNION ALL
    SELECT (generated_date + interval '1 day')::date  -- Ensure the result is cast to date
    FROM date_series
    WHERE generated_date + interval '1 day' < current_date*/
	
WITH numbers AS (
    SELECT 
	ROW_NUMBER() OVER () - 1 AS n
    FROM powerhub_reporting.reporting_person_demographics
    LIMIT 31
),
date_series AS (
    SELECT 
        (date_trunc('month', current_date - INTERVAL '1 day') + n * INTERVAL '1 day')::date AS generated_date
    FROM numbers
    WHERE (date_trunc('month', current_date - INTERVAL '1 day') + n * INTERVAL '1 day')::date 
          <= (current_date - INTERVAL '1 day')::date
), 
    
    payments AS (
			SELECT
				customer_id::varchar,
				loan_id::varchar,
				date(added_at_utc) AS payment_date,
				sum(portion_interest::float + portion_principal::float)/100 as amount_paid
			FROM
				powerhub_reporting.eea_alms_loan_transactions
				where transaction_type in ('Loan Payment', 'Loan Transaction Reversal')
				and date_trunc('month', added_at_utc) = date_trunc('month', CURRENT_DATE)
				and loan_id like 'ug%'
				group by 1,2,3
			
			UNION ALL
			
			SELECT
				customer_id::varchar,
				loan_id::varchar,
				date(added_at_utc) AS payment_date,
				sum(portion_interest::float + portion_principal::float) as amount_paid
				
			FROM
				powerhub_reporting.reporting_loan_transactions
				where transaction_type in ('Payment', 'Reversal','Transfer', 'Automatic Arrears Payment','Refund')
			    and date_trunc('month', added_at_utc) = date_trunc('month', CURRENT_DATE)
				and country = 'UG'
				group by 1,2,3
				
			UNION ALL
			
			SELECT
				customer_id::varchar,
				loan_account_id::varchar as loan_id,
				date(payment_at) AS payment_date,
				sum(amount::float)/100 as amount_paid
				
			FROM
				powerhub_reporting.eea_payment_bookings
				where transaction_type in ('PaymentChargeTransaction', 'RescueChargeTransaction')
	            and date_trunc('month', payment_at) = date_trunc('month', CURRENT_DATE)
				and loan_account_id like 'ug%'
				group by 1,2,3	
		),
		
	latest_ldm_fenix AS (
    SELECT ldm.loan_id,
           ldm.customer_id,
           sh.loan_state as loan_state,
           ldm.benchmark_date,
           ldm.daily_rate,
           ldm.in_repayment,
           ldm.days_elapsed,
           loan_contract_end_date,
           sd.product_type as product,
           sd.associator_manager_name AS team_lead,
           sd.country_of_sale as country,
           (CASE 
           WHEN sd.country_of_sale IN ('Uganda', 'Mozambique', 'Côte d''Ivoire') THEN pd.district
           ELSE pd.region 
           END) as region,
           ROW_NUMBER() OVER (PARTITION BY ldm.loan_id ORDER BY ldm.benchmark_date DESC) AS rn
    FROM powerhub_reporting.reporting_loan_daily_metrics ldm
    LEFT JOIN powerhub_reporting.reporting_loan_current_details sh on ldm.loan_id = sh.loan_id 
    LEFT JOIN sensitive.finance_global_finance_report_monthly l ON ldm.loan_id = l.loan_id AND (DATE_TRUNC('MONTH', ldm.benchmark_date) - INTERVAL '1 day')::DATE = DATE(l.report_date)
    LEFT JOIN powerhub_reporting.reporting_sales_details sd ON (ldm.loan_id = sd.loan_id AND ldm.account_id = sd.account_id AND ldm.days_elapsed > sd.introductory_period)
    LEFT JOIN powerhub_reporting.reporting_person_demographics pd ON sd.customer_id_fenixdb = pd.person_id 
    WHERE date(ldm.benchmark_date) BETWEEN  
    date_trunc('month', current_date - INTERVAL '1 month')::date  
    AND (current_date - INTERVAL '1 day')::date
	and sd.country_of_sale = 'Uganda'
    
),

 
    latest_bench_fenix as ( 
    SELECT loan_id,
           customer_id,
           loan_state,
           benchmark_date,
           daily_rate,
           coalesce(in_repayment, 'FALSE') as in_repayment,
           days_elapsed,
           loan_contract_end_date,
           product,
           team_lead,
           country,
           region,
           rn,
		   generated_date as report_date
    FROM   latest_ldm_fenix l, date_series ds
	WHERE  rn = 1
	
	),
	
	latest_ldm_alms AS (
    SELECT cast(alp.loan_id as varchar) as loan_id,
           cast(alp.customer_id as varchar) as customer_id,
           alp.loan_state as loan_state,
           alp.benchmark_date,
           alp.daily_rate/100 as daily_rate,
           alp.days_elapsed,
           l.loan_contract_end_date,
           soi.product_line_name as product,
           ph.manager_name as Team_Lead,
           cd.country,
           case  when cd.country in ('MZ', 'CI') then cd.area2      
                  else cd.area1 end  as region,
           ROW_NUMBER() OVER (PARTITION BY alp.loan_id ORDER BY alp.benchmark_date DESC) AS rn
    FROM powerhub_reporting.eea_alms_loan_performance_benchmarks alp
    
    LEFT JOIN sensitive.finance_global_finance_report_monthly_alms l ON alp.loan_id = l.loan_id AND (DATE_TRUNC('MONTH', alp.benchmark_date) - INTERVAL '1 day')::DATE = DATE(l.report_date)
    LEFT JOIN powerhub_reporting.eea_loan_account_details lad on lad.loan_account_id = alp.loan_id and lad.customer_id = alp.customer_id
        AND lad.source = 'alms'
	LEFT JOIN powerhub_reporting.eea_customer_details cd on cd.customer_id = alp.customer_id
    LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and alp.benchmark_date = ph.benchmark_date
	LEFT JOIN powerhub_reporting.eea_sales_order_items soi on (lad.sales_case_id = soi.sales_case_id
                                                and alp.loan_id = soi.loan_account_id) 
	WHERE date(alp.benchmark_date) BETWEEN  
    date_trunc('month', current_date - INTERVAL '1 month')::date  
    AND (current_date - INTERVAL '1 day')::date
	and cd.country = 'UG'
),

 
    latest_bench_alms as ( 
    SELECT loan_id,
           customer_id,
           loan_state,
           benchmark_date,
           daily_rate,
           days_elapsed,
           loan_contract_end_date,
           product,
           team_lead,
           country,
           region,
           rn,
		   generated_date as report_date
    FROM   latest_ldm_alms l, date_series ds
	WHERE  rn = 1
	
	)

	
	  

		/*FENIX*/
		
		Select 
		'fenix' as company,
		'current month' as period,
        ll.report_date as report_date,
        ds.generated_date as generated_date,
        l.loan_contract_end_date,
        ll.loan_contract_end_date as new_loan_contract_end,
        ldm.benchmark_date as benchmark_date,
		coalesce(CAST(ldm.loan_id AS VARCHAR), cast(ll.loan_id as varchar)) as loan_id,
        coalesce(CAST(ldm.customer_id AS varchar), cast(ll.customer_id as varchar)) as customer_id,
        coalesce(ldm.daily_rate,ll.daily_rate) as daily_rate,
		sh.loan_state as loan_state,
		ll.loan_state as new_loan_state,
		ldm.in_repayment,
		ll.in_repayment as latest_in_repayment,
		crh.cancellation_reason,
		coalesce(sd.product_type, ll.product) as product,
        coalesce(sd.associator_manager_name, ll.team_lead) AS team_lead,
        coalesce(sd.country_of_sale, ll.country) as country,
        coalesce((CASE 
            WHEN sd.country_of_sale IN ('Uganda', 'Mozambique', 'Côte d''Ivoire') THEN pd.district
            ELSE pd.region 
            END),ll.region) as region,
        p.payment_date,
        p.amount_paid,    
    -- Calcular expected_all_mtd
    CASE 
    WHEN ll.loan_contract_end_date > COALESCE(ldm.benchmark_date, ll.report_date, ds.generated_date) THEN 
        CAST(COALESCE(ldm.daily_rate, ll.daily_rate) AS DOUBLE PRECISION)
    ELSE 
        CASE 
            WHEN COALESCE(ldm.in_repayment, ll.in_repayment) = 'TRUE' THEN 
                SUM(
                    CASE 
                        WHEN COALESCE(ldm.days_elapsed, ll.days_elapsed) <= sd.loan_duration + sd.introductory_period 
                        THEN CAST(COALESCE(ldm.daily_rate, ll.daily_rate) AS DOUBLE PRECISION)
                        ELSE 0 
                    END
                )
            ELSE 0 
        END
    END AS expected_all_mtd,

    -- Calcular expected_in_month_in_repayment
    CASE 
        WHEN ldm.in_repayment = 'TRUE' THEN 
            SUM(
                CASE 
                    WHEN ldm.days_elapsed <= sd.loan_duration + sd.introductory_period 
                    THEN CAST(ldm.daily_rate AS DOUBLE PRECISION)
                    ELSE 0 
                END
            )
        ELSE 0 
    END AS expected_in_month_in_repayment

						
		FROM 
    date_series ds
	LEFT JOIN latest_bench_fenix ll ON ll.rn = 1  and ll.report_date = ds.generated_date 
    LEFT JOIN powerhub_reporting.reporting_loan_daily_metrics ldm ON ldm.loan_id = ll.loan_id AND date(ldm.benchmark_date) = ds.generated_date 
    LEFT JOIN powerhub_reporting.reporting_sales_details sd ON (ldm.loan_id = sd.loan_id AND
                                             ldm.account_id = sd.account_id AND
                                             /*Exclude days prior to intro period */
                                             ldm.days_elapsed > sd.introductory_period)
    
    LEFT JOIN powerhub_reporting.reporting_person_demographics pd ON sd.customer_id_fenixdb = pd.person_id 
    LEFT JOIN payments p on COALESCE(ldm.loan_id, ll.loan_id) = p.loan_id and COALESCE(ldm.benchmark_date, ds.generated_date) = p.payment_date 
    LEFT JOIN powerhub_reporting.reporting_loan_current_details sh on COALESCE(ldm.loan_id, ll.loan_id) = sh.loan_id 
	LEFT JOIN powerhub_reporting.reporting_loan_cancellation_reason_history crh on ldm.loan_id = crh.loan_id and date(ldm.benchmark_date) = date(crh.change_recorded_at_utc)
	LEFT JOIN sensitive.finance_global_finance_report_monthly l on ldm.loan_id = l.loan_id and (DATE_TRUNC('MONTH',  COALESCE(ldm.benchmark_date, ll.report_date)) - INTERVAL '1 day')::DATE = date(l.report_date)
	WHERE date(ll.report_date) BETWEEN date_trunc('month', current_date - INTERVAL '1 day')::date AND (current_date - INTERVAL '1 day')::date
	and (sd.country_of_sale = 'Uganda' or ll.country = 'Uganda')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
		
		UNION ALL
		
    /*MOBISOL*/
	
	Select
	'paygee' as company,
	'current month' as period,
    date(dl.snapshot_at) as report_date,
	date(dl.snapshot_at) as generated_date,
	lad.loan_contract_end_date,
	lad.loan_contract_end_date as new_loan_contract_end,
	date(dl.snapshot_at) as benchmark_date,
	cast (dl.loan_account_id as VARCHAR) as loan_id,
	cast(dl.customer_id as varchar) as customer_id,
	round(NVL(soi.daily_rate/100,(NVL(lad.initial_installment,0)/100)/lad.installment_period_days)) as daily_rate,
	lower(dl.state) as loan_state,
	lower(dl.state) as new_loan_state,
	true as in_repayment,
	true as latest_in_repayment,
	lad.cancel_comment as cancellation_reason,
	soi.product_line_name as product,
	(case when cd.country = 'TZ' then split_part(rbe_list_per_area_v4_20240910_2(cd.area2),'~',1)
	else ph.manager_name end) as team_lead,
    cd.country,
    case when cd.country = 'TZ' then   split_part(rbe_list_per_area_v4_20240910_2(cd.area2),'~',2)
        when cd.country = 'KE' then split_part(udf_ke_stock_points(cd.hub_id),'~',2) 
        when cd.country = 'UG' then loc.region
		when cd.country in ('MZ', 'CI') then cd.area2
    else cd.area1 end as region,
    p.payment_date,
    p.amount_paid,  
	
		-- Calcular expected_all_mtd
    SUM(
    CASE
    WHEN dl.state = 'rescheduled' THEN
      0
    WHEN cd.country <> 'KE'
      AND
      dl.state = 'canceled' THEN
      0
    WHEN dl.state = 'paid_off' THEN
      0
    WHEN lad.loan_type = 'paygo' THEN (
      CASE
      WHEN dl.snapshot_at - date(lad.handover_at_utc) < (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) THEN
        0
        /*When customer is in the first month then only take days elapsed - introperiod */
      WHEN (
          dl.snapshot_at - date(lad.handover_at_utc) - (
          CASE
          WHEN cd.country = 'KE'
            AND
            (
              lad.loan_type <> 'paygo'
              OR
              dl.snapshot_at < '2022-03-21'
            )
            THEN
            30.5
          WHEN lad.loan_type = 'paygo'
            AND
            cd.country = 'KE' THEN
            7
          WHEN cd.country IN ('TZ',
                              'RW' )
            AND
            lad.loan_type <> 'paygo' THEN
            30.5
            ELSE soi.down_payment_days
          END)
        )
        <= extract(day FROM dl.snapshot_at) THEN
        round(nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
        /*When customer is beyond their original loan duration + days expected in the last month then expect 0*/
      WHEN dl.snapshot_at - date(lad.handover_at_utc) + (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) > (nvl(soi.loan_duration,lad.installment_periods * lad.installment_period_days)) + extract(day FROM dl.snapshot_at) THEN
        0
        /*When customer is beyond their original loan duration but payoff within the last month then days in the month
    are expected  */
      WHEN (
          dl.snapshot_at - date(lad.handover_at_utc)
        )
        + (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) > (nvl(soi.loan_duration,lad.installment_periods * lad.installment_period_days)) THEN
        round(nvl(soi.daily_rate                              /100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
        /*Otherwise, the number days in the month times the daily rate*/
        ELSE round(nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
      END )
    WHEN dl.months_on_book < 2 THEN
      0
    WHEN dl.months_on_book - 2 > lad.installment_periods THEN
      0
      ELSE round(COALESCE((nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days)),lad.initial_installment/100/30.5))
    END )AS expected_all_mtd,
    
	
	-- Calcular expected_in_month_in_repayment
    sum(
    CASE
    WHEN dl.state = 'rescheduled' THEN
      0
    WHEN dl.state = 'canceled' THEN
      0
    WHEN dl.terminated_at IS NOT NULL THEN
      0
    WHEN lad.loan_type = 'paygo' THEN (
      CASE
      WHEN date(dl.snapshot_at) - date(lad.handover_at_utc) < COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days ) THEN
        0
      WHEN (
          date(dl.snapshot_at) - date(lad.handover_at_utc) - COALESCE(
          CASE
          WHEN cd.country = 'KE'
            AND
            (
              lad.loan_type <> 'paygo'
              OR
              date(dl.snapshot_at) < '2022-03-21'
            )
            THEN
            30.5
          WHEN lad.loan_type = 'paygo'
            AND
            cd.country = 'KE' THEN
            7
          WHEN cd.country IN ('TZ',
                              'RW' )
            AND
            lad.loan_type <> 'paygo' THEN
            30.5
            ELSE soi.down_payment_days
          END
          ,soi.down_payment_days )
        )
        <= extract(day FROM date(dl.snapshot_at)) THEN
        (                   date(dl.snapshot_at) - date(lad.handover_at_utc) - COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days )) * (round(
        CASE
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG'
          AND
          date(lad.handover_at_utc) <= '2022-04-14' THEN
          initial_installment/100/30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG' THEN
          lad.initial_installment/100
        WHEN lad.loan_type = 'arrears'
          AND
          cd.country IN ('UG',
                         'ZM') THEN
          soi.daily_rate              /100
          ELSE lad.initial_installment/100/30.5
        END ))
      WHEN date(dl.snapshot_at) - date(lad.handover_at_utc) + COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days ) > COALESCE(soi.loan_duration,lad.installment_periods * lad.installment_period_days) + extract(day FROM date(dl.snapshot_at)) THEN
        0
        ELSE ((round(
        CASE
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG'
          AND
          date(lad.handover_at_utc) <= '2022-04-14' THEN
          initial_installment/100/30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG' THEN
          lad.initial_installment/100
        WHEN lad.loan_type = 'arrears'
          AND
          cd.country IN ('UG',
                         'ZM') THEN
          soi.daily_rate              /100
          ELSE lad.initial_installment/100/30.5
        END )) )
      END )
    WHEN dl.months_on_book < 2 THEN
      0
    WHEN dl.months_on_book - 2 > lad.installment_periods THEN
      0
      ELSE COALESCE(round(
      CASE
      WHEN lad.loan_type = 'paygo'
        AND
        cd.country = 'UG'
        AND
        date(lad.handover_at_utc) <= '2022-04-14' THEN
        initial_installment/100/30.5
      WHEN lad.loan_type = 'paygo'
        AND
        cd.country = 'UG' THEN
        lad.initial_installment/100
      WHEN lad.loan_type = 'arrears'
        AND
        cd.country IN ('UG',
                       'ZM') THEN
        soi.daily_rate              /100
        ELSE lad.initial_installment/100/30.5
      END ),lad.initial_installment/100/30.5 )
    END ) AS expected_in_month_in_repayment
    

    from analysts_inputs.credit_loan_accounts_daily dl 
	left join powerhub_reporting.eea_loan_account_details lad on dl.loan_account_id = lad.loan_account_id
    left join payments p on p.loan_id = dl.loan_account_id and date(dl.snapshot_at) = p.payment_date 
	left join powerhub_reporting.eea_customer_details cd on cd.customer_id = dl.customer_id
	LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and dl.snapshot_at = date(ph.benchmark_date)
	left join powerhub_reporting.eea_sales_order_items soi on (soi.loan_account_id = dl.loan_account_id AND
                                                        soi.customer_id = dl.customer_id
                                                        and soi.master_loan_item = 1 
                                                        and  soi.account_type = 'CREDIT' )
	
	LEFT JOIN powerhub_reporting.eea_sales_cases sc on sc.sales_case_id = soi.sales_case_id
	LEFT JOIN analysts_inputs.market_sources_ug_locations loc on trim(' ' FROM initcap(split_part(sc.hub_name, '(', 1))) = loc.location
	WHERE date(dl.snapshot_at) BETWEEN date_trunc('month', current_date - INTERVAL '1 day')::date AND (current_date - INTERVAL '1 day')::date
  --  and lower(dl.state) in ('potentially_paid_off', 'exceeded_grace_period', 'in_grace_period', 'exceeded_loan_period', 'maintained')
	and lad.initial_installment > 0 and lad.initial_amount > 0 
	and cd.country = 'UG'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

    UNION ALL
    
    /*ALMS*/

    select 
	    'alms' as company,
		'current month' as period,
        ll.report_date as report_date,
		ds.generated_date as generated_date,
		l.loan_contract_end_date,
		ll.loan_contract_end_date as new_loan_contract_end,
		alp.benchmark_date as benchmark_date,
        cast(alp.loan_id as varchar) as loan_id,
        cast(alp.customer_id as varchar) as customer_id,
		coalesce(alp.daily_rate/100,ll.daily_rate) as daily_rate,
		alp.loan_state as loan_state,
		ll.loan_state as new_loan_state,
		true as in_repayment,
		true as latest_in_repayment,
		lad.cancel_comment as cancellation_reason,
		coalesce(soi.product_line_name, ll.product) as product,
        coalesce(ph.manager_name, ll.team_lead) AS team_lead,
        coalesce( cd.country, ll.country) as country,
        coalesce(case     when cd.country in ('MZ', 'CI') then cd.area2      
                  else cd.area1 end, ll.region)  as region,
        p.payment_date,
        p.amount_paid,
		
		-- Calcular expected_all_mtd
    CASE 
    WHEN ll.loan_contract_end_date > COALESCE(alp.benchmark_date, ll.report_date, ds.generated_date) THEN 
        CAST(COALESCE(alp.daily_rate/100, ll.daily_rate) AS DOUBLE PRECISION)
    ELSE 
        CASE 
            WHEN COALESCE(alp.loan_state, ll.loan_state) = 'repayment' THEN 
                SUM(
                    CASE 
                        WHEN COALESCE(alp.days_elapsed, ll.days_elapsed) <= soi.loan_duration + soi.down_payment_days
                        THEN CAST(COALESCE(alp.daily_rate/100, ll.daily_rate) AS DOUBLE PRECISION)
                        ELSE 0 
                    END
                )
            ELSE 0 
        END
    END AS expected_all_mtd,

    -- Calcular expected_in_month_in_repayment
    CASE 
        WHEN alp.loan_state = 'repayment' THEN 
            cast(sum(case when alp.days_elapsed <= (soi.loan_duration + soi.down_payment_days)
                    then alp.daily_rate/100 else 0 end) as float) 
        ELSE 0 
    END AS expected_in_month_in_repayment
		
		
		
       
	FROM date_series ds
	LEFT JOIN latest_bench_alms ll ON ll.rn = 1  and ll.report_date = ds.generated_date 
	LEFT JOIN powerhub_reporting.eea_alms_loan_performance_benchmarks alp on alp.loan_id = ll.loan_id AND date(alp.benchmark_date) = ds.generated_date
    LEFT JOIN powerhub_reporting.eea_loan_account_details lad on lad.loan_account_id = alp.loan_id and lad.customer_id = alp.customer_id
        AND lad.source = 'alms'
    LEFT JOIN powerhub_reporting.eea_customer_details cd on cd.customer_id = alp.customer_id
    LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and alp.benchmark_date = ph.benchmark_date
	LEFT JOIN powerhub_reporting.eea_sales_order_items soi on (lad.sales_case_id = soi.sales_case_id
                                                and alp.loan_id = soi.loan_account_id)
                                                
	LEFT JOIN sensitive.finance_global_finance_report_monthly_alms l on alp.loan_id = l.loan_id and ((DATE_TRUNC('MONTH',  COALESCE(alp.benchmark_date, ll.report_date)) - INTERVAL '1 day')::DATE = date(l.report_date))
    LEFT JOIN payments p on p.loan_id = alp.loan_id and alp.benchmark_date = p.payment_date
	WHERE date(ll.report_date) BETWEEN date_trunc('month', current_date - INTERVAL '1 day')::date AND (current_date - INTERVAL '1 day')::date
    and (cd.country = 'UG' or ll.country = 'UG')
    --and lower(alp.loan_state) in ('repayment', 'active')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21






),
previous_month AS (
/*
WITH RECURSIVE date_series (generated_date) AS (
    SELECT date_trunc('month', current_date - interval '1 month')::date AS generated_date
    UNION ALL
    SELECT (generated_date + interval '1 day')::date  -- Ensure the result is cast to date
    FROM date_series
    WHERE generated_date + interval '1 day' < (current_date - interval '1 month')
),*/

WITH numbers AS (
    SELECT 
	ROW_NUMBER() OVER () - 1 AS n
    FROM powerhub_reporting.reporting_person_demographics
    LIMIT 31
),
date_series AS (
    SELECT 
        (date_trunc('month', (current_date - INTERVAL '1 day') - INTERVAL '1 month') + n * INTERVAL '1 day')::date AS generated_date
    FROM numbers
    WHERE (date_trunc('month', (current_date - INTERVAL '1 day') - INTERVAL '1 month') + n * INTERVAL '1 day')::date
          <= (current_date - INTERVAL '1 day')::date  -- Limitar até "ontem"
          AND (date_trunc('month', (current_date - INTERVAL '1 day') - INTERVAL '1 month') + n * INTERVAL '1 day')::date <= (date_trunc('month', (current_date - INTERVAL '1 day')) - INTERVAL '1 day')::date  -- Limitar até o último dia do mês anterior
), 


     payments AS (
			SELECT
				customer_id::varchar,
				loan_id::varchar,
				date(added_at_utc) AS payment_date,
				sum(portion_interest::float + portion_principal::float)/100 as amount_paid
			FROM
				powerhub_reporting.eea_alms_loan_transactions
				where transaction_type in ('Loan Payment', 'Loan Transaction Reversal')
				AND added_at_utc >= date_trunc('month', (CURRENT_DATE -1) - interval '1 month') 
                AND added_at_utc < date_trunc('month', (CURRENT_DATE-1))
                AND EXTRACT(day FROM added_at_utc) <= EXTRACT(day FROM (CURRENT_DATE-1))
				and loan_id like 'ug%'
				group by 1,2,3
			
			UNION ALL
			
			SELECT
				customer_id::varchar,
				loan_id::varchar,
				date(added_at_utc) AS payment_date,
				sum(portion_interest::float + portion_principal::float) as amount_paid
				
			FROM
				powerhub_reporting.reporting_loan_transactions
				where transaction_type in ('Payment', 'Reversal','Transfer', 'Automatic Arrears Payment','Refund')
			    AND added_at_utc >= date_trunc('month', (CURRENT_DATE-1) - interval '1 month') 
                AND added_at_utc < date_trunc('month', (CURRENT_DATE-1))
                AND EXTRACT(day FROM added_at_utc) <= EXTRACT(day FROM (CURRENT_DATE-1))
				and country = 'UG'
				group by 1,2,3
				
			UNION ALL
			
			SELECT
				customer_id::varchar,
				loan_account_id::varchar as loan_id,
				date(payment_at) AS payment_date,
				sum(amount::float)/100 as amount_paid
				
			FROM
				powerhub_reporting.eea_payment_bookings
				where transaction_type in ('PaymentChargeTransaction', 'RescueChargeTransaction')
	            AND payment_at >= date_trunc('month', (CURRENT_DATE -1) - interval '1 month') 
                AND payment_at < date_trunc('month', (CURRENT_DATE-1))
                AND EXTRACT(day FROM payment_at) <= EXTRACT(day FROM (CURRENT_DATE-1))
				and loan_account_id like 'ug%'
				group by 1,2,3	
		),
		
		latest_ldm_fenix AS (
    SELECT ldm.loan_id,
           ldm.customer_id,
           l.loan_state as loan_state,
           ldm.benchmark_date,
           ldm.daily_rate,
           ldm.in_repayment,
           ldm.days_elapsed,
           loan_contract_end_date,
           sd.product_type as product,
           sd.associator_manager_name AS team_lead,
           sd.country_of_sale as country,
           (CASE 
           WHEN sd.country_of_sale IN ('Uganda', 'Mozambique', 'Côte d''Ivoire') THEN pd.district
           ELSE pd.region 
           END) as region,
           ROW_NUMBER() OVER (PARTITION BY ldm.loan_id ORDER BY ldm.benchmark_date DESC) AS rn
    FROM powerhub_reporting.reporting_loan_daily_metrics ldm
   -- LEFT JOIN powerhub_reporting.reporting_loan_current_details sh on ldm.loan_id = sh.loan_id and date(ldm.benchmark_date) = date(sh.benchmark_date_utc)
    LEFT JOIN sensitive.finance_global_finance_report_monthly l ON ldm.loan_id = l.loan_id AND (DATE_TRUNC('MONTH', ldm.benchmark_date) - INTERVAL '1 day')::DATE = DATE(l.report_date)
    LEFT JOIN powerhub_reporting.reporting_sales_details sd ON (ldm.loan_id = sd.loan_id AND ldm.account_id = sd.account_id AND ldm.days_elapsed > sd.introductory_period)
    LEFT JOIN powerhub_reporting.reporting_person_demographics pd ON sd.customer_id_fenixdb = pd.person_id 
    WHERE date(ldm.benchmark_date) BETWEEN date_trunc('month', (current_date - 1) - interval '1 month')::date 
                                  AND ((current_date -1) - interval '1 month')::date
    and sd.country_of_sale = 'Uganda'
),

 
    latest_bench_fenix as ( 
    SELECT loan_id,
           customer_id,
           loan_state,
           benchmark_date,
           daily_rate,
           coalesce(in_repayment, 'FALSE') as in_repayment,
           days_elapsed,
           loan_contract_end_date,
           product,
           team_lead,
           country,
           region,
           rn,
		   generated_date as report_date
    FROM   latest_ldm_fenix l, date_series ds
	WHERE  rn = 1
	),
	
	latest_ldm_alms AS (
    SELECT cast(alp.loan_id as varchar) as loan_id,
           cast(alp.customer_id as varchar) as customer_id,
           alp.loan_state as loan_state,
           alp.benchmark_date,
           alp.daily_rate/100 as daily_rate,
           alp.days_elapsed,
           l.loan_contract_end_date,
           soi.product_line_name as product,
           ph.manager_name as Team_Lead,
           cd.country,
           case  when cd.country in ('MZ', 'CI') then cd.area2      
                  else cd.area1 end  as region,
           ROW_NUMBER() OVER (PARTITION BY alp.loan_id ORDER BY alp.benchmark_date DESC) AS rn
    FROM powerhub_reporting.eea_alms_loan_performance_benchmarks alp
    
    LEFT JOIN sensitive.finance_global_finance_report_monthly_alms l ON alp.loan_id = l.loan_id AND (DATE_TRUNC('MONTH', alp.benchmark_date) - INTERVAL '1 day')::DATE = DATE(l.report_date)
    LEFT JOIN powerhub_reporting.eea_loan_account_details lad on lad.loan_account_id = alp.loan_id and lad.customer_id = alp.customer_id
        AND lad.source = 'alms'
	LEFT JOIN powerhub_reporting.eea_customer_details cd on cd.customer_id = alp.customer_id
    LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and alp.benchmark_date = ph.benchmark_date
	LEFT JOIN powerhub_reporting.eea_sales_order_items soi on (lad.sales_case_id = soi.sales_case_id
                                                and alp.loan_id = soi.loan_account_id)
    WHERE date(alp.benchmark_date) BETWEEN date_trunc('month', (current_date - 1) - interval '1 month')::date 
                                  AND ((current_date - 1) - interval '1 month')::date
    and cd.country = 'UG'
),

 
    latest_bench_alms as ( 
    SELECT loan_id,
           customer_id,
           loan_state,
           benchmark_date,
           daily_rate,
           days_elapsed,
           loan_contract_end_date,
           product,
           team_lead,
           country,
           region,
           rn,
		   generated_date as report_date
    FROM   latest_ldm_alms l, date_series ds
	WHERE  rn = 1
	
	)
		
	  

		/*FENIX*/
		
		Select 
		'fenix' as company,
		'last month' as period,
        ll.report_date as report_date,
        ds.generated_date as generated_date,
        l.loan_contract_end_date,
        ll.loan_contract_end_date as new_loan_contract_end,
        ldm.benchmark_date as benchmark_date,
		coalesce(CAST(ldm.loan_id AS VARCHAR), cast(ll.loan_id as varchar)) as loan_id,
        coalesce(CAST(ldm.customer_id AS varchar), cast(ll.customer_id as varchar)) as customer_id,
        coalesce(ldm.daily_rate,ll.daily_rate) as daily_rate,
		l.loan_state as loan_state,
		ll.loan_state as new_loan_state,
		ldm.in_repayment,
		ll.in_repayment as latest_in_repayment,
		crh.cancellation_reason,
		coalesce(sd.product_type, ll.product) as product,
        coalesce(sd.associator_manager_name, ll.team_lead) AS team_lead,
        coalesce(sd.country_of_sale, ll.country) as country,
        coalesce((CASE 
            WHEN sd.country_of_sale IN ('Uganda', 'Mozambique', 'Côte d''Ivoire') THEN pd.district
            ELSE pd.region 
            END),ll.region) as region,
        p.payment_date,
        p.amount_paid,  
    -- Calcular expected_all_mtd
    CASE 
    WHEN ll.loan_contract_end_date > COALESCE(ldm.benchmark_date, ll.report_date, ds.generated_date) THEN 
        CAST(COALESCE(ldm.daily_rate, ll.daily_rate) AS DOUBLE PRECISION)
    ELSE 
        CASE 
            WHEN COALESCE(ldm.in_repayment, ll.in_repayment) = 'TRUE' THEN 
                SUM(
                    CASE 
                        WHEN COALESCE(ldm.days_elapsed, ll.days_elapsed) <= sd.loan_duration + sd.introductory_period 
                        THEN CAST(COALESCE(ldm.daily_rate, ll.daily_rate) AS DOUBLE PRECISION)
                        ELSE 0 
                    END
                )
            ELSE 0 
        END
    END AS expected_all_mtd,

    -- Calcular expected_in_month_in_repayment
    CASE 
        WHEN ldm.in_repayment = 'TRUE' THEN 
            SUM(
                CASE 
                    WHEN ldm.days_elapsed <= sd.loan_duration + sd.introductory_period 
                    THEN CAST(ldm.daily_rate AS DOUBLE PRECISION)
                    ELSE 0 
                END
            )
        ELSE 0 
    END AS expected_in_month_in_repayment

						
		FROM 
    date_series ds
	LEFT JOIN latest_bench_fenix ll ON ll.rn = 1  and ll.report_date = ds.generated_date 
    LEFT JOIN powerhub_reporting.reporting_loan_daily_metrics ldm ON ldm.loan_id = ll.loan_id AND date(ldm.benchmark_date) = ds.generated_date 
	
    LEFT JOIN powerhub_reporting.reporting_sales_details sd ON (ldm.loan_id = sd.loan_id AND
                                             ldm.account_id = sd.account_id AND
                                             /*Exclude days prior to intro period */
                                             ldm.days_elapsed > sd.introductory_period)
    
    LEFT JOIN powerhub_reporting.reporting_person_demographics pd ON sd.customer_id_fenixdb = pd.person_id 
    LEFT JOIN payments p on ldm.loan_id = p.loan_id and COALESCE(ldm.benchmark_date, ds.generated_date) = p.payment_date 
   -- LEFT JOIN powerhub_reporting.reporting_loan_current_details sh on COALESCE(ldm.loan_id, ll.loan_id) = sh.loan_id and date(COALESCE(ldm.benchmark_date) = date(sh.benchmark_date_utc)
	LEFT JOIN powerhub_reporting.reporting_loan_cancellation_reason_history crh on ldm.loan_id = crh.loan_id and date(ldm.benchmark_date) = date(crh.change_recorded_at_utc)
	LEFT JOIN sensitive.finance_global_finance_report_monthly l on ldm.loan_id = l.loan_id and (DATE_TRUNC('MONTH',  COALESCE(ldm.benchmark_date, ll.report_date)) - INTERVAL '1 day')::DATE = date(l.report_date)
	WHERE date(ll.report_date) BETWEEN date_trunc('month', (current_date - 1) - interval '1 month')::date 
                                  AND ((current_date -1) - interval '1 month')::date
	
	and (sd.country_of_sale = 'Uganda' or ll.country = 'Uganda') 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
		
		UNION ALL
		
        /*MOBISOL*/
		
		Select
	'paygee' as company,
	'last month' as period,
    date(dl.snapshot_at) as report_date,
	date(dl.snapshot_at) as generated_date,
	lad.loan_contract_end_date,
	lad.loan_contract_end_date as new_loan_contract_end,
	date(dl.snapshot_at) as benchmark_date,
	cast (dl.loan_account_id as VARCHAR) as loan_id,
	cast(dl.customer_id as varchar) as customer_id,
	round(NVL(soi.daily_rate/100,(NVL(lad.initial_installment,0)/100)/lad.installment_period_days)) as daily_rate,
	lower(dl.state) as loan_state,
	lower(dl.state) as new_loan_state,
	true as in_repayment,
	true as latest_in_repayment,
	lad.cancel_comment as cancellation_reason,
	soi.product_line_name as product,
	(case when cd.country = 'TZ' then split_part(rbe_list_per_area_v4_20240910_2(cd.area2),'~',1)
	else ph.manager_name end) as team_lead,
    cd.country,
    case when cd.country in ('MZ', 'CI') then cd.area2
    else cd.area1 end as region,
    p.payment_date,
    p.amount_paid,  
	
		-- Calcular expected_all_mtd
    SUM(
    CASE
    WHEN dl.state = 'rescheduled' THEN
      0
    WHEN cd.country <> 'KE'
      AND
      dl.state = 'canceled' THEN
      0
    WHEN dl.state = 'paid_off' THEN
      0
    WHEN lad.loan_type = 'paygo' THEN (
      CASE
      WHEN dl.snapshot_at - date(lad.handover_at_utc) < (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) THEN
        0
        /*When customer is in the first month then only take days elapsed - introperiod */
      WHEN (
          dl.snapshot_at - date(lad.handover_at_utc) - (
          CASE
          WHEN cd.country = 'KE'
            AND
            (
              lad.loan_type <> 'paygo'
              OR
              dl.snapshot_at < '2022-03-21'
            )
            THEN
            30.5
          WHEN lad.loan_type = 'paygo'
            AND
            cd.country = 'KE' THEN
            7
          WHEN cd.country IN ('TZ',
                              'RW' )
            AND
            lad.loan_type <> 'paygo' THEN
            30.5
            ELSE soi.down_payment_days
          END)
        )
        <= extract(day FROM dl.snapshot_at) THEN
        round(nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
        /*When customer is beyond their original loan duration + days expected in the last month then expect 0*/
      WHEN dl.snapshot_at - date(lad.handover_at_utc) + (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) > (nvl(soi.loan_duration,lad.installment_periods * lad.installment_period_days)) + extract(day FROM dl.snapshot_at) THEN
        0
        /*When customer is beyond their original loan duration but payoff within the last month then days in the month
    are expected  */
      WHEN (
          dl.snapshot_at - date(lad.handover_at_utc)
        )
        + (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) > (nvl(soi.loan_duration,lad.installment_periods * lad.installment_period_days)) THEN
        round(nvl(soi.daily_rate                              /100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
        /*Otherwise, the number days in the month times the daily rate*/
        ELSE round(nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
      END )
    WHEN dl.months_on_book < 2 THEN
      0
    WHEN dl.months_on_book - 2 > lad.installment_periods THEN
      0
      ELSE round(COALESCE((nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days)),lad.initial_installment/100/30.5))
    END )AS expected_all_mtd,
    
	
	-- Calcular expected_in_month_in_repayment
    sum(
    CASE
    WHEN dl.state = 'rescheduled' THEN
      0
    WHEN dl.state = 'canceled' THEN
      0
    WHEN dl.terminated_at IS NOT NULL THEN
      0
    WHEN lad.loan_type = 'paygo' THEN (
      CASE
      WHEN date(dl.snapshot_at) - date(lad.handover_at_utc) < COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days ) THEN
        0
      WHEN (
          date(dl.snapshot_at) - date(lad.handover_at_utc) - COALESCE(
          CASE
          WHEN cd.country = 'KE'
            AND
            (
              lad.loan_type <> 'paygo'
              OR
              date(dl.snapshot_at) < '2022-03-21'
            )
            THEN
            30.5
          WHEN lad.loan_type = 'paygo'
            AND
            cd.country = 'KE' THEN
            7
          WHEN cd.country IN ('TZ',
                              'RW' )
            AND
            lad.loan_type <> 'paygo' THEN
            30.5
            ELSE soi.down_payment_days
          END
          ,soi.down_payment_days )
        )
        <= extract(day FROM date(dl.snapshot_at)) THEN
        (                   date(dl.snapshot_at) - date(lad.handover_at_utc) - COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days )) * (round(
        CASE
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG'
          AND
          date(lad.handover_at_utc) <= '2022-04-14' THEN
          initial_installment/100/30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG' THEN
          lad.initial_installment/100
        WHEN lad.loan_type = 'arrears'
          AND
          cd.country IN ('UG',
                         'ZM') THEN
          soi.daily_rate              /100
          ELSE lad.initial_installment/100/30.5
        END ))
      WHEN date(dl.snapshot_at) - date(lad.handover_at_utc) + COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days ) > COALESCE(soi.loan_duration,lad.installment_periods * lad.installment_period_days) + extract(day FROM date(dl.snapshot_at)) THEN
        0
        ELSE ((round(
        CASE
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG'
          AND
          date(lad.handover_at_utc) <= '2022-04-14' THEN
          initial_installment/100/30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG' THEN
          lad.initial_installment/100
        WHEN lad.loan_type = 'arrears'
          AND
          cd.country IN ('UG',
                         'ZM') THEN
          soi.daily_rate              /100
          ELSE lad.initial_installment/100/30.5
        END )) )
      END )
    WHEN dl.months_on_book < 2 THEN
      0
    WHEN dl.months_on_book - 2 > lad.installment_periods THEN
      0
      ELSE COALESCE(round(
      CASE
      WHEN lad.loan_type = 'paygo'
        AND
        cd.country = 'UG'
        AND
        date(lad.handover_at_utc) <= '2022-04-14' THEN
        initial_installment/100/30.5
      WHEN lad.loan_type = 'paygo'
        AND
        cd.country = 'UG' THEN
        lad.initial_installment/100
      WHEN lad.loan_type = 'arrears'
        AND
        cd.country IN ('UG',
                       'ZM') THEN
        soi.daily_rate              /100
        ELSE lad.initial_installment/100/30.5
      END ),lad.initial_installment/100/30.5 )
    END ) AS expected_in_month_in_repayment


    from analysts_inputs.credit_loan_accounts_daily dl 
	left join powerhub_reporting.eea_loan_account_details lad on dl.loan_account_id = lad.loan_account_id
    left join payments p on p.loan_id = dl.loan_account_id and date(dl.snapshot_at) = p.payment_date 
	left join powerhub_reporting.eea_customer_details cd on cd.customer_id = dl.customer_id
	LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and dl.snapshot_at = date(ph.benchmark_date)
	left join powerhub_reporting.eea_sales_order_items soi on (soi.loan_account_id = dl.loan_account_id AND
                                                        soi.customer_id = dl.customer_id
                                                        and soi.master_loan_item = 1 
                                                        and  soi.account_type = 'CREDIT' )
    WHERE date(dl.snapshot_at) BETWEEN date_trunc('month', (current_date - 1) - interval '1 month')::date 
                                  AND ((current_date -1) - interval '1 month')::date
	--and lower(dl.state) in ('potentially_paid_off', 'exceeded_grace_period', 'in_grace_period', 'exceeded_loan_period', 'maintained')
	and lad.initial_installment > 0 and lad.initial_amount > 0 
    and cd.country = 'UG'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

    UNION ALL
    
    /*ALMS*/

    select 
	'alms' as company,
	'last month' as period,
    ll.report_date as report_date,
	ds.generated_date as generated_date,
	l.loan_contract_end_date,
	ll.loan_contract_end_date as new_loan_contract_end,
	alp.benchmark_date as benchmark_date,
    cast(alp.loan_id as varchar) as loan_id,
    cast(alp.customer_id as varchar) as customer_id,
	coalesce(alp.daily_rate/100,ll.daily_rate) as daily_rate,
	alp.loan_state as loan_state,
	ll.loan_state as new_loan_state,
	true as in_repayment,
	true as latest_in_repayment,
	lad.cancel_comment as cancellation_reason,
	coalesce(soi.product_line_name, ll.product) as product,
    coalesce(ph.manager_name, ll.team_lead) AS team_lead,
    coalesce( cd.country, ll.country) as country,
    coalesce(case when cd.country in ('MZ', 'CI') then cd.area2
              when cd.country = 'UG' then loc.region	
              else cd.area1 end, ll.region)  as region,
    p.payment_date,
    p.amount_paid,
	
	-- Calcular expected_all_mtd
    CASE 
    WHEN ll.loan_contract_end_date > COALESCE(alp.benchmark_date, ll.report_date, ds.generated_date) THEN 
        CAST(COALESCE(alp.daily_rate/100, ll.daily_rate) AS DOUBLE PRECISION)
    ELSE 
        CASE 
            WHEN COALESCE(alp.loan_state, ll.loan_state) = 'repayment' THEN 
                SUM(
                    CASE 
                        WHEN COALESCE(alp.days_elapsed, ll.days_elapsed) <= soi.loan_duration + soi.down_payment_days
                        THEN CAST(COALESCE(alp.daily_rate/100, ll.daily_rate) AS DOUBLE PRECISION)
                        ELSE 0 
                    END
                )
            ELSE 0 
        END
    END AS expected_all_mtd,

    -- Calcular expected_in_month_in_repayment
    CASE 
        WHEN alp.loan_state = 'repayment' THEN 
            cast(sum(case when alp.days_elapsed <= (soi.loan_duration + soi.down_payment_days)
                    then alp.daily_rate/100 else 0 end) as float) 
        ELSE 0 
    END AS expected_in_month_in_repayment
		
		
		
       
	FROM date_series ds
	LEFT JOIN latest_bench_alms ll ON ll.rn = 1  and ll.report_date = ds.generated_date 
	LEFT JOIN powerhub_reporting.eea_alms_loan_performance_benchmarks alp on alp.loan_id = ll.loan_id AND date(alp.benchmark_date) = ds.generated_date
    LEFT JOIN powerhub_reporting.eea_loan_account_details lad on lad.loan_account_id = alp.loan_id and lad.customer_id = alp.customer_id
        AND lad.source = 'alms'
    LEFT JOIN powerhub_reporting.eea_customer_details cd on cd.customer_id = alp.customer_id
    LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and alp.benchmark_date = ph.benchmark_date
	LEFT JOIN powerhub_reporting.eea_sales_order_items soi on (lad.sales_case_id = soi.sales_case_id
                                                and alp.loan_id = soi.loan_account_id)
                                                
	LEFT JOIN sensitive.finance_global_finance_report_monthly_alms l on alp.loan_id = l.loan_id and ((DATE_TRUNC('MONTH',  COALESCE(alp.benchmark_date, ll.report_date)) - INTERVAL '1 day')::DATE = date(l.report_date))
    left join powerhub_reporting.eea_sales_cases sc on sc.sales_case_id = soi.sales_case_id
	left join analysts_inputs.market_sources_ug_locations loc on trim(' ' FROM initcap(split_part(sc.hub_name, '(', 1))) = loc.location
	LEFT JOIN payments p on p.loan_id = alp.loan_id and alp.benchmark_date = p.payment_date
	WHERE date(ll.report_date) BETWEEN date_trunc('month', (current_date - 1) - interval '1 month')::date 
                                  AND ((current_date - 1) - interval '1 month')::date
    and (cd.country = 'UG' or ll.country = 'UG')
    --and lower(alp.loan_state) in ('repayment', 'active')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21






),
previous_year AS (
/*
WITH RECURSIVE date_series (generated_date) AS (
    SELECT date_trunc('month', current_date - interval '1 year')::date AS generated_date
    UNION ALL
    SELECT (generated_date + interval '1 day')::date  -- Ensure the result is cast to date
    FROM date_series
    WHERE generated_date + interval '1 day' < (current_date - interval '1 year')
),*/

WITH numbers AS (
   SELECT 
	ROW_NUMBER() OVER () - 1 AS n
    FROM powerhub_reporting.reporting_person_demographics
    LIMIT 31
),
date_series AS (
    SELECT 
        (date_trunc('month', (current_date - INTERVAL '1 day') - INTERVAL '1 year') + n * INTERVAL '1 day')::date AS generated_date
    FROM numbers
    WHERE (date_trunc('month', (current_date - INTERVAL '1 day') - INTERVAL '1 year') + n * INTERVAL '1 day')::date
          <= (current_date - INTERVAL '1 day')::date  -- Limit to "yesterday"
          AND (date_trunc('month', (current_date - INTERVAL '1 day') - INTERVAL '1 year') + n * INTERVAL '1 day')::date <= (date_trunc('month', (current_date - INTERVAL '1 day')) - INTERVAL '1 day')::date  -- Limit to the last day of the previous month
),

latest_ldm_fenix AS (
    SELECT ldm.loan_id,
           ldm.customer_id,
           l.loan_state as loan_state,
           ldm.benchmark_date,
           ldm.daily_rate,
           ldm.in_repayment,
           ldm.days_elapsed,
           loan_contract_end_date,
           sd.product_type as product,
           sd.associator_manager_name AS team_lead,
           sd.country_of_sale as country,
           (CASE 
           WHEN sd.country_of_sale IN ('Uganda', 'Mozambique', 'Côte d''Ivoire') THEN pd.district
           ELSE pd.region 
           END) as region,
           ROW_NUMBER() OVER (PARTITION BY ldm.loan_id ORDER BY ldm.benchmark_date DESC) AS rn
    FROM powerhub_reporting.reporting_loan_daily_metrics ldm
 --  LEFT JOIN powerhub_reporting.reporting_loan_current_details sh on ldm.loan_id = sh.loan_id and date(ldm.benchmark_date) = date(sh.benchmark_date_utc)
    LEFT JOIN sensitive.finance_global_finance_report_monthly l ON ldm.loan_id = l.loan_id AND (DATE_TRUNC('MONTH', ldm.benchmark_date) - INTERVAL '1 day')::DATE = DATE(l.report_date)
    LEFT JOIN powerhub_reporting.reporting_sales_details sd ON (ldm.loan_id = sd.loan_id AND ldm.account_id = sd.account_id AND ldm.days_elapsed > sd.introductory_period)
    LEFT JOIN powerhub_reporting.reporting_person_demographics pd ON sd.customer_id_fenixdb = pd.person_id 
    WHERE date(ldm.benchmark_date) BETWEEN date_trunc('month', (current_date - 1) - interval '1 year')::date
                                  AND ((current_date - 1) - interval '1 year')::date
    and sd.country_of_sale = 'Uganda'
),

 
    latest_bench_fenix as ( 
    SELECT loan_id,
           customer_id,
           loan_state,
           benchmark_date,
           daily_rate,
           coalesce(in_repayment, 'FALSE') as in_repayment,
           days_elapsed,
           loan_contract_end_date,
           product,
           team_lead,
           country,
           region,
           rn,
		   generated_date as report_date
    FROM   latest_ldm_fenix l, date_series ds
	WHERE  rn = 1
	),   
	
	latest_ldm_alms AS (
    SELECT cast(alp.loan_id as varchar) as loan_id,
           cast(alp.customer_id as varchar) as customer_id,
           alp.loan_state as loan_state,
           alp.benchmark_date,
           alp.daily_rate/100 as daily_rate,
           alp.days_elapsed,
           l.loan_contract_end_date,
           soi.product_line_name as product,
           ph.manager_name as Team_Lead,
           cd.country,
           case  when cd.country in ('MZ', 'CI') then cd.area2      
                  else cd.area1 end  as region,
           ROW_NUMBER() OVER (PARTITION BY alp.loan_id ORDER BY alp.benchmark_date DESC) AS rn
    FROM powerhub_reporting.eea_alms_loan_performance_benchmarks alp
    
    LEFT JOIN sensitive.finance_global_finance_report_monthly_alms l ON alp.loan_id = l.loan_id AND (DATE_TRUNC('MONTH', alp.benchmark_date) - INTERVAL '1 day')::DATE = DATE(l.report_date)
    LEFT JOIN powerhub_reporting.eea_loan_account_details lad on lad.loan_account_id = alp.loan_id and lad.customer_id = alp.customer_id
        AND lad.source = 'alms'
	LEFT JOIN powerhub_reporting.eea_customer_details cd on cd.customer_id = alp.customer_id
    LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and alp.benchmark_date = ph.benchmark_date
	LEFT JOIN powerhub_reporting.eea_sales_order_items soi on (lad.sales_case_id = soi.sales_case_id
                                                and alp.loan_id = soi.loan_account_id)
    WHERE date(alp.benchmark_date) BETWEEN date_trunc('month', (current_date - 1) - interval '1 year')::date
                                  AND ((current_date - 1) - interval '1 year')::date
    and cd.country = 'UG'
),

 
    latest_bench_alms as ( 
    SELECT loan_id,
           customer_id,
           loan_state,
           benchmark_date,
           daily_rate,
           days_elapsed,
           loan_contract_end_date,
           product,
           team_lead,
           country,
           region,
           rn,
		   generated_date as report_date
    FROM   latest_ldm_alms l, date_series ds
	WHERE  rn = 1
	
	),


   payments AS (
			SELECT
				customer_id::varchar,
				loan_id::varchar,
				date(added_at_utc) AS payment_date,
				sum(portion_interest::float + portion_principal::float)/100 as amount_paid
			FROM
				powerhub_reporting.eea_alms_loan_transactions
				where transaction_type in ('Loan Payment', 'Loan Transaction Reversal')
				AND added_at_utc >= date_trunc('month', (CURRENT_DATE - 1) - interval '1 year') 
                AND added_at_utc < date_trunc('month', (CURRENT_DATE - 1))
                AND EXTRACT(day FROM added_at_utc) <= EXTRACT(day FROM (CURRENT_DATE - 1))
				and loan_id like 'ug%'
				group by 1,2,3
			
			UNION ALL
			
			SELECT
				customer_id::varchar,
				loan_id::varchar,
				date(added_at_utc) AS payment_date,
				sum(portion_interest::float + portion_principal::float) as amount_paid
				
			FROM
				powerhub_reporting.reporting_loan_transactions
				where transaction_type in ('Payment', 'Reversal','Transfer', 'Automatic Arrears Payment','Refund')
			    AND added_at_utc >= date_trunc('month', (CURRENT_DATE - 1) - interval '1 year') 
                AND added_at_utc < date_trunc('month', (CURRENT_DATE - 1))
                AND EXTRACT(day FROM added_at_utc) <= EXTRACT(day FROM (CURRENT_DATE - 1))
				and country = 'UG'
				group by 1,2,3
				
			UNION ALL
			
			SELECT
				customer_id::varchar,
				loan_account_id::varchar as loan_id,
				date(payment_at) AS payment_date,
				sum(amount::float)/100 as amount_paid
				
			FROM
				powerhub_reporting.eea_payment_bookings
				where transaction_type in ('PaymentChargeTransaction', 'RescueChargeTransaction')
	            AND payment_at >= date_trunc('month', (CURRENT_DATE - 1) - interval '1 year') 
                AND payment_at < date_trunc('month', (CURRENT_DATE - 1))
                AND EXTRACT(day FROM payment_at) <= EXTRACT(day FROM (CURRENT_DATE - 1))
				and loan_account_id like 'ug%'
				group by 1,2,3	
		)
		
	  

		/*FENIX*/
		
		Select 
		'fenix' as company,
		'last year' as period,
        ll.report_date as report_date,
        ds.generated_date as generated_date,
        l.loan_contract_end_date,
        ll.loan_contract_end_date as new_loan_contract_end,
        ldm.benchmark_date as benchmark_date,
		coalesce(CAST(ldm.loan_id AS VARCHAR), cast(ll.loan_id as varchar)) as loan_id,
        coalesce(CAST(ldm.customer_id AS varchar), cast(ll.customer_id as varchar)) as customer_id,
        coalesce(ldm.daily_rate,ll.daily_rate) as daily_rate,
		l.loan_state as loan_state,
		ll.loan_state as new_loan_state,
		ldm.in_repayment,
		ll.in_repayment as latest_in_repayment,
		crh.cancellation_reason,
		coalesce(sd.product_type, ll.product) as product,
        coalesce(sd.associator_manager_name, ll.team_lead) AS team_lead,
        coalesce(sd.country_of_sale, ll.country) as country,
        coalesce((CASE 
            WHEN sd.country_of_sale IN ('Uganda', 'Mozambique', 'Côte d''Ivoire') THEN pd.district
            ELSE pd.region 
            END),ll.region) as region,
        p.payment_date,
        p.amount_paid,    
    -- Calcular expected_all_mtd
    CASE 
    WHEN ll.loan_contract_end_date > COALESCE(ldm.benchmark_date, ll.report_date, ds.generated_date) THEN 
        CAST(COALESCE(ldm.daily_rate, ll.daily_rate) AS DOUBLE PRECISION)
    ELSE 
        CASE 
            WHEN COALESCE(ldm.in_repayment, ll.in_repayment) = 'TRUE' THEN 
                SUM(
                    CASE 
                        WHEN COALESCE(ldm.days_elapsed, ll.days_elapsed) <= sd.loan_duration + sd.introductory_period 
                        THEN CAST(COALESCE(ldm.daily_rate, ll.daily_rate) AS DOUBLE PRECISION)
                        ELSE 0 
                    END
                )
            ELSE 0 
        END
    END AS expected_all_mtd,

    -- Calcular expected_in_month_in_repayment
    CASE 
        WHEN ldm.in_repayment = 'TRUE' THEN 
            SUM(
                CASE 
                    WHEN ldm.days_elapsed <= sd.loan_duration + sd.introductory_period 
                    THEN CAST(ldm.daily_rate AS DOUBLE PRECISION)
                    ELSE 0 
                END
            )
        ELSE 0 
    END AS expected_in_month_in_repayment

						
		FROM 
    date_series ds
	LEFT JOIN latest_bench_fenix ll ON ll.rn = 1  and ll.report_date = ds.generated_date 
    LEFT JOIN powerhub_reporting.reporting_loan_daily_metrics ldm ON ldm.loan_id = ll.loan_id AND date(ldm.benchmark_date) = ds.generated_date 
	
    LEFT JOIN powerhub_reporting.reporting_sales_details sd ON (ldm.loan_id = sd.loan_id AND
                                             ldm.account_id = sd.account_id AND
                                             /*Exclude days prior to intro period */
                                             ldm.days_elapsed > sd.introductory_period)
    
    LEFT JOIN powerhub_reporting.reporting_person_demographics pd ON sd.customer_id_fenixdb = pd.person_id 
    LEFT JOIN payments p on ldm.loan_id = p.loan_id and COALESCE(ldm.benchmark_date, ds.generated_date) = p.payment_date 
   -- LEFT JOIN powerhub_reporting.reporting_loan_current_details sh on COALESCE(ldm.loan_id, ll.loan_id) = sh.loan_id and date(COALESCE(ldm.benchmark_date, ll.report_date)) = date(sh.benchmark_date_utc)
	LEFT JOIN powerhub_reporting.reporting_loan_cancellation_reason_history crh on ldm.loan_id = crh.loan_id and date(ldm.benchmark_date) = date(crh.change_recorded_at_utc)
	LEFT JOIN sensitive.finance_global_finance_report_monthly l on ldm.loan_id = l.loan_id and (DATE_TRUNC('MONTH',  COALESCE(ldm.benchmark_date, ll.report_date)) - INTERVAL '1 day')::DATE = date(l.report_date)
	WHERE date(ll.report_date) BETWEEN date_trunc('month', (current_date - 1) - interval '1 year')::date
                                  AND ((current_date - 1) - interval '1 year')::date
	and (sd.country_of_sale = 'Uganda' or ll.country = 'Uganda')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
		
	
		
		UNION ALL
		
        /*MOBISOL*/
		
		Select
	'paygee' as company,
	'last year' as period,
    date(dl.snapshot_at) as report_date,
	date(dl.snapshot_at) as generated_date,
	lad.loan_contract_end_date,
	lad.loan_contract_end_date as new_loan_contract_end,
	date(dl.snapshot_at) as benchmark_date,
	cast (dl.loan_account_id as VARCHAR) as loan_id,
	cast(dl.customer_id as varchar) as customer_id,
	round(NVL(soi.daily_rate/100,(NVL(lad.initial_installment,0)/100)/lad.installment_period_days)) as daily_rate,
	lower(dl.state) as loan_state,
	lower(dl.state) as new_loan_state,
	true as in_repayment,
	true as latest_in_repayment,
	lad.cancel_comment as cancellation_reason,
	soi.product_line_name as product,
	(case when cd.country = 'TZ' then split_part(rbe_list_per_area_v4_20240910_2(cd.area2),'~',1)
	else ph.manager_name end) as team_lead,
    cd.country,
    case when cd.country in ('MZ', 'CI') then cd.area2
    else cd.area1 end as region,
    p.payment_date,
    p.amount_paid,  
	
		-- Calcular expected_all_mtd
    SUM(
    CASE
    WHEN dl.state = 'rescheduled' THEN
      0
    WHEN cd.country <> 'KE'
      AND
      dl.state = 'canceled' THEN
      0
    WHEN dl.state = 'paid_off' THEN
      0
    WHEN lad.loan_type = 'paygo' THEN (
      CASE
      WHEN dl.snapshot_at - date(lad.handover_at_utc) < (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) THEN
        0
        /*When customer is in the first month then only take days elapsed - introperiod */
      WHEN (
          dl.snapshot_at - date(lad.handover_at_utc) - (
          CASE
          WHEN cd.country = 'KE'
            AND
            (
              lad.loan_type <> 'paygo'
              OR
              dl.snapshot_at < '2022-03-21'
            )
            THEN
            30.5
          WHEN lad.loan_type = 'paygo'
            AND
            cd.country = 'KE' THEN
            7
          WHEN cd.country IN ('TZ',
                              'RW' )
            AND
            lad.loan_type <> 'paygo' THEN
            30.5
            ELSE soi.down_payment_days
          END)
        )
        <= extract(day FROM dl.snapshot_at) THEN
        round(nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
        /*When customer is beyond their original loan duration + days expected in the last month then expect 0*/
      WHEN dl.snapshot_at - date(lad.handover_at_utc) + (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) > (nvl(soi.loan_duration,lad.installment_periods * lad.installment_period_days)) + extract(day FROM dl.snapshot_at) THEN
        0
        /*When customer is beyond their original loan duration but payoff within the last month then days in the month
    are expected  */
      WHEN (
          dl.snapshot_at - date(lad.handover_at_utc)
        )
        + (
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            dl.snapshot_at < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END) > (nvl(soi.loan_duration,lad.installment_periods * lad.installment_period_days)) THEN
        round(nvl(soi.daily_rate                              /100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
        /*Otherwise, the number days in the month times the daily rate*/
        ELSE round(nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days))
      END )
    WHEN dl.months_on_book < 2 THEN
      0
    WHEN dl.months_on_book - 2 > lad.installment_periods THEN
      0
      ELSE round(COALESCE((nvl(soi.daily_rate/100,(nvl(lad.initial_installment,0)/100)/lad.installment_period_days)),lad.initial_installment/100/30.5))
    END )AS expected_all_mtd,
    
	
	-- Calcular expected_in_month_in_repayment
    sum(
    CASE
    WHEN dl.state = 'rescheduled' THEN
      0
    WHEN dl.state = 'canceled' THEN
      0
    WHEN dl.terminated_at IS NOT NULL THEN
      0
    WHEN lad.loan_type = 'paygo' THEN (
      CASE
      WHEN date(dl.snapshot_at) - date(lad.handover_at_utc) < COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days ) THEN
        0
      WHEN (
          date(dl.snapshot_at) - date(lad.handover_at_utc) - COALESCE(
          CASE
          WHEN cd.country = 'KE'
            AND
            (
              lad.loan_type <> 'paygo'
              OR
              date(dl.snapshot_at) < '2022-03-21'
            )
            THEN
            30.5
          WHEN lad.loan_type = 'paygo'
            AND
            cd.country = 'KE' THEN
            7
          WHEN cd.country IN ('TZ',
                              'RW' )
            AND
            lad.loan_type <> 'paygo' THEN
            30.5
            ELSE soi.down_payment_days
          END
          ,soi.down_payment_days )
        )
        <= extract(day FROM date(dl.snapshot_at)) THEN
        (                   date(dl.snapshot_at) - date(lad.handover_at_utc) - COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days )) * (round(
        CASE
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG'
          AND
          date(lad.handover_at_utc) <= '2022-04-14' THEN
          initial_installment/100/30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG' THEN
          lad.initial_installment/100
        WHEN lad.loan_type = 'arrears'
          AND
          cd.country IN ('UG',
                         'ZM') THEN
          soi.daily_rate              /100
          ELSE lad.initial_installment/100/30.5
        END ))
      WHEN date(dl.snapshot_at) - date(lad.handover_at_utc) + COALESCE(
        CASE
        WHEN cd.country = 'KE'
          AND
          (
            lad.loan_type <> 'paygo'
            OR
            date(dl.snapshot_at) < '2022-03-21'
          )
          THEN
          30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'KE' THEN
          7
        WHEN cd.country IN ('TZ',
                            'RW' )
          AND
          lad.loan_type <> 'paygo' THEN
          30.5
          ELSE soi.down_payment_days
        END
        ,soi.down_payment_days ) > COALESCE(soi.loan_duration,lad.installment_periods * lad.installment_period_days) + extract(day FROM date(dl.snapshot_at)) THEN
        0
        ELSE ((round(
        CASE
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG'
          AND
          date(lad.handover_at_utc) <= '2022-04-14' THEN
          initial_installment/100/30.5
        WHEN lad.loan_type = 'paygo'
          AND
          cd.country = 'UG' THEN
          lad.initial_installment/100
        WHEN lad.loan_type = 'arrears'
          AND
          cd.country IN ('UG',
                         'ZM') THEN
          soi.daily_rate              /100
          ELSE lad.initial_installment/100/30.5
        END )) )
      END )
    WHEN dl.months_on_book < 2 THEN
      0
    WHEN dl.months_on_book - 2 > lad.installment_periods THEN
      0
      ELSE COALESCE(round(
      CASE
      WHEN lad.loan_type = 'paygo'
        AND
        cd.country = 'UG'
        AND
        date(lad.handover_at_utc) <= '2022-04-14' THEN
        initial_installment/100/30.5
      WHEN lad.loan_type = 'paygo'
        AND
        cd.country = 'UG' THEN
        lad.initial_installment/100
      WHEN lad.loan_type = 'arrears'
        AND
        cd.country IN ('UG',
                       'ZM') THEN
        soi.daily_rate              /100
        ELSE lad.initial_installment/100/30.5
      END ),lad.initial_installment/100/30.5 )
    END ) AS expected_in_month_in_repayment


    from analysts_inputs.credit_loan_accounts_daily dl 
	left join powerhub_reporting.eea_loan_account_details lad on dl.loan_account_id = lad.loan_account_id
    left join payments p on p.loan_id = dl.loan_account_id and date(dl.snapshot_at) = p.payment_date 
	left join powerhub_reporting.eea_customer_details cd on cd.customer_id = dl.customer_id
	LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and dl.snapshot_at = date(ph.benchmark_date)
	left join powerhub_reporting.eea_sales_order_items soi on (soi.loan_account_id = dl.loan_account_id AND
                                                        soi.customer_id = dl.customer_id
                                                        and soi.master_loan_item = 1 
                                                        and  soi.account_type = 'CREDIT' )
    WHERE date(dl.snapshot_at) BETWEEN date_trunc('month', (current_date - 1) - interval '1 year')::date
                                  AND ((current_date - 1) - interval '1 year')::date
--	and lower(dl.state) in ('potentially_paid_off', 'exceeded_grace_period', 'in_grace_period', 'exceeded_loan_period', 'maintained')
	and lad.initial_installment > 0 and lad.initial_amount > 0 
    and cd.country = 'UG'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

    UNION ALL
    
    /*ALMS*/
    SELECT
    'alms' as company,
	'last year' as period,
    ll.report_date as report_date,
	ds.generated_date as generated_date,
	l.loan_contract_end_date,
	ll.loan_contract_end_date as new_loan_contract_end,
	alp.benchmark_date as benchmark_date,
    cast(alp.loan_id as varchar) as loan_id,
    cast(alp.customer_id as varchar) as customer_id,
	coalesce(alp.daily_rate/100,ll.daily_rate) as daily_rate,
	alp.loan_state as loan_state,
	ll.loan_state as new_loan_state,
	true as in_repayment,
	true as latest_in_repayment,
	lad.cancel_comment as cancellation_reason,
	coalesce(soi.product_line_name, ll.product) as product,
    coalesce(ph.manager_name, ll.team_lead) AS team_lead,
    coalesce( cd.country, ll.country) as country,
    coalesce(case when cd.country in ('MZ', 'CI') then cd.area2      
              else cd.area1 end, ll.region)  as region,
    p.payment_date,
    p.amount_paid,
	
	-- Calcular expected_all_mtd
    CASE 
    WHEN ll.loan_contract_end_date > COALESCE(alp.benchmark_date, ll.report_date, ds.generated_date) THEN 
        CAST(COALESCE(alp.daily_rate/100, ll.daily_rate) AS DOUBLE PRECISION)
    ELSE 
        CASE 
            WHEN COALESCE(alp.loan_state, ll.loan_state) = 'repayment' THEN 
                SUM(
                    CASE 
                        WHEN COALESCE(alp.days_elapsed, ll.days_elapsed) <= soi.loan_duration + soi.down_payment_days
                        THEN CAST(COALESCE(alp.daily_rate/100, ll.daily_rate) AS DOUBLE PRECISION)
                        ELSE 0 
                    END
                )
            ELSE 0 
        END
    END AS expected_all_mtd,

    -- Calcular expected_in_month_in_repayment
    CASE 
        WHEN alp.loan_state = 'repayment' THEN 
            cast(sum(case when alp.days_elapsed <= (soi.loan_duration + soi.down_payment_days)
                    then alp.daily_rate/100 else 0 end) as float) 
        ELSE 0 
    END AS expected_in_month_in_repayment
	
        FROM date_series ds
	LEFT JOIN latest_bench_alms ll ON ll.rn = 1  and ll.report_date = ds.generated_date 
	LEFT JOIN powerhub_reporting.eea_alms_loan_performance_benchmarks alp on alp.loan_id = ll.loan_id AND date(alp.benchmark_date) = ds.generated_date
    LEFT JOIN powerhub_reporting.eea_loan_account_details lad on lad.loan_account_id = alp.loan_id and lad.customer_id = alp.customer_id
        AND lad.source = 'alms'
    LEFT JOIN powerhub_reporting.eea_customer_details cd on cd.customer_id = alp.customer_id
	LEFT JOIN analysts_inputs.credit_ph_agent_colors_historical ph ON cd.assigned_to_contractor_id = ph.sales_agent_id and alp.benchmark_date = ph.benchmark_date
    LEFT JOIN powerhub_reporting.eea_sales_order_items soi on (lad.sales_case_id = soi.sales_case_id
                                                    and alp.loan_id = soi.loan_account_id)
                                                    
	LEFT JOIN sensitive.finance_global_finance_report_monthly_alms l on alp.loan_id = l.loan_id and ((DATE_TRUNC('MONTH',  COALESCE(alp.benchmark_date, ll.report_date)) - INTERVAL '1 day')::DATE = date(l.report_date))												
    LEFT JOIN payments p on p.loan_id = alp.loan_id and alp.benchmark_date = p.payment_date
    WHERE date(ll.report_date) BETWEEN date_trunc('month', (current_date - 1) - interval '1 year')::date
        AND ((current_date - 1) - interval '1 year')::date
		--and lower(alp.loan_state) in ('repayment', 'active')
    and (cd.country = 'UG' or ll.country = 'UG')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21


)

select 
l.*,
fx.end_of_month_rate as eom_rate_usd
from (
SELECT * FROM current_month
UNION ALL
SELECT * FROM previous_month
UNION ALL
SELECT * FROM previous_year) as l

LEFT JOIN analysts_inputs.market_sources_finance_a_and_r_fx_rates
 fx ON last_day(l.report_date) = fx.month AND 
 (case WHEN l.country = 'BJ' THEN 'Benin'
WHEN l.country = 'CI' THEN 'Côte d''Ivoire'
WHEN l.country = 'KE' THEN 'Kenya'
WHEN l.country = 'MZ' THEN 'Mozambique'
WHEN l.country = 'NG' THEN 'Nigeria'
WHEN l.country = 'RW' THEN 'Rwanda'
WHEN l.country = 'TZ' THEN 'Tanzania'
WHEN l.country = 'UG' THEN 'Uganda'
WHEN l.country = 'ZM' THEN 'Zambia'
ELSE l.country 
END) = fx.country
