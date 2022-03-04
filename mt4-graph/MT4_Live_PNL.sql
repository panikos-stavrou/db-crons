WITH sums AS (
WITH totals AS 
(
-- sum up all of the closed trades since end of day yesterday, '2 HOUR'::INTERVAL is used to offset server vs local time
SELECT SUM((profit + commission + "storage")  / lr.fx_to_eur) AS closed_today_pnl, 0 AS balance_transactions_pnl
FROM data_mart.trades t 
LEFT JOIN data_mart.users u ON u.login = t.login
LEFT JOIN data_mart."groups" g ON g.group_name = u."group"
LEFT JOIN mt5_data_mart_reports.live_rates lr ON lr.currency = g.currency 
WHERE u."group" NOT ILIKE '%TEST%' AND g.currency IS NOT NULL AND t.cmd IN (0, 1)
	AND TO_TIMESTAMP(t.closing_time) > CURRENT_DATE + '2 HOUR'::INTERVAL
	
UNION  
-- sum up all balance transactions since end of day yesterday, '2 HOUR'::INTERVAL is used to offset server vs local time
-- this includes inactivity fees, interest given, balance from bonus, balance fixed and brokeree fee charges
-- credit transactions (bonus cancelled, removal, etc.) are not included in the pnl calculations
SELECT 0 AS closed_today_pnl, SUM(profit) AS balance_transactions_pnl
FROM data_mart.trades t 
LEFT JOIN data_mart.users u ON u.login = t.login
LEFT JOIN data_mart."groups" g ON g.group_name = u."group"
LEFT JOIN mt5_data_mart_reports.live_rates lr ON lr.currency = g.currency 
WHERE u."group" NOT ILIKE '%TEST%' AND g.currency IS NOT NULL AND t.cmd IN (6, 7)
	AND TO_TIMESTAMP(t.closing_time) > CURRENT_DATE  AND TO_TIMESTAMP(t."timestamp") > CURRENT_DATE + '2 HOUR'::INTERVAL	
	AND t."comment" LIKE ANY(ARRAY['%IAC%', 'INT01', 'INT001', '%balance from bonus%', 'balance fixed', 'fee #%'])
)
--combine closed trades and balance transactions totals to get total closed pnl since eod yesterday 
SELECT 0 AS eod_open_pnl, SUM(closed_today_pnl) + SUM(balance_transactions_pnl) AS closed_pnl, 0 AS open_pnl_eur
FROM totals
UNION
SELECT 0 AS eod_open_pnl, 0 AS closed_pnl, SUM((t.profit + t.commission + t."storage") / lr.fx_to_eur) AS open_pnl_eur
FROM data_mart.trades t 
LEFT JOIN data_mart.users u ON u.login = t.login
LEFT JOIN data_mart."groups" g ON g.group_name = u."group"
LEFT JOIN mt5_data_mart_reports.live_rates lr ON lr.currency = g.currency 
WHERE u."group" NOT ILIKE '%TEST%' AND g.currency IS NOT NULL AND t.cmd IN (0, 1) AND t.closing_time = 0
UNION
SELECT SUM(open_pnl / lr.fx_to_eur) AS eod_open_pnl, 0 AS closed_pnl, 0 AS open_pnl_eur
FROM mt4_data_mart_reports.tbl_hist_daily_pnl thpl
LEFT JOIN mt5_data_mart_reports.live_rates lr ON lr.currency = thpl.currency 
WHERE effective_date = CURRENT_DATE - INTERVAL '1 DAY')
SELECT SUM(open_pnl_eur - eod_open_pnl + closed_pnl) * -1 AS live_pnl_eur
FROM sums;