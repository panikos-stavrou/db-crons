/* 	
	Table mt4_data_mart_reports.tbl_hist_daily_pnl calculates the daily pnl amounts grouped by 1. Regulator 2. IB vs Retail 3. Account Currency
	It saves the results every night using the cron job SELECT cron.schedule('10 21 * * *', 'CALL mt4_data_mart_reports.sp_hist_daily_pnl();');

	# ┌───────────── minute (0 - 59)
	# │ ┌───────────── hour (0 - 23)
	# │ │ ┌───────────── day of the month (1 - 31)
	# │ │ │ ┌───────────── month (1 - 12)
	# │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
	# │ │ │ │ │                                   7 is also Sunday on some systems)
	# │ │ │ │ │
	# │ │ │ │ │
	# * * * * * <command to execute>

	Sample Return:
	Effective Date  Regulator	IB vs Retail 	A/C Currency	Closed P&L  	Open P&L
	2022-03-01		CY			IB				EUR					-96,224			 -10
	2022-03-01		CY			IB				GBP		 		      5,147		  	   0
	2022-03-01		CY			RETAIL			USD				   -130,610	 	-234,501
	2022-03-01		SI			IB				EUR				 -6,732,773		  -4,762
	2022-03-01		SI			IB				USD				 -4,441,224		 -74,991
	2022-03-01		SI			RETAIL			GBP				-10,621,439	 		   0
*/

CREATE TABLE mt4_data_mart_reports.tbl_hist_daily_pnl (
	effective_date date NOT NULL,
	regulator varchar(16) NOT NULL,
	ib_vs_retail varchar(16) NOT NULL,
	currency varchar(16) NOT NULL,
	closed_pnl float8 NULL,
	open_pnl float8 NULL
);

CREATE OR REPLACE PROCEDURE mt4_data_mart_reports.sp_hist_daily_pnl()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

INSERT INTO mt4_data_mart_reports.tbl_hist_daily_pnl

SELECT CURRENT_DATE AS effective_date, regulator, ib_vs_retail, currency, 
		ROUND(SUM(closed_pnl)) AS closed_pnl, ROUND(SUM(open_pnl)) AS open_pnl
FROM (
-- closed trade pnl
SELECT  
	CASE WHEN SUBSTRING(u."group" FROM 1 FOR 2) = 'SI' THEN 'SI' ELSE 'CY' END AS regulator,
	CASE WHEN u.agent_account > 0 THEN 'IB'::TEXT ELSE 'RETAIL'::TEXT END AS ib_vs_retail,
	g.currency AS currency, 
	ROUND(SUM(CASE WHEN t.cmd = 6 AND (t."comment"::text ~~* ANY ('{%IAC%,%INT0%,%balance from bonus%,%balance fixed%,%fee #%}'::text[])) THEN t.profit ELSE 0 END + 
		CASE WHEN t.cmd IN (0, 1) THEN (t.profit + t.commission + t."storage") ELSE 0 END + 		
		CASE WHEN t.cmd = 7 THEN t.profit ELSE 0 END)::NUMERIC, 2) AS closed_pnl,  
	0 AS open_pnl
FROM data_mart.trades t 
LEFT JOIN data_mart.users u ON u.login = t.login
LEFT JOIN data_mart."groups" g ON g.group_name = u."group"
WHERE u."group" NOT ILIKE '%TEST%' AND g.currency IS NOT NULL AND t.cmd IN (0, 1, 6, 7) AND t.closing_time > 0
GROUP BY regulator, ib_vs_retail, g.currency
UNION
-- open trade pnl
SELECT 
	CASE WHEN SUBSTRING(u."group" FROM 1 FOR 2) = 'SI' THEN 'SI' ELSE 'CY' END AS regulator,
	CASE WHEN u.agent_account > 0 THEN 'IB'::TEXT ELSE 'RETAIL'::TEXT END AS ib_vs_retail,
	g.currency AS currency, 0 AS closed_pnl,
	ROUND(SUM(CASE WHEN t.cmd = 6 AND (t."comment"::text ~~* ANY ('{%IAC%,%INT0%,%balance from bonus%,%balance fixed%,%fee #%}'::text[])) THEN t.profit ELSE 0 END + 
		CASE WHEN t.cmd IN (0, 1) THEN (t.profit + t.commission + t."storage") ELSE 0 END + 		
		CASE WHEN t.cmd = 7 THEN t.profit ELSE 0 END)::NUMERIC, 2) AS open_pnl 
FROM data_mart.trades t 
LEFT JOIN data_mart.users u ON u.login = t.login
LEFT JOIN data_mart."groups" g ON g.group_name = u."group"
WHERE u."group" NOT ILIKE '%TEST%' AND g.currency IS NOT NULL AND t.cmd IN (0, 1) AND t.closing_time = 0
GROUP BY regulator, ib_vs_retail, g.currency	
) totals
GROUP BY regulator, ib_vs_retail, currency;

END;
$procedure$
;