/* 	
	Table mt4_data_mart_reports.tbl_hist_symbol_pnl calculates the daily pnl amounts grouped by 1. Symbol 2. Account Currency
	It saves the results every night using the cron job SELECT cron.schedule('10 21 * * *', 'CALL mt4_data_mart_reports.sp_hist_symbol_pnl();');

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
	Effective Date  Symbol		A/C Currency	Closed P&L  	Open P&L
	2022-03-01		AAL.UK		GBP					 10.33				0.0
	2022-03-01		AAL.UK		USD					248.59				0.0
	2022-03-01		AAL.US		EUR				  1,813.64				0.0
	2022-03-01		AAL.US		GBP				  7,750.71				0.0
	2022-03-01		AAL.US		USD				 -3,038.46			 -125.1
*/

CREATE TABLE mt4_data_mart_reports.tbl_hist_symbol_pnl (
	effective_date date NOT NULL,
	symbol varchar(16) NULL,
	currency varchar(16) NOT NULL,
	closed_pnl float8 NOT NULL,
	open_pnl float8 NULL
);

CREATE OR REPLACE PROCEDURE mt4_data_mart_reports.sp_hist_symbol_pnl()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

INSERT INTO mt4_data_mart_reports.tbl_hist_symbol_pnl

SELECT CURRENT_DATE AS effective_date, 
		ins.symbol AS symbol,
		g.currency AS currency,
		ROUND(COALESCE(SUM(CASE WHEN t.closing_time > 0 THEN (t.profit + t."storage" + t.commission) / lr.fx_to_eur END), 0)::numeric, 2)::double precision AS closed_pnl,
		ROUND(COALESCE(SUM(CASE WHEN t.closing_time = 0 THEN (t.profit + t."storage" + t.commission) / lr.fx_to_eur END), 0)::numeric, 2)::double precision AS open_pnl
FROM data_mart.trades t 
LEFT JOIN data_mart.users u ON u.login = t.login
LEFT JOIN data_mart."groups" g ON g.group_name = u."group" 
LEFT JOIN data_mart.tbl_instruments ins ON ins.symbol = t.symbol
LEFT JOIN mtx_data_mart.live_rates lr ON lr.currency = g.currency 
WHERE t.cmd IN (0, 1) AND u."group" NOT ILIKE '%TEST%' AND g.currency IS NOT NULL AND ins.symbol IS NOT NULL 
GROUP BY ins.symbol, g.currency
ORDER BY ins.symbol, g.currency;

END
$procedure$
;