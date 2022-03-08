/*  
    Table mt4_data_mart_reports.tbl_live_account_pnl saves LIVE, ON DEMAND, the P&L Change and Equity in EUR amounts for each and every one trading account.
    Amounts are grouped by Login, Regulator, IB vs Retail. (Regulator and IB vs Retail are included because they are used to drive the 
    MT4 Live Equity by License € and MT4 Live Equity by IB vs Retail € Management Reports Dashboard)
    All amounts are calculated on the fly from the mt4_data_mart_reports.account_pnl view (which sums up all appropriate records from the data_mart.trades table).
    It calculates P&L Change and Equity for 4 Different Periods: Live, Daily, Weekly, Biweekly and Monthly

	The DAILY amounts are being used on two MT4 Management Reports Dashboard:
	1. MT4 Live Top 10 Losing  Clients €
	2. MT4 Live Top 10 Winning Clients €

    Sample Return:
    Login	Regulator, IB vs Retail   P&L Change Equity EUR   Effective Period
	1467623	  	CY	RETAIL				   665.0	   665.0		  LIVE
	1479722	 	SI  IB					 1,080.46	     0.46		  LIVE
	....
	2831216	 	CY  IB					-1,310.18	   255.25	    WEEKLY
	2768978		SI  RETAIL					 0.0	   202.36		WEEKLY
	...
	2167672	   	SI  RETAIL				  -427.67	 9,184.62	  BIWEEKLY
	2676620	    CY  IB						 0.0	 1,184.76	  BIWEEKLY
*/

CREATE OR REPLACE PROCEDURE mt4_data_mart_reports.sp_live_account_pnl()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

TRUNCATE TABLE mt4_data_mart_reports.tbl_live_account_pnl;

-- live account pnl 
INSERT INTO mt4_data_mart_reports.tbl_live_account_pnl
SELECT ap."login", ap.regulator, ap.ib_vs_retail, 
		ROUND(SUM((equity - deposits) / lr.fx_to_eur)::NUMERIC, 2) AS pnl_diff, 
		ROUND(SUM(equity / lr.fx_to_eur)::NUMERIC, 2) AS equity_eur,
		'LIVE' AS effective_period 
FROM mt4_data_mart_reports.account_pnl ap
LEFT JOIN public.mt4_mt5_account_mappings am ON am.login = ap.login 
LEFT JOIN mtx_data_mart.live_rates lr ON am.currency = lr.currency 
WHERE ABS(equity - deposits) > 0.1 
GROUP BY ap."login", ap.regulator, ap.ib_vs_retail;

-- daily
INSERT INTO mt4_data_mart_reports.tbl_live_account_pnl
WITH yesterday_account_pnl AS 
(
SELECT hap."login", hap.regulator, hap.ib_vs_retail,
		ROUND(SUM((equity - deposits) / lr.fx_to_eur)::NUMERIC, 2) AS pnl_change
FROM mt4_data_mart_reports.tbl_hist_account_pnl hap
LEFT JOIN public.mt4_mt5_account_mappings am ON am.login = hap.login 
LEFT JOIN mtx_data_mart.live_rates lr ON am.currency = lr.currency 
WHERE effective_date  = CURRENT_DATE - INTERVAL '1 DAY'
GROUP BY hap."login", hap.regulator, hap.ib_vs_retail
)
SELECT lap."login", lap.regulator, lap.ib_vs_retail,
		SUM(lap.pnl_change - yap.pnl_change) AS pnl_diff, 
		lap.equity_eur AS equity_eur,
		'DAILY' AS effective_period 
FROM mt4_data_mart_reports.tbl_live_account_pnl lap 
INNER JOIN yesterday_account_pnl yap ON yap."login" = lap."login"
WHERE lap.effective_period = 'LIVE'
GROUP BY lap."login", lap.regulator, lap.ib_vs_retail, lap.equity_eur
ORDER BY 2;

-- weekly
INSERT INTO mt4_data_mart_reports.tbl_live_account_pnl
WITH yesterday_account_pnl AS 
(
SELECT hap."login", hap.regulator, hap.ib_vs_retail, 
		ROUND(SUM((equity - deposits) / lr.fx_to_eur)::NUMERIC, 2) AS pnl_change
FROM mt4_data_mart_reports.tbl_hist_account_pnl hap
LEFT JOIN public.mt4_mt5_account_mappings am ON am.login = hap.login 
LEFT JOIN mtx_data_mart.live_rates lr ON am.currency = lr.currency 
WHERE effective_date  = CURRENT_DATE - INTERVAL '7 DAY'
GROUP BY hap."login", hap.regulator, hap.ib_vs_retail
)
SELECT lap."login", lap.regulator, lap.ib_vs_retail,
		SUM(lap.pnl_change - yap.pnl_change) AS pnl_diff, 
		lap.equity_eur AS equity_eur,
		'WEEKLY' AS effective_period 
FROM mt4_data_mart_reports.tbl_live_account_pnl lap 
INNER JOIN yesterday_account_pnl yap ON yap."login" = lap."login"
WHERE lap.effective_period = 'LIVE'
GROUP BY lap."login", lap.regulator, lap.ib_vs_retail, lap.equity_eur
ORDER BY 2;

-- biweekly
INSERT INTO mt4_data_mart_reports.tbl_live_account_pnl
WITH yesterday_account_pnl AS 
(
SELECT hap."login", hap.regulator, hap.ib_vs_retail, 
		ROUND(SUM((equity - deposits) / lr.fx_to_eur)::NUMERIC, 2) AS pnl_change
FROM mt4_data_mart_reports.tbl_hist_account_pnl hap
LEFT JOIN public.mt4_mt5_account_mappings am ON am.login = hap.login 
LEFT JOIN mtx_data_mart.live_rates lr ON am.currency = lr.currency 
WHERE effective_date  = CURRENT_DATE - INTERVAL '15 DAY'
GROUP BY hap."login", hap.regulator, hap.ib_vs_retail
)
SELECT lap."login", lap.regulator, lap.ib_vs_retail,
		SUM(lap.pnl_change - yap.pnl_change) AS pnl_diff, 
		lap.equity_eur AS equity_eur,
		'BIWEEKLY' AS effective_period 
FROM mt4_data_mart_reports.tbl_live_account_pnl lap 
INNER JOIN yesterday_account_pnl yap ON yap."login" = lap."login"
WHERE lap.effective_period = 'LIVE'
GROUP BY lap."login", lap.regulator, lap.ib_vs_retail, lap.equity_eur
ORDER BY 2;

-- monthly
INSERT INTO mt4_data_mart_reports.tbl_live_account_pnl
WITH yesterday_account_pnl AS 
(
SELECT hap."login", hap.regulator, hap.ib_vs_retail,
		ROUND(SUM((equity - deposits) / lr.fx_to_eur)::NUMERIC, 2) AS pnl_change
FROM mt4_data_mart_reports.tbl_hist_account_pnl hap
LEFT JOIN public.mt4_mt5_account_mappings am ON am.login = hap.login 
LEFT JOIN mtx_data_mart.live_rates lr ON am.currency = lr.currency 
WHERE effective_date  = CURRENT_DATE - INTERVAL '30 DAY'
GROUP BY hap."login", hap.regulator, hap.ib_vs_retail
)
SELECT lap."login", lap.regulator, lap.ib_vs_retail,
		SUM(lap.pnl_change - yap.pnl_change) AS pnl_diff, 
		lap.equity_eur AS equity_eur,
		'MONTHLY' AS effective_period 
FROM mt4_data_mart_reports.tbl_live_account_pnl lap 
INNER JOIN yesterday_account_pnl yap ON yap."login" = lap."login"
WHERE lap.effective_period = 'LIVE'
GROUP BY lap."login", lap.regulator, lap.ib_vs_retail, lap.equity_eur
ORDER BY 2;

END;
$procedure$
;