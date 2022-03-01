/*  
    Table mt4_data_mart_reports.tbl_hist_unrealised_pnl calculates the daily unrealized pnl amount grouped by Account currency
    It saves the results every night using the cron job SELECT cron.schedule('30 20 * * *', 'CALL mt4_data_mart_reports.sp_hist_unrealised_pnl();');

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
    Effective Date  A/C Currency  Unrealised P&L
    2022-03-01      EUR             -114,360.0
    2022-03-01      USD             -403,920.0  
*/

CREATE TABLE mt4_data_mart_reports.tbl_hist_unrealised_pnl (
    effective_date date NOT NULL,
    currency varchar(6) NOT NULL,
    unrealised_pnl float8 NULL,
    CONSTRAINT tbl_hist_unrealised_pnl_pkey PRIMARY KEY (effective_date,currency)
);

CREATE OR REPLACE PROCEDURE mt4_data_mart_reports.sp_hist_unrealised_pnl()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

INSERT INTO mt4_data_mart_reports.tbl_hist_unrealised_pnl

SELECT CURRENT_DATE, SUM((t.profit + t."storage" + t.commission) / lr.fx_to_eur) AS unrealised_pnl
FROM data_mart.trades t
LEFT JOIN data_mart.users u ON u.login = t.login 
LEFT JOIN data_mart."groups" g ON g.group_name = u."group" 
LEFT JOIN mtx_data_mart.live_rates lr ON lr.currency = g.currency 
WHERE t.cmd IN (0,1) AND t.closing_time = 0 AND u."group" NOT ILIKE '%TEST%';


END;
$procedure$
;