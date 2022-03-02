/*  
    Table mt4_data_mart_reports.sp_hist_account_pnl saves the daily pnl amounts of each and every one trading account.
    All amounts are calculated on the fly from the mt4_data_mart_reports.account_pnl view (which sums up all appropriate records from the data_mart.trades table).
    It calculates deposits, balance_from_bonus,bonus, balance_fixed, inactivity_fee, interest, brokeree_fee, open_trade_totals, close_trade_totals and equity
    It saves the results every night using the cron job SELECT cron.schedule('59 20 * * *', 'CALL mt4_data_mart_reports.sp_hist_unrealised_pnl();');

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
    Effective Date  Profile ID 									Login	Regulator 	IB vs Retail A/C Currency   deposits 	balance_from_bonus 	bonus 	balance_fixed 	inactivity_fee  interest brokeree_fee open_trade_totals close_trade_totals equity
	2022-02-16		9e7593e6-9297-4fc6-96f7-0d206838dc83		2831104	SI			IB			 USD				100.0				17.18	155.57			0.0				0.0			0.0		 -33.22		  	133.345			0.0				372.88
	2022-02-16		PLAYER-8b5c6d35-74de-4edd-90c0-56190b1c93f2	100722	CY			RETAIL		 EUR				  0.0				 0.0	  0.0			0.0			  -50.0			0.0			0.0		 -4,750.51 			0.0				441.46
	2022-02-16		PLAYER-d493c6bc-c11e-40c6-8441-4ec756ce04b3	2184566	SI			RETAIL		 EUR			 21,445.0			   871.64	  0.0	    8,669.41			0.0			0.0			0.0		-30,985.75 			0.0				  0.3
	2022-02-16		PLAYER-f88a06a3-e396-46d8-a144-83e8930f2b46	1740021	CY			IB			 USD				  0.0				 0.0	  0.0			0.0				0.0			0.0			0.0		   -999.65			0.0				  0.35
	2022-02-16		PLAYER-8257ced6-1e99-4ffc-a0c6-fc4e6bdff008	1997124	SI			RETAIL		 EUR			-11,151.77				 0.0	  0.0	   -3,046.86		 -130.0			0.0			0.0		 14,380.85			0.0				 52.22
	2022-02-16		PLAYER-55612e20-4037-4e08-b83a-9e0a4261677a	2496307	SI			RETAIL		 EUR				260.0				 0.0	  0.0			0.0			 -195.0			0.42		0.0			  0.0			0.0				 65.0
*/

CREATE TABLE mt4_data_mart_reports.tbl_hist_account_pnl (
	effective_date date NOT NULL,
	player_id varchar NULL,
	login int4 NOT NULL,
	regulator varchar(16) NOT NULL,
	ib_vs_retail varchar(8) NULL,
	currency varchar(16) NULL,
	deposits float8 NULL,
	balance_from_bonus float8 NULL,
	bonus float8 NULL,
	balance_fixed float8 NULL,
	inactivity_fee float8 NULL,
	interest float8 NULL,
	brokeree_fee float8 NULL,
	open_trade_totals float8 NULL,
	close_trade_totals float8 NULL,
	equity float8 NULL
);

CREATE OR REPLACE PROCEDURE mt4_data_mart_reports.sp_hist_account_pnl()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	INSERT INTO mt4_data_mart_reports.tbl_hist_account_pnl 
		SELECT CURRENT_DATE, *
		FROM mt4_data_mart_reports.account_pnl
		WHERE ABS(equity) > 0.1;
END;
$procedure$
;