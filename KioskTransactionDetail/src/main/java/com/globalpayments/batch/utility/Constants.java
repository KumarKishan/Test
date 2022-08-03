package com.globalpayments.batch.utility;

import java.util.regex.Pattern;

public class Constants {
	public static final String NULL = "NULL";
	public static final String BQ_TIMESTAMP = "yyyy-MM-dd HH:mm:ss";
	public static final String DATA_TIMESTAMP = "yyyyMMddHHmmssSSS";
	public static final String ETLBATCHID = "etlbatchid";
	public static final String SOURCE = "Source";
	public static final String TARGET = "Target";
	
	public static final String DELIMITER = "delimiter";
	public static final String DATEFORMAT = "date_format";
	public static final String DESTINATION_SPLIT = Pattern.quote(".");
	public static final String BLANK = "";

	public static final String URL = "Url";
	
	public static final String QUERY_TICKETS="SELECT mname,client_id,terminal_name,MIN(action_timestamp) as action_timestamp,transaction_id,customer_id,game_date,gaming_day_begins,transaction_primary,transaction_subordinate,acctnum,cname,CASE WHEN transaction_primary = 'Ticket' THEN 'Ticket' END AS type,MAX(CASE  WHEN action_primary = 'Handpay' THEN  concat(action_subordinate,' Receipt') WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN 'Wager Acct Deposit' WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN 'Ticket Payout' ELSE '1' END) AS sub_type,MAX(CASE WHEN action_primary = 'Handpay' THEN Cast(transaction_id as char) WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN identifier_account WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN identifier_action END) AS sub_account,SUM(CASE WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN value * .01 ELSE 0 END)  + SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Bill Dispenser' THEN (value * count) * .01 ELSE 0 END) + SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Coin Hopper' THEN (value * count) * .01 ELSE 0 END) + SUM(CASE WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN value * .01 ELSE 0 END) AS total,CASE WHEN credit = 1 AND action_primary = 'Ticket' THEN identifier_action END AS account,SUM(CASE WHEN credit = 1 THEN value  * .01 ELSE 0 END) AS amt,SUM(CASE WHEN credit = 1 THEN value * .01 ELSE 0 END) AS sub_amt,SUM(CASE WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN value * .01 ELSE 0 END)  AS wageract,SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Bill Dispenser' THEN (value * count) * .01 ELSE 0 END) AS bills,SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Coin Hopper' THEN (value * count) * .01 ELSE 0 END) AS coins,SUM(CASE WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN value * .01 ELSE 0 END)   AS tickets,SUM(CASE WHEN action_primary = 'Handpay' THEN value * .01 ELSE 0 END) AS handpay,client_id as cguid ,NULL as auth,NULL as fee, max(insert_date) FROM vw_kiosk_data ";
	public static final String QUERY_BILLBREAK="SELECT mname,client_id,terminal_name,MIN(action_timestamp) as action_timestamp,transaction_id,customer_id,game_date,gaming_day_begins,transaction_primary,transaction_subordinate,acctnum,cname,CASE WHEN transaction_primary = 'Ticket' THEN 'Ticket' WHEN transaction_primary = 'Bill-Break' THEN 'Bill Break' WHEN transaction_primary = 'Cash Access' AND transaction_subordinate <> 'Credit Card Cash Advance' THEN transaction_subordinate WHEN transaction_primary = 'Cash Access' AND transaction_subordinate = 'Credit Card Cash Advance' THEN 'CCCA' WHEN transaction_primary = 'Wager Account Withdrawal' THEN 'Wager Acct' ELSE 'Other' END AS type,MAX(CASE  WHEN action_primary = 'Handpay' THEN  concat(action_subordinate,' Receipt') WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN 'Wager Acct Deposit' WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN 'Ticket Payout' ELSE '1' END) AS sub_type,MAX(CASE WHEN action_primary = 'Handpay' THEN transaction_id WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN identifier_account WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN identifier_action END) AS sub_account,SUM(CASE WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN Value * .01 ELSE 0 END)+ SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Bill Dispenser' THEN (Value * Count) * .01 ELSE 0 END) + SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Coin Hopper' THEN (Value * Count) * .01 ELSE 0 END) +  SUM(CASE WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN Value * .01 ELSE 0 END)  AS total,MAX(CASE WHEN credit = 1 AND action_primary = 'Ticket' THEN identifier_action WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN NULL ELSE identifier_account END) AS account,SUM(CASE WHEN credit = 1 THEN Value * .01  ELSE 0 END) AS amt,NULL as sub_amt,SUM(CASE WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN Value * .01 ELSE 0 END)AS wageract,SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Bill Dispenser' THEN (Value * Count) * .01 ELSE 0 END)  AS bills,SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Coin Hopper' THEN (Value * Count) * .01 ELSE 0 END) AS coins,SUM(CASE WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN Value * .01 ELSE 0 END)  AS tickets,SUM(CASE WHEN action_primary ='Handpay' THEN Value * .01 ELSE 0 END) AS handpay,client_id as cguid ,MAX(CASE WHEN credit = 1 AND action_primary <> 'Ticket' THEN identifier_action END) as auth ,SUM(CASE WHEN credit = 1 THEN Fee * .01  ELSE 0 END) AS fee, max(insert_date) FROM vw_kiosk_data ";
	public static final String QUERY_CASHACCESS="SELECT mname,client_id,terminal_name,MIN(action_timestamp) as action_timestamp,transaction_id,customer_id,game_date,gaming_day_begins,transaction_primary,transaction_subordinate,acctnum,cname,transaction_subordinate as type,MAX(CASE  WHEN action_primary = 'Handpay' THEN  concat(action_subordinate, ' Receipt') WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN 'Wager Acct Deposit' WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN 'Ticket Payout' ELSE '1' END) AS sub_type,MAX(CASE WHEN action_primary = 'Handpay' THEN transaction_id WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN identifier_account WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN identifier_action END) AS sub_account,SUM(CASE WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN Value * .01 ELSE 0 END) + SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Bill Dispenser' THEN (Value * Count) * .01 ELSE 0 END) + SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Coin Hopper' THEN (Value * Count) * .01 ELSE 0 END) + SUM(CASE WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN Value * .01 ELSE 0 END) AS total,CASE WHEN credit = 1 AND action_primary = 'Ticket' THEN identifier_action END AS account,SUM(CASE WHEN credit = 1 THEN Value * .01  ELSE 0 END) AS amt,SUM(CASE WHEN credit = 1 THEN Value * .01 ELSE 0 END) AS sub_amt,SUM(CASE WHEN action_primary = 'Wager Account' AND transaction_primary <> 'Wager Account Withdrawal' THEN Value * .01 ELSE 0 END) AS wageract,SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Bill Dispenser' THEN (Value * Count) * .01 ELSE 0 END) AS bills,SUM(CASE WHEN action_primary = 'Currency' AND device_name = 'Coin Hopper' THEN (Value * Count) * .01 ELSE 0 END) AS coins,SUM(CASE WHEN action_primary = 'Ticket' AND device_name = 'Ticket Printer' THEN Value * .01 ELSE 0 END) AS tickets,SUM(CASE WHEN action_primary = 'Handpay' THEN Value * .01 ELSE 0 END) AS handpay,client_id as cguid,MAX(CASE WHEN credit = 1 AND action_primary <> 'Ticket' THEN identifier_action END) AS auth,SUM(CASE WHEN credit = 1 THEN fee * .01  ELSE 0 END) AS fee, max(insert_date) FROM vw_kiosk_data ";
	
	public static final String USERNAME = "UserName";
	public static final String PASSWORD = "Password";
	public static final String PROJECT = "Project";
	public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	public static final String GAMINGCONFIGPATH = "gamingConfigPath";
	public static final String APP_CONFIG_DELIMITER = Pattern.quote("*");
	
	public static final String Base_Query = "insert into gaming_consumption_layer.rpt_kiosk_transaction_details";
	
	
}