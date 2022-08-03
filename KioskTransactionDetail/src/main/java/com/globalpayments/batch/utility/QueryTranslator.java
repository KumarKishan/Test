package com.globalpayments.batch.utility;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class QueryTranslator implements SerializableFunction<String, String> {

	ValueProvider<String> endDate;

	public QueryTranslator(ValueProvider<String> endDate) {
		this.endDate = endDate;
	}

	private static final long serialVersionUID = -2754362391392873056L;

	@Override
	public String apply(String input) {
		String startDate = input;
		String endDate = this.endDate.get();
		
		String queryTickets = String.format(Constants.QUERY_TICKETS+" WHERE insert_date >= '%s' and insert_date <= '%s' AND (transaction_primary = 'Ticket') AND (Track = 1) AND (Transfer = 1) group by mname,client_id,terminal_name,transaction_id,customer_id,CASE WHEN transaction_primary = 'Ticket' THEN 'Ticket' END,CASE WHEN credit = 1 AND action_primary = 'Ticket' THEN identifier_action END,game_date,gaming_day_begins,transaction_subordinate,acctnum,cname,identifier_action",startDate,endDate);
		
		String queryBillBreak=String.format(Constants.QUERY_BILLBREAK+" WHERE insert_date >= '%s' and insert_date <= '%s' AND (transaction_primary = 'Bill-Break' OR transaction_primary = 'Cash Access' OR transaction_primary = 'Wager Account Withdrawal') AND (Track = 1) AND (Transfer = 1) Group by mname,client_id,terminal_name,transaction_id,customer_id,game_date,gaming_day_begins,transaction_primary,transaction_subordinate,acctnum,cname,CASE WHEN transaction_primary = 'Ticket' THEN 'Ticket' WHEN transaction_primary = 'Bill-Break' THEN 'Bill Break' WHEN transaction_primary = 'Cash Access' AND transaction_subordinate <> 'Credit Card Cash Advance' THEN transaction_subordinate WHEN transaction_primary = 'Cash Access' AND transaction_subordinate = 'Credit Card Cash Advance' THEN 'CCCA' WHEN transaction_primary = 'Wager Account Withdrawal' THEN 'Wager Acct' ELSE 'Other' END ",startDate,endDate);
		
		String queryCashAccess=String.format(Constants.QUERY_CASHACCESS+" WHERE  insert_date >= '%s' and insert_date <= '%s' AND (Track = '1') AND  (transaction_primary = 'Cash Access') AND (transaction_subordinate = 'Credit POS' OR transaction_subordinate = 'Debit POS') Group by mname,client_id,terminal_name,acctnum,transaction_id,customer_id,game_date,gaming_day_begins,cname,CASE WHEN credit = 1 AND action_primary='Ticket' THEN identifier_action END,transaction_primary,transaction_subordinate,identifier_action ",startDate,endDate);
		
		String query = queryTickets+ " Union "+queryBillBreak+" Union "+queryCashAccess ;
//		return String.format(Constants.SQLQUERY + " where insert_date >= '%s' and insert_date <= '%s' ;", startDate,
//				endDate);
		return query;
	}
}
