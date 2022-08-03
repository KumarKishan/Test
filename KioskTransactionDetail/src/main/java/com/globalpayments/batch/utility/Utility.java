package com.globalpayments.batch.utility;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Utility {

	public static final Logger LOG = LoggerFactory.getLogger(Utility.class);	
	
	
		
	public static Connection connectToCloudSql(String url, String username, String password) throws SQLException
	{
		try{
			
			return DriverManager.getConnection(url,username,password);
		}
		catch(SQLException s)
		{
			LOG.error(String.format("SQL exception. Could not establish connection to Cloud SQL: %s", s.getMessage(),s.toString()));
			return null;
		}
		catch(Exception e)
		{
			LOG.error(String.format("Exception. Could not establish connection to Cloud SQL: %s",e.getMessage(), e.toString()));
			return null;
		}
	}
	
	public static String getCurrentDateTime(){
		return new SimpleDateFormat(Constants.BQ_TIMESTAMP).format(new Date());
	}
	
	public static BigInteger getETLBatchID() {
	return new BigInteger(new SimpleDateFormat(Constants.DATA_TIMESTAMP).format(new Date()));
}
	
}	