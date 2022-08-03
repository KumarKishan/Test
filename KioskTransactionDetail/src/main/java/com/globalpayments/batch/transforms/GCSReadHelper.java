package com.globalpayments.batch.transforms;


import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globalpayments.batch.pipeline.TransactionDetail;
import com.globalpayments.batch.utility.Constants;
import com.google.common.io.ByteStreams;

public class GCSReadHelper 
{
	private static final Logger LOG = LoggerFactory.getLogger(TransactionDetail.class);
	static String url = null;
	static String userName = null;
	static String password = null;
	static String tableDetails = null;
	static String baseQuery = null;
	static String dataSet = null;
	static String trustedDataset = null;
	static String transformedDataset = null;
	static String tablename = null;

	@SuppressWarnings("deprecation")
	public HashMap<String,String> readFromGCSBucket(Map<String, ValueProvider<String>> optionsMap) throws IOException
	{
		HashMap<String,String> gamingConfig = new HashMap<String,String>();
		try
		{
			MatchResult listResult = FileSystems.match(String.valueOf(optionsMap.get(Constants.GAMINGCONFIGPATH)));
			listResult
			    .metadata()
			    .forEach(
			        metadata -> {
			          ResourceId resourceId = metadata.resourceId();
			          System.out.println(resourceId.toString());
			        });

			ResourceId existingFileResourceId = FileSystems
			    .matchSingleFileSpec(String.valueOf(optionsMap.get(Constants.GAMINGCONFIGPATH)))
			    .resourceId();

			try (ByteArrayOutputStream out = new ByteArrayOutputStream();
			    ReadableByteChannel readerChannel = FileSystems.open(existingFileResourceId);
			    WritableByteChannel writerChannel = Channels.newChannel(out)) 
				{
					ByteStreams.copy(readerChannel, writerChannel);										
					url= out.toString().split("\n")[0].split(Constants.APP_CONFIG_DELIMITER)[1].trim();
					userName = out.toString().split("\n")[2].split(Constants.APP_CONFIG_DELIMITER)[1].trim();
					password = out.toString().split("\n")[3].split(Constants.APP_CONFIG_DELIMITER)[1].trim();					
				}
			if(url!=null && !"".equalsIgnoreCase(url) 
					&& userName!=null && !"".equalsIgnoreCase(userName)
					&& password!=null && !"".equalsIgnoreCase(password))
			{
				gamingConfig.put(Constants.URL, url);
				gamingConfig.put(Constants.USERNAME, userName);
				gamingConfig.put(Constants.PASSWORD, password);
				
				//LOG.info("gamingConfig::::::::::: " + gamingConfig);
				return gamingConfig;
			}
			else
			{
				return null;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			LOG.error("Failed to read the gamingConfig from GCS Folder. Please ensure the filepath");
			return null;
		}
	}

}
