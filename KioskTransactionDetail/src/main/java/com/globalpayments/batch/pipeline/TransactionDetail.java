package com.globalpayments.batch.pipeline;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globalpayments.batch.airflow.AirflowOptions;
import com.globalpayments.batch.exception.MappingFileException;
import com.globalpayments.batch.transforms.CurrentDateTimeDoFn;
import com.globalpayments.batch.transforms.EtlBatchIdDoFn;
import com.globalpayments.batch.transforms.FinalizeDumpData;
import com.globalpayments.batch.transforms.GCSReadHelper;
import com.globalpayments.batch.utility.Constants;
import com.globalpayments.batch.utility.QueryTranslator;
import com.google.api.services.bigquery.model.TableRow;

public class TransactionDetail {
	static Logger log  = LoggerFactory.getLogger(TransactionDetail.class);

	static String createdDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

	public static void main(String[] args) throws Exception {

		Logger log = LoggerFactory.getLogger(TransactionDetail.class);
		AirflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(AirflowOptions.class);

		Pipeline pipeline = Pipeline.create(options);

		Map<String, ValueProvider<String>> optionsMap = new HashMap<>();
		optionsMap.put(Constants.GAMINGCONFIGPATH, options.getGamingConfigPath());

		HashMap<String, String> gamingConfig = new GCSReadHelper().readFromGCSBucket(optionsMap);

		PCollectionView<Map<String, String>> mappingData = pipeline
				.apply("Read Mapping file from GCS", TextIO.read().from(options.getMappingFileLocation()))
				.apply("Parse Mapping", ParDo.of(new DoFn<String, KV<String, String>>() {
					/**
					 * This DoFn is used to parse the Mapping file at GCS to a Map
					 */
					private static final long serialVersionUID = -7421713799525792648L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] mappingDataEntry = c.element().split(Pattern.quote("|"));
						if (mappingDataEntry.length != 2) {
							log.error("Initial mapping parse failed.");
							throw new MappingFileException("Mapping entry error");
						} else {
							mappingDataEntry = StringUtils.stripAll(mappingDataEntry);
							c.output(KV.of(mappingDataEntry[0], mappingDataEntry[1]));
						}
						log.info(mappingDataEntry.toString());
					}
				})).apply("Map View", View.asMap());

		PCollection<TableRow> sourceData = pipeline
				.apply("Read from Cloud SQL View",JdbcIO.<TableRow>read()
						.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
								.create(Constants.JDBC_DRIVER, gamingConfig.get(Constants.URL))
								.withUsername(gamingConfig.get(Constants.USERNAME))
								.withPassword(gamingConfig.get(Constants.PASSWORD)))
						.withQuery(NestedValueProvider.of(options.getStartDate(),new QueryTranslator(options.getEndDate())))
						.withCoder(TableRowJsonCoder.of()).withRowMapper(new JdbcIO.RowMapper<TableRow>() {
							private static final long serialVersionUID = 1L;

							public TableRow mapRow(ResultSet resultSet) throws Exception {
								TableRow row = new TableRow();

								row.put("mname", resultSet.getString(1));
								row.put("client_id", resultSet.getString(2));
								row.put("terminal_name", resultSet.getString(3));
								row.put("action_timestamp", resultSet.getString(4));
								row.put("transaction_id", resultSet.getString(5));
								row.put("customer_id", resultSet.getString(6));
								row.put("game_date", resultSet.getString(7));
								row.put("gaming_day_begins", resultSet.getString(8));
								row.put("transaction_primary", resultSet.getString(9));
								row.put("transaction_subordinate", resultSet.getString(10));
								row.put("acctnum", resultSet.getString(11));
								row.put("cname", resultSet.getString(12));
								row.put("type", resultSet.getString(13));
								row.put("sub_type", resultSet.getString(14));
								row.put("sub_account", resultSet.getString(15));
								row.put("total", resultSet.getString(16));
								row.put("account", resultSet.getString(17));
								row.put("amt", resultSet.getString(18));
								row.put("sub_amt", resultSet.getString(19));
								row.put("wageract", resultSet.getString(20));
								row.put("bills", resultSet.getString(21));
								row.put("coins", resultSet.getString(22));
								row.put("tickets", resultSet.getString(23));
								row.put("handpay", resultSet.getString(24));
								row.put("cguid", resultSet.getString(25));
								row.put("auth", resultSet.getString(26));
								row.put("fee", resultSet.getString(27));
								row.put("insert_date", resultSet.getString(28));
								
								//log.info(row.toString());

								return row;
							}
						}));


		PCollectionView<String> currentDateTimeView = pipeline.apply(Create.of("DateTime"))
				.apply("Getting Current Date Time", ParDo.of(new CurrentDateTimeDoFn()))
				.apply("Converting To View", View.asSingleton());

		PCollectionView<BigInteger> etlBatchIdView = pipeline.apply(Create.of("DateTime"))
				.apply("Getting ETLBatchID", ParDo.of(new EtlBatchIdDoFn()))
				.apply("Converting To View", View.asSingleton());

		PCollection<String> writeData = sourceData.apply("Column Transaform",new FinalizeDumpData(mappingData,etlBatchIdView,currentDateTimeView));
		
		writeData.apply("Write to Cloud SQL Consumption Table",
				JdbcIO.<String>write()
				.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
						.create(Constants.JDBC_DRIVER, gamingConfig.get(Constants.URL))
						.withUsername(gamingConfig.get(Constants.USERNAME))
						.withPassword(gamingConfig.get(Constants.PASSWORD)))
				.withStatement(Constants.Base_Query
						+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
				.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {
					private static final long serialVersionUID = 1L;

					// @Override
					public void setParameters(String element, java.sql.PreparedStatement preparedStatement)
							throws Exception {
						String[] rowElement = element.split(",");
						int index = 1;
						for (String rowArray : rowElement) {
							if (rowArray.equals("default")) {
								preparedStatement.setNull(index, Types.NULL);
							} else {
								preparedStatement.setString(index, rowArray);
							}
							index = index + 1;
						}
					}
				}).withBatchSize(50000));

		pipeline.run();

	}

}
