package com.globalpayments.batch.transforms;

import java.math.BigInteger;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;

import com.globalpayments.batch.utility.Constants;
import com.google.api.services.bigquery.model.TableRow;

public class CombineDataDoFn extends DoFn<TableRow, String> {

	private static final long serialVersionUID = -210218195101809627L;
	private static final Logger LOG = LoggerFactory.getLogger(CombineDataDoFn.class);

	PCollectionView<Map<String, String>> mappingData;

	boolean doFnInitialized = false;
	String[] target;
	String[] source;
	PCollectionView<BigInteger> etlBatchIdView;
	PCollectionView<String> currentDateTimeView;

	public CombineDataDoFn(PCollectionView<Map<String, String>> mappingData, PCollectionView<BigInteger> etlBatchIdView,
			PCollectionView<String> currentDateTimeView) {
		this.mappingData = mappingData;
		this.doFnInitialized = false;
		this.etlBatchIdView = etlBatchIdView;
		this.currentDateTimeView = currentDateTimeView;

	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		BigInteger etlBatchID=c.sideInput(etlBatchIdView);
		String createdDate=c.sideInput(currentDateTimeView);

		Map<String, String> mappingInfo = c.sideInput(this.mappingData);

		TableRow sourceTableRow = c.element();

		StringBuilder mappedRow = new StringBuilder();

		String surrogate_Key = java.util.UUID.randomUUID().toString();
		mappedRow.append(surrogate_Key).append(",");

		if (!doFnInitialized) {
			this.target = StringUtils.stripAll(mappingInfo.get(Constants.TARGET).split(Pattern.quote(",")));

			this.source = StringUtils.stripAll(mappingInfo.get(Constants.SOURCE).split(Pattern.quote(",")));

			doFnInitialized = true;
		}

		for (int index = 0; index < this.source.length - 1; index++) {
			Object joinColumnData = sourceTableRow.get(this.source[index]);
			if (!(joinColumnData == null))
				mappedRow.append(sourceTableRow.get(this.source[index])).append(",");
			else
				mappedRow.append("default").append(",");
		}

		mappedRow.append(sourceTableRow.get(this.source[27])).append(",");
		mappedRow.append(createdDate).append(",");// dw_create_date_time
		mappedRow.append(createdDate).append(",");// dw_update_date_time
		mappedRow.append(etlBatchID);

		c.output(mappedRow.toString());
		LOG.info(mappedRow.toString());
	}

}
