package com.globalpayments.batch.transforms;

import java.math.BigInteger;
import java.util.Map;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class FinalizeDumpData extends PTransform<PCollection<TableRow>, PCollection<String>> {

	/**
	 * Perform all other transforms required before dumping the data to BigQuery
	 */
	private static final long serialVersionUID = 5338302191967493252L;
	PCollectionView<Map<String, String>> mappingData;
	PCollectionView<BigInteger> etlBatchIdView;
	PCollectionView<String> currentDateTimeView;

	public FinalizeDumpData(PCollectionView<Map<String, String>> mappingData,
			PCollectionView<BigInteger> etlBatchIdView, PCollectionView<String> currentDateTimeView) {
		this.mappingData = mappingData;
		this.etlBatchIdView = etlBatchIdView;
		this.currentDateTimeView = currentDateTimeView;
	}

	@Override
	public PCollection<String> expand(PCollection<TableRow> sourceData) {
		return sourceData.apply(ParDo.of(new CombineDataDoFn(mappingData, etlBatchIdView, currentDateTimeView))
				.withSideInputs(mappingData).withSideInputs(etlBatchIdView).withSideInputs(currentDateTimeView));
	}

}
