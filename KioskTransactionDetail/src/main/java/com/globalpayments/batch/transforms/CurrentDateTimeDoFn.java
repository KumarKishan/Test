package com.globalpayments.batch.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import com.globalpayments.batch.utility.Utility;

public class CurrentDateTimeDoFn extends DoFn<String, String>
{
	private static final long serialVersionUID = -893859901519653161L;

	@ProcessElement
	public void processElement(ProcessContext c)
	{
		c.output(Utility.getCurrentDateTime());
	}
}