package com.globalpayments.batch.transforms;

import java.math.BigInteger;

import org.apache.beam.sdk.transforms.DoFn;

import com.globalpayments.batch.utility.Utility;


public class EtlBatchIdDoFn extends DoFn<String, BigInteger> {
	private static final long serialVersionUID = -893859901519653161L;
	
	@ProcessElement
	public void processElement(ProcessContext c)
	{
		c.output(Utility.getETLBatchID());
	}
}