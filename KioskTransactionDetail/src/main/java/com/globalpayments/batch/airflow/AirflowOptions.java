package com.globalpayments.batch.airflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface AirflowOptions extends DataflowPipelineOptions, DataflowWorkerHarnessOptions
{	
	
	public void setGamingConfigPath(ValueProvider<String> gamingConfigPath);
	public ValueProvider<String> getGamingConfigPath();
	
	public void setMappingFileLocation(ValueProvider<String> mappingFileLocation);
	public ValueProvider<String> getMappingFileLocation();
	
	public void setStartDate(ValueProvider<String> startDate);
	public ValueProvider<String> getStartDate();
	
	public void setEndDate(ValueProvider<String> endDate);
	public ValueProvider<String> getEndDate();

}
