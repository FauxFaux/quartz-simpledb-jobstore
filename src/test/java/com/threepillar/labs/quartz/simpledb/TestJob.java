package com.threepillar.labs.quartz.simpledb;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class TestJob implements Job {

	  private int timeout;
	  
	  /**
	   * Setter called after the ExampleJob is instantiated
	   * with the value from the JobDetailBean (5)
	   */ 
	  public void setTimeout(int timeout) {
	    this.timeout = timeout;
	  }
	  
	  public void execute(JobExecutionContext ctx) throws JobExecutionException {
	      System.out.println("Job running with timeout: " + timeout);
	  }
}