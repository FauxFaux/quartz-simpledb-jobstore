package com.threepillar.labs.quartz.simpledb;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.quartz.CronExpression;

public abstract class FixCronTriggerMixIn
{
	@JsonIgnore 
	public abstract void setCronExpression(CronExpression cron);

	@JsonProperty("cronExpression") 
	public abstract void setCronExpression(String cronExpression);
	
	@JsonProperty("cronExpression") 
	public abstract String getCronExpression();
}