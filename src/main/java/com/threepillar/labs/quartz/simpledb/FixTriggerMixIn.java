package com.threepillar.labs.quartz.simpledb;

import java.util.Date;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public abstract class FixTriggerMixIn {

	@JsonIgnore
	public abstract Date getFinalFireTime();

	@JsonIgnore
	public abstract String getExpressionSummary();

	@JsonIgnore
	public abstract String getKey();

	@JsonIgnore
	public abstract String getJobKey();

	@JsonProperty("volatile")
	public abstract void setVolatility(boolean b);

	@JsonIgnore
	public abstract boolean getFullName();

	@JsonIgnore
	public abstract boolean getFullJobName();

	@JsonIgnore
	public abstract String[] getTriggerListenerNames();
}
