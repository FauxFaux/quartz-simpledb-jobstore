package com.threepillar.labs.quartz.simpledb;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public abstract class FixJobDetailMixIn
{
	@JsonIgnore 
	public abstract String[] getJobListenerNames();
	
	@JsonIgnore 
	public abstract String getKey();

	@JsonIgnore 
	public abstract boolean getFullName();

	@JsonIgnore 
	public abstract boolean isStateful();
	
	@JsonProperty("volatile") 
	public abstract void setVolatility(boolean b);

	@JsonProperty("durable") 
	public abstract void setDurability(boolean b);
	
}