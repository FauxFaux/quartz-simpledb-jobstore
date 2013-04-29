package com.threepillar.labs.quartz.simpledb;

public class QueryBuilder {
	
	private String jobDomain;
	private String triggerDomain;

	private static final String COUNT_QUERY = "select count(*) from `%s`";
	private static final String NAME_QUERY = "select (name) from `%s` where group = '%s'";
	private static final String GROUP_QUERY = "select (group) from `%s`";
	private static final String TRIGGERS_FOR_JOB_QUERY = "select * from `%s` where jobName = '%s' and jobGroup = '%s'";
	private static final String ACQUIRE_TRIGGER_QUERY = "select * from `%s` where nextFireTime < '%s' limit 1";
	
	
	public QueryBuilder(String jobDomain, String triggerDomain) {
		this.jobDomain = jobDomain;
		this.triggerDomain = triggerDomain;		
	}

	public String countJobs() {
		return String.format(COUNT_QUERY, jobDomain);
	}

	public String countTriggers() {
		return String.format(COUNT_QUERY, triggerDomain);
	}

	public String jobNamesInGroup(String groupName) {
		return String.format(NAME_QUERY, jobDomain, groupName);
	}

	public String triggerNamesInGroup(String groupName) {
		return String.format(NAME_QUERY, triggerDomain, groupName);
	}
	
	public String triggerGroups() {
		return String.format(GROUP_QUERY, triggerDomain);
	}

	public String jobGroups() {
		return String.format(GROUP_QUERY, jobDomain);
	}

	public String triggersForJob(String jobName, String jobGroup) {
		return String.format(TRIGGERS_FOR_JOB_QUERY, triggerDomain, jobName, jobGroup);
	}

	public String acquireTrigger(String noLaterThan) {
		return String.format(ACQUIRE_TRIGGER_QUERY, triggerDomain,  noLaterThan);
	}
}
