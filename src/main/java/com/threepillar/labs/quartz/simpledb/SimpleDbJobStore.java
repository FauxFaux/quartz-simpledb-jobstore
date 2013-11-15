package com.threepillar.labs.quartz.simpledb;

/* 
 * Copyright 2001-2009 3Pillar Global Inc. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at 
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0 
 *   
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations 
 * under the License.
 * 
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.util.ISO8601DateFormat;
import org.quartz.Calendar;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.core.SchedulingContext;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

/**
 * <p>
 * This class implements a <code>{@link org.quartz.spi.JobStore}</code> that
 * utilizes AWS SimpleDB as its storage device.
 * </p>
 * 
 * <p>
 * Important: Currently it does not handle durable jobs and misfiring
 * </p>
 * 
 * @author Abhinav Maheshwari
 */
public class SimpleDbJobStore implements JobStore {

	/*
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 * 
	 * Attribute names in Job Domain.
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 */
	private static final String JOB_CLASS = "class";
	private static final String JOB_JOBCLASS = "jobClass";
	private static final String JOB_GROUP = "group";
	private static final String JOB_NAME = "name";
	private static final String JOB_DATA_MAP = "jobDataMap";

	/*
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 * 
	 * Attribute names in Trigger domain
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 */
	private static final String TRIGGER_CLASS = "class";
	private static final String TRIGGER_GROUP = "group";
	private static final String TRIGGER_NAME = "name";
	private static final String TRIGGER_PRIORITY = "priority";
	private static final String TRIGGER_START_TIME = "startTime";
	private static final String TRIGGER_NEXT_FIRE_TIME = "nextFireTime";
	private static final String TRIGGER_END_TIME = "endTime";
	private static final String TRIGGER_JOB_NAME = "jobName";
	private static final String TRIGGER_JOB_GROUP = "jobGroup";
	private static final String TRIGGER_CALENDAR_NAME = "calendarName";
	private static final String TRIGGER_STATE = "state";
	private static final String TRIGGER_JSON = "json";
	private static final String TRIGGER_JSON_LENGTH = "json_length";

	/*
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 * 
	 * Domain names for SimpleDB.
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 */
	protected static final String JOB_DOMAIN = "quartzJobs";
	protected static final String TRIGGER_DOMAIN = "quartzTriggers";
	private static final int MAX_ATTR_LENGTH = 1024;

	/*
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 * 
	 * Configuration parameters.
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 */

	private String awsAccessKey;
	private String awsSecretKey;
	private String prefix;
	private boolean recreate;

	/*
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 * 
	 * Data members.
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 */
	private AmazonSimpleDBClient amazonSimpleDb;
	private String jobDomain;
	private String triggerDomain;
	private final ObjectMapper mapper;
	private QueryBuilder query;
	private final ISO8601DateFormat dateFormat = new ISO8601DateFormat();

	protected final Object lock = new Object();
	protected long misfireThreshold = 5000l;
	protected SchedulerSignaler signaler;
	private final Log log = LogFactory.getLog(getClass());

	/*
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 * 
	 * Constructors.
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 */

	/**
	 * <p>
	 * Create a new <code>RAMJobStore</code>.
	 * </p>
	 */
	public SimpleDbJobStore() {
		this.mapper = new ObjectMapper();
		this.mapper.getDeserializationConfig().addMixInAnnotations(
				Trigger.class, FixCronTriggerMixIn.class);
		this.mapper.getDeserializationConfig().addMixInAnnotations(
				JobDetail.class, FixJobDetailMixIn.class);
		this.mapper.getDeserializationConfig().addMixInAnnotations(
				CronTrigger.class, FixTriggerMixIn.class);
		this.mapper.getDeserializationConfig().addMixInAnnotations(
				SimpleTrigger.class, FixTriggerMixIn.class);
	}

	/*
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 * 
	 * Interface.
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 */

	protected Log getLog() {
		return log;
	}

	public void setAwsAccessKey(String accessKey) {
		this.awsAccessKey = accessKey;
	}

	public void setAwsSecretKey(String secretKey) {
		this.awsSecretKey = secretKey;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public void setRecreate(boolean recreate) {
		this.recreate = recreate;
	}

	/**
	 * <p>
	 * Called by the QuartzScheduler before the <code>JobStore</code> is used,
	 * in order to give the it a chance to initialize.
	 * </p>
	 */
	@Override
	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) {

		logDebug("Initializing SimpleDbJobStore");
		this.jobDomain = String.format("%s.%s", prefix,
				SimpleDbJobStore.JOB_DOMAIN);
		this.triggerDomain = String.format("%s.%s", prefix,
				SimpleDbJobStore.TRIGGER_DOMAIN);
		this.query = new QueryBuilder(this.jobDomain, this.triggerDomain);
		this.amazonSimpleDb = new AmazonSimpleDBClient(new BasicAWSCredentials(
				awsAccessKey, awsSecretKey));
		this.signaler = signaler;

		boolean foundJobs = false, foundTriggers = false;
		List<String> domainNames = getSimpleDbDomainNames();
		for (String name : domainNames) {
			if (name.equals(jobDomain)) {
				if (recreate) {
					amazonSimpleDb.deleteDomain(new DeleteDomainRequest(
							jobDomain));
				} else {
					foundJobs = true;
				}
			}
			if (name.equals(triggerDomain)) {
				if (recreate) {
					amazonSimpleDb.deleteDomain(new DeleteDomainRequest(
							triggerDomain));
				} else {
					foundTriggers = true;
				}
			}
			if (foundJobs && foundTriggers) {
				break;
			}
		}
		if (!foundJobs) {
			amazonSimpleDb.createDomain(new CreateDomainRequest(jobDomain));
		}
		if (!foundTriggers) {
			amazonSimpleDb.createDomain(new CreateDomainRequest(triggerDomain));
		}
		log.info("SimpleDbJobStore initialized.");
	}

	private List<String> getSimpleDbDomainNames() {

		ListDomainsResult result = amazonSimpleDb.listDomains();
		List<String> names = result.getDomainNames();
		String nextToken = result.getNextToken();
		while (nextToken != null && !nextToken.isEmpty()) {
			result = amazonSimpleDb.listDomains(new ListDomainsRequest()
					.withNextToken(nextToken));
			names.addAll(result.getDomainNames());
			nextToken = result.getNextToken();
		}
		return names;
	}

	@Override
	public void schedulerStarted() throws SchedulerException {
		// nothing to do
	}

	public long getMisfireThreshold() {
		return misfireThreshold;
	}

	/**
	 * The number of milliseconds by which a trigger must have missed its
	 * next-fire-time, in order for it to be considered "misfired" and thus have
	 * its misfire instruction applied.
	 * 
	 * @param misfireThreshold
	 */
	public void setMisfireThreshold(long misfireThreshold) {
		if (misfireThreshold < 1) {
			throw new IllegalArgumentException(
					"'misfireThreshold' must be >= 1");
		}
		this.misfireThreshold = misfireThreshold;
	}

	/**
	 * <p>
	 * Called by the QuartzScheduler to inform the <code>JobStore</code> that it
	 * should free up all of it's resources because the scheduler is shutting
	 * down.
	 * </p>
	 */
	@Override
	public void shutdown() {
		this.amazonSimpleDb.shutdown();
	}

	@Override
	public boolean supportsPersistence() {
		return true;
	}

	/**
	 * <p>
	 * Store the given <code>{@link org.quartz.JobDetail}</code> and
	 * <code>{@link org.quartz.Trigger}</code>.
	 * </p>
	 * 
	 * @param newJob
	 *            The <code>JobDetail</code> to be stored.
	 * @param newTrigger
	 *            The <code>Trigger</code> to be stored.
	 * @throws ObjectAlreadyExistsException
	 *             if a <code>Job</code> with the same name/group already
	 *             exists.
	 */
	@Override
	public void storeJobAndTrigger(SchedulingContext ctxt, JobDetail newJob,
			Trigger newTrigger) throws JobPersistenceException {
		storeJob(ctxt, newJob, false);
		storeTrigger(ctxt, newTrigger, false);
	}

	/**
	 * <p>
	 * Store the given <code>{@link org.quartz.Job}</code>.
	 * </p>
	 * 
	 * @param newJob
	 *            The <code>Job</code> to be stored.
	 * @param replaceExisting
	 *            If <code>true</code>, any <code>Job</code> existing in the
	 *            <code>JobStore</code> with the same name & group should be
	 *            over-written.
	 * @throws ObjectAlreadyExistsException
	 *             if a <code>Job</code> with the same name/group already
	 *             exists, and replaceExisting is set to false.
	 */
	@Override
	public void storeJob(SchedulingContext ctxt, JobDetail newJob,
			boolean replaceExisting) throws ObjectAlreadyExistsException {

		logDebug("Storing Job: ", newJob.getFullName());
		ReplaceableItem item = null;
		try {
			List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
			attributes.add(new ReplaceableAttribute(JOB_NAME, newJob.getName(),
					true));
			attributes.add(new ReplaceableAttribute(JOB_GROUP, newJob
					.getGroup(), true));
			attributes.add(new ReplaceableAttribute(JOB_JOBCLASS, newJob
					.getJobClass().getName(), true));
			attributes.add(new ReplaceableAttribute(JOB_CLASS, newJob
					.getClass().getName(), true));
			if (newJob.getJobDataMap() != null) {
				attributes.add(new ReplaceableAttribute(JOB_DATA_MAP, mapper
						.writeValueAsString(newJob.getJobDataMap()), true));
			}
			item = new ReplaceableItem(JobWrapper.getJobNameKey(newJob),
					attributes);
			amazonSimpleDb.batchPutAttributes(new BatchPutAttributesRequest(
					jobDomain, Collections.singletonList(item)));
		} catch (Exception e) {
			log.error("Could not store Job: " + newJob.getFullName(), e);
		}

	}

	/**
	 * <p>
	 * Remove (delete) the <code>{@link org.quartz.Job}</code> with the given
	 * name, and any <code>{@link org.quartz.Trigger}</code> s that reference
	 * it.
	 * </p>
	 * 
	 * @param jobName
	 *            The name of the <code>Job</code> to be removed.
	 * @param groupName
	 *            The group name of the <code>Job</code> to be removed.
	 * @return <code>true</code> if a <code>Job</code> with the given name &
	 *         group was found and removed from the store.
	 */
	@Override
	public boolean removeJob(SchedulingContext ctxt, String jobName,
			String groupName) {
		logDebug("Removing Job: ", groupName, ".", jobName);
		try {
			String key = JobWrapper.getJobNameKey(jobName, groupName);
			amazonSimpleDb.deleteAttributes(new DeleteAttributesRequest(
					jobDomain, key));
			return true;
		} catch (Exception e) {
			log.error("Could not remove Job: " + groupName + "." + jobName, e);
			return false;
		}
	}

	/**
	 * <p>
	 * Store the given <code>{@link org.quartz.Trigger}</code>.
	 * </p>
	 * 
	 * @param newTrigger
	 *            The <code>Trigger</code> to be stored.
	 * @param replaceExisting
	 *            If <code>true</code>, any <code>Trigger</code> existing in the
	 *            <code>JobStore</code> with the same name & group should be
	 *            over-written.
	 * @throws ObjectAlreadyExistsException
	 *             if a <code>Trigger</code> with the same name/group already
	 *             exists, and replaceExisting is set to false.
	 * 
	 * @see #pauseTriggerGroup(SchedulingContext, String)
	 */
	@Override
	public void storeTrigger(SchedulingContext ctxt, Trigger newTrigger,
			boolean replaceExisting) throws JobPersistenceException {

		logDebug("Storing Trigger: ", newTrigger.getFullName());
		ReplaceableItem item = null;
		try {

			List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
			attributes.add(new ReplaceableAttribute(TRIGGER_CLASS, newTrigger
					.getClass().getName(), true));
			attributes.add(new ReplaceableAttribute(TRIGGER_NAME, newTrigger
					.getName(), true));
			if (newTrigger.getCalendarName() != null) {
				attributes.add(new ReplaceableAttribute(TRIGGER_CALENDAR_NAME,
						newTrigger.getCalendarName(), true));
			}
			attributes.add(new ReplaceableAttribute(TRIGGER_GROUP, newTrigger
					.getGroup(), true));
			attributes.add(new ReplaceableAttribute(TRIGGER_JOB_GROUP,
					newTrigger.getJobGroup(), true));
			attributes.add(new ReplaceableAttribute(TRIGGER_JOB_NAME,
					newTrigger.getJobName(), true));
			attributes.add(new ReplaceableAttribute(TRIGGER_PRIORITY, String
					.valueOf(newTrigger.getPriority()), true));
			if (newTrigger.getEndTime() != null) {
				attributes.add(new ReplaceableAttribute(TRIGGER_END_TIME,
						dateFormat.format(newTrigger.getEndTime()), true));
			}
			if (newTrigger.getStartTime() != null) {
				attributes.add(new ReplaceableAttribute(TRIGGER_START_TIME,
						dateFormat.format(newTrigger.getStartTime()), true));
			}
			if (newTrigger.getNextFireTime() != null) {
				attributes.add(new ReplaceableAttribute(TRIGGER_NEXT_FIRE_TIME,
						dateFormat.format(newTrigger.getNextFireTime()), true));
			}
			item = new ReplaceableItem(
					TriggerWrapper.getTriggerNameKey(newTrigger), attributes);
			String json = mapper.writeValueAsString(newTrigger);
			attributes.add(new ReplaceableAttribute(TRIGGER_JSON_LENGTH, String
					.valueOf(json.length()), true));

			// Store the JSON representation in multiple attributes since max
			// length is 1024
			for (int i = 0; json.length() > i * MAX_ATTR_LENGTH; i++) {
				int end = Math.min((i + 1) * MAX_ATTR_LENGTH, json.length());
				attributes.add(new ReplaceableAttribute(TRIGGER_JSON
						+ String.valueOf(i), json.substring(
						i * MAX_ATTR_LENGTH, end), true));
			}
			amazonSimpleDb.batchPutAttributes(new BatchPutAttributesRequest(
					triggerDomain, Collections.singletonList(item)));
		} catch (Exception e) {
			log.error("Could not store Trigger: " + newTrigger.getFullName(), e);
		}

	}

	/**
	 * <p>
	 * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the
	 * given name.
	 * </p>
	 * 
	 * @param triggerName
	 *            The name of the <code>Trigger</code> to be removed.
	 * @param groupName
	 *            The group name of the <code>Trigger</code> to be removed.
	 * @return <code>true</code> if a <code>Trigger</code> with the given name &
	 *         group was found and removed from the store.
	 */
	@Override
	public boolean removeTrigger(SchedulingContext ctxt, String triggerName,
			String groupName) {
		return removeTrigger(ctxt, triggerName, groupName, true);
	}

	private boolean removeTrigger(SchedulingContext ctxt, String triggerName,
			String groupName, boolean removeOrphanedJob) {
		logDebug("Removing Trigger: ", groupName, ".", triggerName);
		try {
			String key = TriggerWrapper.getTriggerNameKey(triggerName,
					groupName);
			amazonSimpleDb.deleteAttributes(new DeleteAttributesRequest(
					triggerDomain, key));
			return true;
		} catch (Exception e) {
			log.error("Could not remove Trigger: " + groupName + "."
					+ triggerName, e);
			return false;
		}
	}

	/**
	 * @see org.quartz.spi.JobStore#replaceTrigger(org.quartz.core.SchedulingContext,
	 *      java.lang.String, java.lang.String, org.quartz.Trigger)
	 */
	@Override
	public boolean replaceTrigger(SchedulingContext ctxt, String triggerName,
			String groupName, Trigger newTrigger)
			throws JobPersistenceException {
		logDebug("Replacing Trigger: ", triggerName, ".", groupName, " with ",
				newTrigger.getFullName());
		Trigger found = retrieveTrigger(ctxt, triggerName, groupName);

		if (found != null) {
			if (!found.getJobName().equals(newTrigger.getJobName())
					|| !found.getJobGroup().equals(newTrigger.getJobGroup())) {
				throw new JobPersistenceException(
						"New trigger is not related to the same job as the old trigger.");
			}
			try {
				removeTrigger(ctxt, triggerName, groupName);
				storeTrigger(ctxt, newTrigger, false);
			} catch (JobPersistenceException jpe) {
				storeTrigger(ctxt, found, false); // put previous trigger
													// back...
				throw jpe;
			}
		}

		return (found != null);
	}

	/**
	 * <p>
	 * Retrieve the <code>{@link org.quartz.JobDetail}</code> for the given
	 * <code>{@link org.quartz.Job}</code>.
	 * </p>
	 * 
	 * @param jobName
	 *            The name of the <code>Job</code> to be retrieved.
	 * @param groupName
	 *            The group name of the <code>Job</code> to be retrieved.
	 * @return The desired <code>Job</code>, or null if there is no match.
	 */
	@Override
	public JobDetail retrieveJob(SchedulingContext ctxt, String jobName,
			String groupName) {
		logDebug("Retrieving Job: ", groupName, ".", jobName);
		String key = JobWrapper.getJobNameKey(jobName, groupName);
		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(jobDomain, key)
						.withConsistentRead(new Boolean(true)));
		JobDetail job = null;
		try {
			job = jobDetailFromAttributes(result.getAttributes());
		} catch (Exception e) {
			log.error("Could not retrieve Job: " + groupName + "." + jobName, e);
		}
		return job;
	}

	private JobDetail jobDetailFromAttributes(List<Attribute> attributes)
			throws Exception {
		if (attributes == null || attributes.size() == 0) {
			throw new Exception("No attributes");
		}
		Map<String, String> map = new HashMap<String, String>();
		for (Attribute attr : attributes) {
			map.put(attr.getName(), attr.getValue());
		}
		Class<? extends JobDetail> clz = Class.forName(map.get(JOB_CLASS))
				.asSubclass(JobDetail.class);
		JobDetail jobDetail = clz.newInstance();
		jobDetail.setJobClass(Class.forName(map.get(JOB_JOBCLASS)));
		jobDetail.setName(map.get(JOB_NAME));
		jobDetail.setGroup(map.get(JOB_GROUP));
		jobDetail.setJobDataMap(mapper.readValue(map.get(JOB_DATA_MAP),
				JobDataMap.class));
		return jobDetail;
	}

	/**
	 * <p>
	 * Retrieve the given <code>{@link org.quartz.Trigger}</code>.
	 * </p>
	 * 
	 * @param triggerName
	 *            The name of the <code>Trigger</code> to be retrieved.
	 * @param groupName
	 *            The group name of the <code>Trigger</code> to be retrieved.
	 * @return The desired <code>Trigger</code>, or null if there is no match.
	 */
	@Override
	public Trigger retrieveTrigger(SchedulingContext ctxt, String triggerName,
			String groupName) {
		logDebug("Retrieving Trigger: ", triggerName, ".", groupName);
		String key = TriggerWrapper.getTriggerNameKey(triggerName, groupName);
		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(triggerDomain, key)
						.withConsistentRead(new Boolean(true)));
		TriggerWrapper tw = null;
		try {
			tw = triggerFromAttributes(result.getAttributes());
			return tw.trigger;
		} catch (Exception e) {
			logDebug("Trigger not found: ", triggerName, ".", groupName);
		}
		return null;
	}

	private TriggerWrapper triggerFromAttributes(List<Attribute> attributes)
			throws Exception {
		if (attributes == null || attributes.size() == 0) {
			throw new Exception("No attributes");
		}

		Map<String, String> map = new HashMap<String, String>();
		for (Attribute attr : attributes) {
			map.put(attr.getName(), attr.getValue());
		}

		Class<? extends Trigger> clz = Class.forName(map.get(TRIGGER_CLASS))
				.asSubclass(Trigger.class);

		int len = Integer.parseInt(map.get(TRIGGER_JSON_LENGTH));
		int n = (len - 1) / MAX_ATTR_LENGTH + 1;
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < n; i++) {
			buf.append(map.get(TRIGGER_JSON + String.valueOf(i)));
		}

		Trigger trigger = mapper.readValue(buf.toString(), clz);
		trigger.setName(map.get(TRIGGER_NAME));
		trigger.setGroup(map.get(TRIGGER_GROUP));
		if (map.get(TRIGGER_CALENDAR_NAME) != null) {
			trigger.setCalendarName(map.get(TRIGGER_CALENDAR_NAME));
		}
		trigger.setJobName(map.get(TRIGGER_JOB_NAME));
		trigger.setJobGroup(map.get(TRIGGER_JOB_GROUP));
		trigger.setStartTime(dateFormat.parse(map.get(TRIGGER_START_TIME)));
		if (map.get(TRIGGER_END_TIME) != null) {
			trigger.setEndTime(dateFormat.parse(map.get(TRIGGER_END_TIME)));
		}
		trigger.setPriority(Integer.parseInt(map.get(TRIGGER_PRIORITY)));

		TriggerWrapper wrapper = new TriggerWrapper(trigger);
		if (map.get(TRIGGER_STATE) != null) {
			wrapper.state = Integer.parseInt(map.get(TRIGGER_STATE));
		}
		return wrapper;
	}

	private void updateState(TriggerWrapper tw) {
		logDebug("Updating state of Trigger: ", tw.trigger.getFullName());
		String key = TriggerWrapper.getTriggerNameKey(tw.trigger);
		ReplaceableAttribute attr = new ReplaceableAttribute(TRIGGER_STATE,
				String.valueOf(tw.state), true);
		amazonSimpleDb.putAttributes(new PutAttributesRequest(triggerDomain,
				key, Collections.singletonList(attr)));
	}

	/**
	 * <p>
	 * Get the current state of the identified <code>{@link Trigger}</code>.
	 * </p>
	 * 
	 * @see Trigger#STATE_NORMAL
	 * @see Trigger#STATE_PAUSED
	 * @see Trigger#STATE_COMPLETE
	 * @see Trigger#STATE_ERROR
	 * @see Trigger#STATE_BLOCKED
	 * @see Trigger#STATE_NONE
	 */
	@Override
	public int getTriggerState(SchedulingContext ctxt, String triggerName,
			String groupName) throws JobPersistenceException {
		logDebug("Finding state of Trigger: ", triggerName, ".", groupName);
		String key = TriggerWrapper.getTriggerNameKey(triggerName, groupName);

		TriggerWrapper tw = null;
		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(triggerDomain, key)
						.withConsistentRead(new Boolean(true)));
		try {
			tw = triggerFromAttributes(result.getAttributes());
		} catch (Exception e) {
			log.error("Error finding state of Trigger: " + triggerName + "."
					+ groupName, e);
		}

		if (tw == null) {
			return Trigger.STATE_NONE;
		}

		switch (tw.state) {
		case TriggerWrapper.STATE_COMPLETE:
			return Trigger.STATE_COMPLETE;
		case TriggerWrapper.STATE_PAUSED:
			return Trigger.STATE_PAUSED;
		case TriggerWrapper.STATE_PAUSED_BLOCKED:
			return Trigger.STATE_PAUSED;
		case TriggerWrapper.STATE_BLOCKED:
			return Trigger.STATE_BLOCKED;
		case TriggerWrapper.STATE_ERROR:
			return Trigger.STATE_ERROR;
		default:
			return Trigger.STATE_NORMAL;
		}

	}

	/**
	 * <p>
	 * Store the given <code>{@link org.quartz.Calendar}</code>.
	 * </p>
	 * 
	 * @param calendar
	 *            The <code>Calendar</code> to be stored.
	 * @param replaceExisting
	 *            If <code>true</code>, any <code>Calendar</code> existing in
	 *            the <code>JobStore</code> with the same name & group should be
	 *            over-written.
	 * @param updateTriggers
	 *            If <code>true</code>, any <code>Trigger</code>s existing in
	 *            the <code>JobStore</code> that reference an existing Calendar
	 *            with the same name with have their next fire time re-computed
	 *            with the new <code>Calendar</code>.
	 * @throws ObjectAlreadyExistsException
	 *             if a <code>Calendar</code> with the same name already exists,
	 *             and replaceExisting is set to false.
	 */
	@Override
	public void storeCalendar(SchedulingContext ctxt, String name,
			Calendar calendar, boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException {

	}

	/**
	 * <p>
	 * Remove (delete) the <code>{@link org.quartz.Calendar}</code> with the
	 * given name.
	 * </p>
	 * 
	 * <p>
	 * If removal of the <code>Calendar</code> would result in
	 * <code>Trigger</code>s pointing to non-existent calendars, then a
	 * <code>JobPersistenceException</code> will be thrown.
	 * </p>
	 * *
	 * 
	 * @param calName
	 *            The name of the <code>Calendar</code> to be removed.
	 * @return <code>true</code> if a <code>Calendar</code> with the given name
	 *         was found and removed from the store.
	 */
	@Override
	public boolean removeCalendar(SchedulingContext ctxt, String calName)
			throws JobPersistenceException {

		return true;
	}

	/**
	 * <p>
	 * Retrieve the given <code>{@link org.quartz.Trigger}</code>.
	 * </p>
	 * 
	 * @param calName
	 *            The name of the <code>Calendar</code> to be retrieved.
	 * @return The desired <code>Calendar</code>, or null if there is no match.
	 */
	@Override
	public Calendar retrieveCalendar(SchedulingContext ctxt, String calName) {
		return null;
	}

	/**
	 * <p>
	 * Get the number of <code>{@link org.quartz.JobDetail}</code> s that are
	 * stored in the <code>JobsStore</code>.
	 * </p>
	 */
	@Override
	public int getNumberOfJobs(SchedulingContext ctxt) {
		logDebug("Finding number of jobs");
		try {
			SelectResult result = amazonSimpleDb.select(new SelectRequest(query
					.countJobs()));
			Item item = result.getItems().get(0);
			return Integer.parseInt(item.getAttributes().get(0).getValue());
		} catch (Exception e) {
			log.error("Could not find number of jobs", e);
			return -1;
		}
	}

	/**
	 * <p>
	 * Get the number of <code>{@link org.quartz.Trigger}</code> s that are
	 * stored in the <code>JobsStore</code>.
	 * </p>
	 */
	@Override
	public int getNumberOfTriggers(SchedulingContext ctxt) {
		logDebug("Finding number of triggers");
		try {
			SelectResult result = amazonSimpleDb.select(new SelectRequest(query
					.countTriggers()));
			Item item = result.getItems().get(0);
			return Integer.parseInt(item.getAttributes().get(0).getValue());
		} catch (Exception e) {
			log.error("Could not find number of triggers", e);
			return -1;
		}
	}

	/**
	 * <p>
	 * Get the number of <code>{@link org.quartz.Calendar}</code> s that are
	 * stored in the <code>JobsStore</code>.
	 * </p>
	 */
	@Override
	public int getNumberOfCalendars(SchedulingContext ctxt) {
		return 0;
	}

	/**
	 * <p>
	 * Get the names of all of the <code>{@link org.quartz.Job}</code> s that
	 * have the given group name.
	 * </p>
	 */
	@Override
	public String[] getJobNames(SchedulingContext ctxt, String groupName) {
		logDebug("Getting names of jobs");
		SelectResult result = amazonSimpleDb.select(new SelectRequest(query
				.jobNamesInGroup(groupName)));
		List<Item> jobs = result.getItems();
		String[] outList = new String[jobs.size()];
		int i = 0;
		for (Item item : jobs) {
			outList[i++] = item.getAttributes().get(0).getValue();
		}
		return outList;
	}

	/**
	 * <p>
	 * Get the names of all of the <code>{@link org.quartz.Calendar}</code> s in
	 * the <code>JobStore</code>.
	 * </p>
	 * 
	 * <p>
	 * If there are no Calendars in the given group name, the result should be a
	 * zero-length array (not <code>null</code>).
	 * </p>
	 */
	@Override
	public String[] getCalendarNames(SchedulingContext ctxt) {
		return new String[0];
	}

	/**
	 * <p>
	 * Get the names of all of the <code>{@link org.quartz.Trigger}</code> s
	 * that have the given group name.
	 * </p>
	 */
	@Override
	public String[] getTriggerNames(SchedulingContext ctxt, String groupName) {
		logDebug("Getting names of triggers");
		SelectResult result = amazonSimpleDb.select(new SelectRequest(query
				.triggerNamesInGroup(groupName)));
		List<Item> jobs = result.getItems();

		String[] outList = new String[jobs.size()];
		int i = 0;
		for (Item item : jobs) {
			outList[i++] = item.getAttributes().get(0).getValue();
		}
		return outList;
	}

	/**
	 * <p>
	 * Get the names of all of the <code>{@link org.quartz.Job}</code> groups.
	 * </p>
	 */
	@Override
	public String[] getJobGroupNames(SchedulingContext ctxt) {
		logDebug("Getting job group names");
		SelectResult result = amazonSimpleDb.select(new SelectRequest(query
				.jobGroups()));
		List<Item> jobs = result.getItems();
		Set<String> groups = new HashSet<String>();
		for (Item item : jobs) {
			groups.add(item.getAttributes().get(0).getValue());
		}
		String[] outList = new String[groups.size()];
		return groups.toArray(outList);

	}

	/**
	 * <p>
	 * Get the names of all of the <code>{@link org.quartz.Trigger}</code>
	 * groups.
	 * </p>
	 */
	@Override
	public String[] getTriggerGroupNames(SchedulingContext ctxt) {
		logDebug("Getting trigger group names");
		SelectResult result = amazonSimpleDb.select(new SelectRequest(query
				.triggerGroups()));
		List<Item> jobs = result.getItems();
		Set<String> groups = new HashSet<String>();
		for (Item item : jobs) {
			groups.add(item.getAttributes().get(0).getValue());
		}
		String[] outList = new String[groups.size()];
		return groups.toArray(outList);
	}

	/**
	 * <p>
	 * Get all of the Triggers that are associated to the given Job.
	 * </p>
	 * 
	 * <p>
	 * If there are no matches, a zero-length array should be returned.
	 * </p>
	 */
	@Override
	public Trigger[] getTriggersForJob(SchedulingContext ctxt, String jobName,
			String groupName) {
		logDebug("Get triggers for Job: " + jobName + "." + groupName);
		SelectResult result = amazonSimpleDb.select(new SelectRequest(query
				.triggersForJob(jobName, groupName)));
		List<Item> items = result.getItems();
		Trigger[] triggers = new Trigger[items.size()];
		int i = 0;
		for (Item item : items) {
			try {
				TriggerWrapper tw = triggerFromAttributes(item.getAttributes());
				triggers[i++] = tw.trigger;
			} catch (Exception e) {
				log.error("Could not create trigger for Item: "
						+ item.getName());
			}
		}
		return triggers;
	}

	/**
	 * <p>
	 * Pause the <code>{@link Trigger}</code> with the given name.
	 * </p>
	 * 
	 */
	@Override
	public void pauseTrigger(SchedulingContext ctxt, String triggerName,
			String groupName) {
		logDebug("Pausing Trigger: ", triggerName, ".", groupName);
		String key = TriggerWrapper.getTriggerNameKey(triggerName, groupName);
		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(triggerDomain, key)
						.withConsistentRead(new Boolean(true)));
		TriggerWrapper tw = null;

		try {
			tw = triggerFromAttributes(result.getAttributes());
		} catch (Exception e) {
			log.error("Could not create trigger for Item: " + result.toString());
		}

		// does the trigger exist?
		if (tw == null || tw.trigger == null) {
			return;
		}

		// if the trigger is "complete" pausing it does not make sense...
		if (tw.state == TriggerWrapper.STATE_COMPLETE) {
			return;
		}

		if (tw.state == TriggerWrapper.STATE_BLOCKED) {
			tw.state = TriggerWrapper.STATE_PAUSED_BLOCKED;
		} else {
			tw.state = TriggerWrapper.STATE_PAUSED;
		}

		updateState(tw);

	}

	/**
	 * <p>
	 * Pause all of the <code>{@link Trigger}s</code> in the given group.
	 * </p>
	 * 
	 * <p>
	 * The JobStore should "remember" that the group is paused, and impose the
	 * pause on any new triggers that are added to the group while the group is
	 * paused.
	 * </p>
	 * 
	 */
	@Override
	public void pauseTriggerGroup(SchedulingContext ctxt, String groupName) {
		logDebug("Pausing all triggers of goup: ", groupName);
		String[] names = getTriggerNames(ctxt, groupName);

		for (int i = 0; i < names.length; i++) {
			pauseTrigger(ctxt, names[i], groupName);
		}
	}

	/**
	 * <p>
	 * Pause the <code>{@link org.quartz.JobDetail}</code> with the given name -
	 * by pausing all of its current <code>Trigger</code>s.
	 * </p>
	 * 
	 */
	@Override
	public void pauseJob(SchedulingContext ctxt, String jobName,
			String groupName) {
		logDebug("Pausing all triggers of Job: ", jobName, ".", groupName);
		Trigger[] triggers = getTriggersForJob(ctxt, jobName, groupName);
		for (int j = 0; j < triggers.length; j++) {
			pauseTrigger(ctxt, triggers[j].getName(), triggers[j].getGroup());
		}
	}

	/**
	 * <p>
	 * Pause all of the <code>{@link org.quartz.JobDetail}s</code> in the given
	 * group - by pausing all of their <code>Trigger</code>s.
	 * </p>
	 * 
	 * 
	 * <p>
	 * The JobStore should "remember" that the group is paused, and impose the
	 * pause on any new jobs that are added to the group while the group is
	 * paused.
	 * </p>
	 */
	@Override
	public void pauseJobGroup(SchedulingContext ctxt, String groupName) {
		logDebug("Pausing all jobs of group: ", groupName);
		String[] jobNames = getJobNames(ctxt, groupName);
		for (int i = 0; i < jobNames.length; i++) {
			Trigger[] triggers = getTriggersForJob(ctxt, jobNames[i], groupName);
			for (int j = 0; j < triggers.length; j++) {
				pauseTrigger(ctxt, triggers[j].getName(),
						triggers[j].getGroup());
			}
		}
	}

	/**
	 * <p>
	 * Resume (un-pause) the <code>{@link Trigger}</code> with the given name.
	 * </p>
	 * 
	 * <p>
	 * If the <code>Trigger</code> missed one or more fire-times, then the
	 * <code>Trigger</code>'s misfire instruction will be applied.
	 * </p>
	 * 
	 */
	@Override
	public void resumeTrigger(SchedulingContext ctxt, String triggerName,
			String groupName) {
		logDebug("Resuming Trigger: ", triggerName, ".", groupName);
		String key = TriggerWrapper.getTriggerNameKey(triggerName, groupName);

		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(triggerDomain, key)
						.withConsistentRead(new Boolean(true)));
		TriggerWrapper tw = null;

		try {
			tw = triggerFromAttributes(result.getAttributes());
		} catch (Exception e) {
			log.error("Could not create trigger for Item: " + result.toString());
		}

		// does the trigger exist?
		if (tw == null || tw.trigger == null) {
			return;
		}

		// if the trigger is not paused resuming it does not make sense...
		if (tw.state != TriggerWrapper.STATE_PAUSED
				&& tw.state != TriggerWrapper.STATE_PAUSED_BLOCKED) {
			return;
		}
		applyMisfire(tw);

	}

	/**
	 * <p>
	 * Resume (un-pause) all of the <code>{@link Trigger}s</code> in the given
	 * group.
	 * </p>
	 * 
	 * <p>
	 * If any <code>Trigger</code> missed one or more fire-times, then the
	 * <code>Trigger</code>'s misfire instruction will be applied.
	 * </p>
	 * 
	 */
	@Override
	public void resumeTriggerGroup(SchedulingContext ctxt, String groupName) {
		logDebug("Resuming all triggers of group: ", groupName);
		String[] names = getTriggerNames(ctxt, groupName);
		for (int i = 0; i < names.length; i++) {
			resumeTrigger(ctxt, names[i], groupName);
		}
	}

	/**
	 * <p>
	 * Resume (un-pause) the <code>{@link org.quartz.JobDetail}</code> with the
	 * given name.
	 * </p>
	 * 
	 * <p>
	 * If any of the <code>Job</code>'s<code>Trigger</code> s missed one or more
	 * fire-times, then the <code>Trigger</code>'s misfire instruction will be
	 * applied.
	 * </p>
	 * 
	 */
	@Override
	public void resumeJob(SchedulingContext ctxt, String jobName,
			String groupName) {
		logDebug("Resuming all triggers for Job: ", jobName, ".", groupName);
		Trigger[] triggers = getTriggersForJob(ctxt, jobName, groupName);
		for (int j = 0; j < triggers.length; j++) {
			resumeTrigger(ctxt, triggers[j].getName(), triggers[j].getGroup());
		}
	}

	/**
	 * <p>
	 * Resume (un-pause) all of the <code>{@link org.quartz.JobDetail}s</code>
	 * in the given group.
	 * </p>
	 * 
	 * <p>
	 * If any of the <code>Job</code> s had <code>Trigger</code> s that missed
	 * one or more fire-times, then the <code>Trigger</code>'s misfire
	 * instruction will be applied.
	 * </p>
	 * 
	 */
	@Override
	public void resumeJobGroup(SchedulingContext ctxt, String groupName) {
		logDebug("Resuming all jobs of group: ", groupName);
		String[] jobNames = getJobNames(ctxt, groupName);

		for (int i = 0; i < jobNames.length; i++) {
			Trigger[] triggers = getTriggersForJob(ctxt, jobNames[i], groupName);
			for (int j = 0; j < triggers.length; j++) {
				resumeTrigger(ctxt, triggers[j].getName(),
						triggers[j].getGroup());
			}
		}
	}

	/**
	 * <p>
	 * Pause all triggers - equivalent of calling
	 * <code>pauseTriggerGroup(group)</code> on every group.
	 * </p>
	 * 
	 * <p>
	 * When <code>resumeAll()</code> is called (to un-pause), trigger misfire
	 * instructions WILL be applied.
	 * </p>
	 * 
	 * @see #resumeAll(SchedulingContext)
	 * @see #pauseTriggerGroup(SchedulingContext, String)
	 */
	@Override
	public void pauseAll(SchedulingContext ctxt) {
		logDebug("Pausing all triggers");
		String[] names = getTriggerGroupNames(ctxt);
		for (int i = 0; i < names.length; i++) {
			pauseTriggerGroup(ctxt, names[i]);
		}
	}

	/**
	 * <p>
	 * Resume (un-pause) all triggers - equivalent of calling
	 * <code>resumeTriggerGroup(group)</code> on every group.
	 * </p>
	 * 
	 * <p>
	 * If any <code>Trigger</code> missed one or more fire-times, then the
	 * <code>Trigger</code>'s misfire instruction will be applied.
	 * </p>
	 * 
	 * @see #pauseAll(SchedulingContext)
	 */
	@Override
	public void resumeAll(SchedulingContext ctxt) {
		logDebug("Resuming all triggers");
		String[] names = getTriggerGroupNames(ctxt);
		for (int i = 0; i < names.length; i++) {
			resumeTriggerGroup(ctxt, names[i]);
		}
	}

	protected boolean applyMisfire(TriggerWrapper tw) {
		logDebug("Applying misfire on Trigger: ", tw.trigger.getFullName());

		long misfireTime = System.currentTimeMillis();
		if (getMisfireThreshold() > 0) {
			misfireTime -= getMisfireThreshold();
		}

		Date tnft = tw.trigger.getNextFireTime();
		if (tnft == null || tnft.getTime() > misfireTime) {
			return false;
		}

		Calendar cal = null;
		if (tw.trigger.getCalendarName() != null) {
			cal = retrieveCalendar(null, tw.trigger.getCalendarName());
		}

		signaler.notifyTriggerListenersMisfired((Trigger) tw.trigger.clone());

		tw.trigger.updateAfterMisfire(cal);

		if (tw.trigger.getNextFireTime() == null) {
			tw.state = TriggerWrapper.STATE_COMPLETE;
			signaler.notifySchedulerListenersFinalized(tw.trigger);
			removeTrigger(null, tw.trigger.getName(), tw.trigger.getGroup());
		} else if (tnft.equals(tw.trigger.getNextFireTime())) {
			return false;
		}

		return true;
	}

	private static AtomicLong ftrCtr = new AtomicLong(
			System.currentTimeMillis());

	protected String getFiredTriggerRecordId() {
		return String.valueOf(ftrCtr.incrementAndGet());
	}

	/**
	 * <p>
	 * Get a handle to the next trigger to be fired, and mark it as 'reserved'
	 * by the calling scheduler.
	 * </p>
	 * 
	 * @see #releaseAcquiredTrigger(SchedulingContext, Trigger)
	 */
	@Override
	public Trigger acquireNextTrigger(SchedulingContext ctxt, long noLaterThan) {
		log.info("Acquiring next trigger: "
				+ query.acquireTrigger(dateFormat.format(new Date(noLaterThan))));
		SelectResult result = amazonSimpleDb.select(new SelectRequest(query
				.acquireTrigger(dateFormat.format(new Date(noLaterThan)))));
		List<Item> items = result.getItems();

		if (items.size() == 1) {
			try {
				TriggerWrapper tw = triggerFromAttributes(items.get(0)
						.getAttributes());
				logDebug("Acquired next Trigger: ", tw.trigger.getFullName());
				if (tw.trigger.getNextFireTime() != null) {
					tw.state = TriggerWrapper.STATE_ACQUIRED;
					updateState(tw);
					return tw.trigger;
				} else {
					removeTrigger(ctxt, tw.trigger.getName(),
							tw.trigger.getGroup());
				}
			} catch (Exception e) {
				log.error("Could not acquire trigger", e);
			}
		}
		return null;
	}

	/**
	 * <p>
	 * Inform the <code>JobStore</code> that the scheduler no longer plans to
	 * fire the given <code>Trigger</code>, that it had previously acquired
	 * (reserved).
	 * </p>
	 */
	@Override
	public void releaseAcquiredTrigger(SchedulingContext ctxt, Trigger trigger) {
		logDebug("Releasing Trigger: ", trigger.getFullName());
		String key = TriggerWrapper.getTriggerNameKey(trigger);
		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(triggerDomain, key)
						.withConsistentRead(new Boolean(true)));
		TriggerWrapper tw = null;
		try {
			tw = triggerFromAttributes(result.getAttributes());
			if (tw.state == TriggerWrapper.STATE_ACQUIRED) {
				tw.state = TriggerWrapper.STATE_WAITING;
				updateState(tw);
			}
		} catch (Exception e) {
			log.error("Could not release Trigger: " + trigger.getFullName(), e);
		}
	}

	/**
	 * <p>
	 * Inform the <code>JobStore</code> that the scheduler is now firing the
	 * given <code>Trigger</code> (executing its associated <code>Job</code>),
	 * that it had previously acquired (reserved).
	 * </p>
	 */
	@Override
	public TriggerFiredBundle triggerFired(SchedulingContext ctxt,
			Trigger trigger) {

		logDebug("Fired Trigger: ", trigger.getFullName());
		String key = TriggerWrapper.getTriggerNameKey(trigger);
		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(triggerDomain, key)
						.withConsistentRead(new Boolean(true)));
		TriggerWrapper tw = null;
		try {
			tw = triggerFromAttributes(result.getAttributes());
		} catch (Exception e) {
			log.error("Could not load Trigger: " + trigger.getFullName());
		}

		// was the trigger deleted since being acquired?
		if (tw == null || tw.trigger == null) {
			return null;
		}
		// was the trigger completed, paused, blocked, etc. since being
		// acquired?
		if (tw.state != TriggerWrapper.STATE_ACQUIRED) {
			return null;
		}

		Calendar cal = null;
		if (tw.trigger.getCalendarName() != null) {
			cal = retrieveCalendar(ctxt, tw.trigger.getCalendarName());
			if (cal == null)
				return null;
		}
		Date prevFireTime = trigger.getPreviousFireTime();
		// call triggered on our copy, and the scheduler's copy
		tw.trigger.triggered(cal);
		trigger.triggered(cal);

		try {
			removeTrigger(ctxt, trigger.getName(), trigger.getGroup());
			storeTrigger(ctxt, trigger, true);
			tw.state = TriggerWrapper.STATE_WAITING;
			updateState(tw);
		} catch (Exception e) {
			log.error("Error while firing Trigger: " + trigger.getFullName());
		}

		TriggerFiredBundle bndle = new TriggerFiredBundle(retrieveJob(ctxt,
				trigger.getJobName(), trigger.getJobGroup()), trigger, cal,
				false, new Date(), trigger.getPreviousFireTime(), prevFireTime,
				trigger.getNextFireTime());

		// JobDetail job = bndle.getJobDetail();
		// TODO: Handle concurrent job execution

		return bndle;
	}

	/**
	 * <p>
	 * Inform the <code>JobStore</code> that the scheduler has completed the
	 * firing of the given <code>Trigger</code> (and the execution its
	 * associated <code>Job</code>), and that the
	 * <code>{@link org.quartz.JobDataMap}</code> in the given
	 * <code>JobDetail</code> should be updated if the <code>Job</code> is
	 * stateful.
	 * </p>
	 */
	@Override
	public void triggeredJobComplete(SchedulingContext ctxt, Trigger trigger,
			JobDetail jobDetail, int triggerInstCode) {
		logDebug("Completing Job: ", trigger.getFullName());

		String jobKey = JobWrapper.getJobNameKey(jobDetail.getName(),
				jobDetail.getGroup());
		GetAttributesResult jobresult = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(jobDomain, jobKey)
						.withConsistentRead(new Boolean(true)));
		JobDetail job = null;
		try {
			job = jobDetailFromAttributes(jobresult.getAttributes());
			if (job.isStateful()) {
				// TODO: Implement support for stateful jobs
			}
		} catch (Exception e) {
			log.error(
					"Could not complete job for Trigger: "
							+ trigger.getFullName(), e);
		}

		String triggerKey = TriggerWrapper.getTriggerNameKey(trigger);
		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(triggerDomain,
						triggerKey).withConsistentRead(new Boolean(true)));
		TriggerWrapper tw = null;

		try {
			tw = triggerFromAttributes(result.getAttributes());
		} catch (Exception e) {
			log.error("Could not find Trigger: " + trigger.getFullName(), e);
		}

		// It's possible that the job is null if:
		// 1- it was deleted during execution
		// 2- RAMJobStore is being used only for volatile jobs / triggers
		// from the JDBC job store

		// check for trigger deleted during execution...
		if (tw != null) {
			if (triggerInstCode == Trigger.INSTRUCTION_DELETE_TRIGGER) {

				if (trigger.getNextFireTime() == null) {
					// double check for possible reschedule within job
					// execution, which would cancel the need to delete...
					if (tw.getTrigger().getNextFireTime() == null) {
						removeTrigger(ctxt, trigger.getName(),
								trigger.getGroup());
					}
				} else {
					removeTrigger(ctxt, trigger.getName(), trigger.getGroup());
					signaler.signalSchedulingChange(0L);
				}
			} else if (triggerInstCode == Trigger.INSTRUCTION_SET_TRIGGER_COMPLETE) {
				tw.state = TriggerWrapper.STATE_COMPLETE;
				removeTrigger(ctxt, trigger.getName(), trigger.getGroup());
				signaler.signalSchedulingChange(0L);
			} else if (triggerInstCode == Trigger.INSTRUCTION_SET_TRIGGER_ERROR) {
				getLog().info(
						"Trigger " + trigger.getFullName()
								+ " set to ERROR state.");
				tw.state = TriggerWrapper.STATE_ERROR;
				updateState(tw);
				signaler.signalSchedulingChange(0L);
			} else if (triggerInstCode == Trigger.INSTRUCTION_SET_ALL_JOB_TRIGGERS_ERROR) {
				getLog().info(
						"All triggers of Job " + trigger.getFullJobName()
								+ " set to ERROR state.");
				setAllTriggersOfJobToState(trigger.getJobName(),
						trigger.getJobGroup(), TriggerWrapper.STATE_ERROR);
				signaler.signalSchedulingChange(0L);
			} else if (triggerInstCode == Trigger.INSTRUCTION_SET_ALL_JOB_TRIGGERS_COMPLETE) {
				setAllTriggersOfJobToState(trigger.getJobName(),
						trigger.getJobGroup(), TriggerWrapper.STATE_COMPLETE);
				signaler.signalSchedulingChange(0L);
			}
		}
	}

	protected void setAllTriggersOfJobToState(String jobName, String jobGroup,
			int state) {
		logDebug("Setting state of all triggers of Job: ", jobName, ".",
				jobGroup);
		Trigger[] triggers = getTriggersForJob(null, jobName, jobGroup);
		for (int i = 0; i < triggers.length; i++) {
			TriggerWrapper tw = new TriggerWrapper(triggers[i]);
			if (state != TriggerWrapper.STATE_WAITING) {
				removeTrigger(null, tw.trigger.getName(), tw.trigger.getGroup());
			} else {
				tw.state = state;
				updateState(tw);
			}
		}
	}

	/**
	 * @see org.quartz.spi.JobStore#getPausedTriggerGroups(org.quartz.core.SchedulingContext)
	 */
	@Override
	public Set getPausedTriggerGroups(SchedulingContext ctxt)
			throws JobPersistenceException {
		return null;
	}

	@Override
	public void setInstanceId(String schedInstId) {
		//
	}

	@Override
	public void setInstanceName(String schedName) {
		//
	}

	@Override
	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		return 5;
	}

	@Override
	public boolean isClustered() {
		return false;
	}

	private void logDebug(Object... args) {
		if (log.isDebugEnabled()) {
			StringBuilder buffer = new StringBuilder(256);
			for (Object object : args) {
				buffer.append(object.toString());
			}
			log.debug(buffer.toString());
		}
	}

}

/*******************************************************************************
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * 
 * Helper Classes. * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * * * *
 */

class TriggerComparator implements Comparator {

	@Override
	public int compare(Object obj1, Object obj2) {
		TriggerWrapper trig1 = (TriggerWrapper) obj1;
		TriggerWrapper trig2 = (TriggerWrapper) obj2;

		int comp = trig1.trigger.compareTo(trig2.trigger);
		if (comp != 0) {
			return comp;
		}

		comp = trig2.trigger.getPriority() - trig1.trigger.getPriority();
		if (comp != 0) {
			return comp;
		}

		return trig1.trigger.getFullName().compareTo(
				trig2.trigger.getFullName());
	}

	@Override
	public boolean equals(Object obj) {
		return (obj instanceof TriggerComparator);
	}
}

class JobWrapper {

	public String key;

	public JobDetail jobDetail;

	JobWrapper(JobDetail jobDetail) {
		this.jobDetail = jobDetail;
		key = getJobNameKey(jobDetail);
	}

	JobWrapper(JobDetail jobDetail, String key) {
		this.jobDetail = jobDetail;
		this.key = key;
	}

	static String getJobNameKey(JobDetail jobDetail) {
		return jobDetail.getGroup() + "_$x$x$_" + jobDetail.getName();
	}

	static String getJobNameKey(String jobName, String groupName) {
		return groupName + "_$x$x$_" + jobName;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof JobWrapper) {
			JobWrapper jw = (JobWrapper) obj;
			if (jw.key.equals(this.key)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

}

class TriggerWrapper {

	public String key;
	public String jobKey;
	public Trigger trigger;

	public int state = STATE_WAITING;
	public static final int STATE_WAITING = 0;
	public static final int STATE_ACQUIRED = 1;
	public static final int STATE_EXECUTING = 2;
	public static final int STATE_COMPLETE = 3;
	public static final int STATE_PAUSED = 4;
	public static final int STATE_BLOCKED = 5;
	public static final int STATE_PAUSED_BLOCKED = 6;
	public static final int STATE_ERROR = 7;

	TriggerWrapper(Trigger trigger) {
		this.trigger = trigger;
		key = getTriggerNameKey(trigger);
		this.jobKey = JobWrapper.getJobNameKey(trigger.getJobName(),
				trigger.getJobGroup());
	}

	static String getTriggerNameKey(Trigger trigger) {
		return trigger.getGroup() + "_$x$x$_" + trigger.getName();
	}

	static String getTriggerNameKey(String triggerName, String groupName) {
		return groupName + "_$x$x$_" + triggerName;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TriggerWrapper) {
			TriggerWrapper tw = (TriggerWrapper) obj;
			if (tw.key.equals(this.key)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	public Trigger getTrigger() {
		return this.trigger;
	}

}
