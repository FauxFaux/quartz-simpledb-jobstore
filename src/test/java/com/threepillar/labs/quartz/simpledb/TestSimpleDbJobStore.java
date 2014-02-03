package com.threepillar.labs.quartz.simpledb;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;

@RunWith(JUnit4.class)
public class TestSimpleDbJobStore {

	private static final Log log = LogFactory.getLog(TestSimpleDbJobStore.class);

	private static final String TEST_CRON_EXPR = "0 0/5 * * * ?";

	private ObjectMapper mapper;

	private JobDetail job;

	private CronTrigger cronTrigger;
	private String prefix;
	private String awsAccessKey;
	private static String awsSecretKey;
	private static String jobDomain;

	private void readCredentialsIfAvailable() throws IOException {
		Properties p = new Properties();
		final InputStream resource = getClass().getResourceAsStream("/simpledb_test.properties");
		if (null == resource) {
			log.warn("provide a simpledb_test.properties if you want explicit credentials");
			awsAccessKey = "";
			awsSecretKey = "";
			return;
		}
		p.load(resource);
		awsAccessKey = p.getProperty("awsAccessKey");
		awsSecretKey = p.getProperty("awsSecretKey");
	}

	@Before
	public void setUp() throws Exception {
		// simpleDB configuration
		readCredentialsIfAvailable();
		prefix = String.format("%sSimpleDB", System.getProperty("user.name"));
		jobDomain = String.format("%s.%s", prefix, SimpleDbJobStore.JOB_DOMAIN);

		this.mapper = new ObjectMapper();
		this.mapper.getDeserializationConfig().addMixInAnnotations(
				CronTrigger.class, FixCronTriggerMixIn.class);
		this.mapper.getDeserializationConfig().addMixInAnnotations(
				JobDetail.class, FixJobDetailMixIn.class);
		this.mapper.getDeserializationConfig().addMixInAnnotations(
				Trigger.class, FixTriggerMixIn.class);

		job = new JobDetail();
		job.setName("testJob");
		job.setJobClass(TestJob.class);
		job.setJobDataMap(new JobDataMap(Collections.singletonMap("timeout",
				"5")));
		cronTrigger = new CronTrigger();
		cronTrigger.setJobName(job.getName());
		cronTrigger.setCronExpression(TEST_CRON_EXPR);
		cronTrigger.setJobDataMap(job.getJobDataMap());
		cronTrigger.setName("cronTrigger");
	}

	@Test
	public void testTriggerSerialization() throws Exception {

		String json = this.mapper.writeValueAsString(cronTrigger);
		String className = cronTrigger.getClass().getName();
		Class<? extends Trigger> clz = Class.forName(className).asSubclass(
				Trigger.class);
		CronTrigger newTrigger = (CronTrigger) this.mapper.readValue(json, clz);
		assertEquals(TEST_CRON_EXPR, newTrigger.getCronExpression());
	}

	@Test
	public void testRemoveJob() throws Exception {

		AmazonSimpleDBClient amazonSimpleDb = SimpleDbJobStore.makeSimpleDbClient(awsAccessKey, awsSecretKey);
		Properties quartzProperties = new Properties();
		quartzProperties.setProperty("org.quartz.scheduler.instanceName",
				"testScheduler");
		quartzProperties.setProperty("org.quartz.threadPool.class",
				"org.quartz.simpl.SimpleThreadPool");
		quartzProperties.setProperty("org.quartz.threadPool.threadCount", "3");
		quartzProperties.setProperty("org.quartz.jobStore.class",
				SimpleDbJobStore.class.getName());
		quartzProperties.setProperty("org.quartz.jobStore.awsAccessKey",
				awsAccessKey);
		quartzProperties.setProperty("org.quartz.jobStore.awsSecretKey",
				awsSecretKey);
		quartzProperties.setProperty("org.quartz.jobStore.prefix", prefix);
		quartzProperties.setProperty("org.quartz.jobStore.recreate", "true");
		StdSchedulerFactory sf = new StdSchedulerFactory();
		sf.initialize(quartzProperties);

		Scheduler scheduler = sf.getScheduler();
		scheduler.start();
		scheduler.scheduleJob(job, cronTrigger);

		String key = JobWrapper.getJobNameKey(job.getName(), job.getGroup());
		GetAttributesResult result = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(jobDomain, key)
						.withConsistentRead(new Boolean(true)));
		List<Attribute> attributes = result.getAttributes();
		if (attributes == null || attributes.size() == 0) {
			fail("Error occurred during execution");
		}
		Map<String, String> map = new HashMap<String, String>();
		for (Attribute attr : attributes) {
			map.put(attr.getName(), attr.getValue());
		}
		if (map.get("name") == null || !job.getName().equals(map.get("name"))) {
			fail("Error occurred during execution. Unable to retrieve the job.");
		}
		scheduler.deleteJob(job.getName(), job.getGroup());
		GetAttributesResult dresult = amazonSimpleDb
				.getAttributes(new GetAttributesRequest(jobDomain, key)
						.withConsistentRead(new Boolean(true)));
		List<Attribute> dattributes = dresult.getAttributes();
		if (dattributes.size() > 0) {
			fail("Error occurred during execution. Unable to delete the job.");
		}

		scheduler.shutdown();

	}
}
