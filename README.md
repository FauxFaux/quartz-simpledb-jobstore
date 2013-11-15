Quartz Simpledb Jobstore
========================

A Quartz job store for persistence of job details and triggers in Amazon SimpleDB. 

Getting Started
----------------

The project artifacts are available from Maven Central. The dependency information is as follows:

    <dependency>
        <groupId>com.3pillarglobal.labs</groupId>
        <artifactId>quartz-simpledb-jobstore</artifactId>
        <version>1.0</version>
    </dependency>

Configuration
--------------

The job store is configured in standard Quartz fashion by setting a Java `Properties` instance on the Quartz scheduler.

```java
    String awsAccessKey = ""; // AWS access key
    String awsSecretKey = ""; // AWS secret key
    Properties properties = new Properties();
    properties.setProperty("org.quartz.jobStore.class", 
                    "com.threepillar.labs.quartz.simpledb.SimpleDbJobStore");
    properties.setProperty("org.quartz.jobStore.awsAccessKey", awsAccessKey);
    properties.setProperty("org.quartz.jobStore.awsSecretKey", awsSecretKey);
```

### Modify SimpleDB domain prefix

By default, the jobstore will create 2 domains: "quartz.Jobs" and "quartz.Triggers", where `quartz` is the prefix. You can modify this prefix by setting a property:

```java
    String somePrefix = "yada";
    properties.setProperty("org.quartz.jobStore.prefix", somePrefix);
```

### Recreate domains on scheduler startup

Mostly useful for testing, this property will re-create the `Jobs` and `Triggers` domains everytime the Quartz scheduler is started.

```java
    properties.setProperty("org.quartz.jobStore.recreate", "true");
```
