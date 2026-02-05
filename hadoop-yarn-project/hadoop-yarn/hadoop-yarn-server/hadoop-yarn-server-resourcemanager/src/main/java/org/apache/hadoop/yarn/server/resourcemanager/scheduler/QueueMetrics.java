/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.metrics.CustomResourceMetricValue;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;

/**
 * Metrics tracking for YARN scheduler queues providing real-time visibility
 * into resource allocation, application states, and container management.
 *
 * <p>This class maintains per-queue metrics including allocated/pending/reserved
 * resources, application counts, and container allocation statistics. Metrics
 * are propagated hierarchically from child queues to parent queues.
 *
 * @performance Linear scaling with number of concurrent applications and queues.
 *              Metrics2 library provides thread-safe atomic operations via
 *              MutableGauge/MutableCounter classes ensuring O(1) individual updates.
 *              Aggregation overhead during getMetrics collection: O(q) where
 *              q=number of queues in hierarchy. Memory footprint scales linearly
 *              with number of users when user metrics are enabled.
 *              Source: QueueMetrics.java:188-208 (constructor), QueueMetrics.java:448-451 (getMetrics)
 *
 * @implNote Uses MutableGauge/MutableCounter from Hadoop metrics2 library for
 *           thread-safe O(1) atomic counter and gauge operations without explicit
 *           synchronization. Parent metrics aggregation strategy propagates all
 *           metric updates through the queue hierarchy by recursively calling
 *           parent.methodName() - this ensures consistent aggregated views at
 *           each queue level but introduces O(d) overhead where d=queue depth.
 *           ConcurrentHashMap used for QUEUE_METRICS cache (line 256-257) to
 *           enable lock-free concurrent access during queue registration while
 *           maintaining thread-safety. User metrics map uses HashMap protected
 *           by method-level synchronization since user creation is less frequent
 *           than metric updates.
 *           Source: QueueMetrics.java:256-257 (QUEUE_METRICS), QueueMetrics.java:200 (users map)
 */
@InterfaceAudience.Private
@Metrics(context="yarn")
public class QueueMetrics implements MetricsSource {
  @Metric("# of apps submitted") MutableCounterInt appsSubmitted;
  @Metric("# of running apps") MutableGaugeInt appsRunning;
  @Metric("# of pending apps") MutableGaugeInt appsPending;
  @Metric("# of apps completed") MutableCounterInt appsCompleted;
  @Metric("# of apps killed") MutableCounterInt appsKilled;
  @Metric("# of apps failed") MutableCounterInt appsFailed;

  @Metric("# of Unmanaged apps submitted")
  private MutableCounterInt unmanagedAppsSubmitted;
  @Metric("# of Unmanaged running apps")
  private MutableGaugeInt unmanagedAppsRunning;
  @Metric("# of Unmanaged pending apps")
  private MutableGaugeInt unmanagedAppsPending;
  @Metric("# of Unmanaged apps completed")
  private MutableCounterInt unmanagedAppsCompleted;
  @Metric("# of Unmanaged apps killed")
  private MutableCounterInt unmanagedAppsKilled;
  @Metric("# of Unmanaged apps failed")
  private MutableCounterInt unmanagedAppsFailed;

  @Metric("Aggregate # of allocated node-local containers")
    MutableCounterLong aggregateNodeLocalContainersAllocated;
  @Metric("Aggregate # of allocated rack-local containers")
    MutableCounterLong aggregateRackLocalContainersAllocated;
  @Metric("Aggregate # of allocated off-switch containers")
    MutableCounterLong aggregateOffSwitchContainersAllocated;
  @Metric("Aggregate # of preempted containers") MutableCounterLong
      aggregateContainersPreempted;
  @Metric("Aggregate # of preempted memory seconds") MutableCounterLong
      aggregateMemoryMBSecondsPreempted;
  @Metric("Aggregate # of preempted vcore seconds") MutableCounterLong
      aggregateVcoreSecondsPreempted;
  @Metric("# of active users") MutableGaugeInt activeUsers;
  @Metric("# of active applications") MutableGaugeInt activeApplications;
  @Metric("App Attempt First Container Allocation Delay")
    MutableRate appAttemptFirstContainerAllocationDelay;
  @Metric("Aggregate total of preempted memory MB")
    MutableCounterLong aggregateMemoryMBPreempted;
  @Metric("Aggregate total of preempted vcores")
    MutableCounterLong aggregateVcoresPreempted;

  //Metrics updated only for "default" partition
  @Metric("Allocated memory in MB") MutableGaugeLong allocatedMB;
  @Metric("Allocated CPU in virtual cores") MutableGaugeInt allocatedVCores;
  @Metric("# of allocated containers") MutableGaugeInt allocatedContainers;
  @Metric("Aggregate # of allocated containers")
    MutableCounterLong aggregateContainersAllocated;
  @Metric("Aggregate # of released containers")
    MutableCounterLong aggregateContainersReleased;
  @Metric("Available memory in MB") MutableGaugeLong availableMB;
  @Metric("Available CPU in virtual cores") MutableGaugeInt availableVCores;
  @Metric("Pending memory allocation in MB") MutableGaugeLong pendingMB;
  @Metric("Pending CPU allocation in virtual cores")
    MutableGaugeInt pendingVCores;
  @Metric("# of pending containers") MutableGaugeInt pendingContainers;
  @Metric("# of reserved memory in MB") MutableGaugeLong reservedMB;
  @Metric("Reserved CPU in virtual cores") MutableGaugeInt reservedVCores;
  @Metric("# of reserved containers") MutableGaugeInt reservedContainers;

  // INTERNAL ONLY
  private static final String CONFIGURATION_VALIDATION = "yarn.configuration-validation";

  private final MutableGaugeInt[] runningTime;
  private TimeBucketMetrics<ApplicationId> runBuckets;

  static final Logger LOG = LoggerFactory.getLogger(QueueMetrics.class);
  static final MetricsInfo RECORD_INFO = info("QueueMetrics",
      "Metrics for the resource scheduler");
  protected static final MetricsInfo QUEUE_INFO =
      info("Queue", "Metrics by queue");
  protected static final MetricsInfo USER_INFO =
      info("User", "Metrics by user");
  protected static final MetricsInfo PARTITION_INFO =
      info("Partition", "Metrics by partition");
  static final Splitter Q_SPLITTER =
      Splitter.on('.').omitEmptyStrings().trimResults();

  protected final MetricsRegistry registry;
  protected final String queueName;
  private QueueMetrics parent;
  private Queue parentQueue;
  protected final MetricsSystem metricsSystem;
  protected final Map<String, QueueMetrics> users;
  protected final Configuration conf;
  private QueueMetricsForCustomResources queueMetricsForCustomResources;

  private final boolean enableUserMetrics;

  protected static final MetricsInfo P_RECORD_INFO =
      info("PartitionQueueMetrics", "Metrics for the resource scheduler");

  // Use "default" to operate NO_LABEL (default) partition internally
  public static final String DEFAULT_PARTITION = "default";

  // Use "" to register NO_LABEL (default) partition into metrics system
  public static final String DEFAULT_PARTITION_JMX_STR = "";

  // Metric Name Delimiter
  public static final String METRIC_NAME_DELIMITER = ".";

  private static final String ALLOCATED_RESOURCE_METRIC_PREFIX =
      "AllocatedResource.";
  private static final String ALLOCATED_RESOURCE_METRIC_DESC =
    "Allocated NAME";

  private static final String AVAILABLE_RESOURCE_METRIC_PREFIX =
    "AvailableResource.";
  private static final String AVAILABLE_RESOURCE_METRIC_DESC =
    "Available NAME";

  private static final String PENDING_RESOURCE_METRIC_PREFIX =
    "PendingResource.";
  private static final String PENDING_RESOURCE_METRIC_DESC =
    "Pending NAME";

  private static final String RESERVED_RESOURCE_METRIC_PREFIX =
    "ReservedResource.";
  private static final String RESERVED_RESOURCE_METRIC_DESC =
    "Reserved NAME";

  private static final String AGGREGATE_PREEMPTED_SECONDS_METRIC_PREFIX =
    "AggregatePreemptedSeconds.";
  private static final String AGGREGATE_PREEMPTED_SECONDS_METRIC_DESC =
    "Aggregate Preempted Seconds for NAME";
  protected Set<String> storedPartitionMetrics = Sets.newConcurrentHashSet();

  public QueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {

    if (this instanceof PartitionQueueMetrics) {
      registry = new MetricsRegistry(P_RECORD_INFO);
    } else {
      registry = new MetricsRegistry(RECORD_INFO);
    }
    this.queueName = queueName;

    this.parent = parent != null ? parent.getMetrics() : null;
    this.parentQueue = parent;
    this.users = enableUserMetrics ? new HashMap<String, QueueMetrics>() : null;
    this.enableUserMetrics = enableUserMetrics;

    metricsSystem = ms;
    this.conf = conf;
    runningTime = buildBuckets(conf);

    createQueueMetricsForCustomResources();
  }

  protected QueueMetrics tag(MetricsInfo info, String value) {
    registry.tag(info, value);
    return this;
  }

  protected static StringBuilder sourceName(String queueName) {
    StringBuilder sb = new StringBuilder(RECORD_INFO.name());
    int i = 0;
    for (String node : Q_SPLITTER.split(queueName)) {
      sb.append(",q").append(i++).append('=').append(node);
    }
    return sb;
  }

  static StringBuilder pSourceName(String partition) {
    StringBuilder sb = new StringBuilder(P_RECORD_INFO.name());
    sb.append(",partition").append('=').append(partition);
    return sb;
  }

  static StringBuilder qSourceName(String queueName) {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (String node : Q_SPLITTER.split(queueName)) {
      sb.append(",q").append(i++).append('=').append(node);
    }
    return sb;
  }

  /**
   * Factory method to get or create QueueMetrics for a named queue.
   * Delegates to the overloaded method with the default MetricsSystem.
   *
   * @param queueName the name of the queue
   * @param parent the parent queue (may be null for root queue)
   * @param enableUserMetrics whether to track per-user metrics
   * @param conf the configuration
   * @return QueueMetrics instance for the specified queue
   *
   * @complexity Time: O(1) for cache hit; O(q) for cache miss where q=queue hierarchy
   *             depth due to parent queue traversal during construction and metric
   *             registration. Space: O(1) - returns cached or newly created reference.
   *             Source: QueueMetrics.java:268-285 (delegated forQueue implementation)
   */
  public synchronized static QueueMetrics forQueue(String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    return forQueue(DefaultMetricsSystem.instance(), queueName, parent,
        enableUserMetrics, conf);
  }

  /**
   * Helper method to clear cache.
   */
  @Private
  public synchronized static void clearQueueMetrics() {
    QUEUE_METRICS.clear();
  }

  /**
   * Simple metrics cache to help prevent re-registrations.
   */
  private static final Map<String, QueueMetrics> QUEUE_METRICS =
      new ConcurrentHashMap<>();

  /**
   * Returns the metrics cache to help prevent re-registrations.
   *
   * @return A string to {@link QueueMetrics} map.
   */
  public static Map<String, QueueMetrics> getQueueMetrics() {
    return QUEUE_METRICS;
  }

  /**
   * Factory method to get or create QueueMetrics for a named queue with
   * a specified MetricsSystem. Uses a ConcurrentHashMap cache (QUEUE_METRICS)
   * for efficient lookup and to prevent duplicate registrations.
   *
   * @param ms the MetricsSystem to register metrics with
   * @param queueName the name of the queue
   * @param parent the parent queue (may be null for root queue)
   * @param enableUserMetrics whether to track per-user metrics
   * @param conf the configuration
   * @return QueueMetrics instance for the specified queue
   *
   * @complexity Time: O(1) average for cache lookup in ConcurrentHashMap;
   *             O(q) worst-case for cache miss where q=queue hierarchy depth
   *             due to parent traversal in constructor (line 198) and queue name
   *             parsing in sourceName() which iterates through hierarchical queue
   *             path components. Space: O(1) for cache hit; O(m) for cache miss
   *             where m=number of metric fields initialized in new QueueMetrics.
   *             Source: QueueMetrics.java:268-285
   *
   * @implNote Cache lookup uses ConcurrentHashMap.get() which is lock-free for
   *           reads. Synchronized method prevents race conditions during cache
   *           miss handling where new QueueMetrics creation and registration
   *           must be atomic to avoid duplicate metrics registrations.
   */
  public synchronized static QueueMetrics forQueue(MetricsSystem ms,
      String queueName, Queue parent, boolean enableUserMetrics,
      Configuration conf) {
    QueueMetrics metrics = getQueueMetrics().get(queueName);
    if (metrics == null) {
      metrics = new QueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
          .tag(QUEUE_INFO, queueName);

      // Register with the MetricsSystems
      if (ms != null) {
        metrics = ms.register(sourceName(queueName).toString(),
            "Metrics for queue: " + queueName, metrics);
      }
      getQueueMetrics().put(queueName, metrics);
    }

    return metrics;
  }

  /**
   * Gets or creates user-specific QueueMetrics for tracking per-user resource usage.
   * User metrics are lazily created on first access if user metrics are enabled.
   *
   * @param userName the name of the user
   * @return QueueMetrics for the user, or null if user metrics are disabled
   *
   * @complexity Time: O(1) average for HashMap lookup; O(m) for cache miss where
   *             m=number of metric fields initialized in new QueueMetrics.
   *             Space: O(1) for existing user; O(m) for new user metrics creation.
   *             Source: QueueMetrics.java:287-302
   *
   * @implNote Synchronized method protects HashMap access since user metrics
   *           creation is less frequent than metric updates. Uses standard HashMap
   *           (not ConcurrentHashMap) since method-level synchronization provides
   *           sufficient thread-safety for user registration use case.
   */
  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }
    QueueMetrics metrics = users.get(userName);
    if (metrics == null) {
      metrics =
          new QueueMetrics(metricsSystem, queueName, null, false, conf);
      users.put(userName, metrics);
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '"+ userName +"' in queue '"+ queueName +"'",
          metrics.tag(QUEUE_INFO, queueName).tag(USER_INFO, userName));
    }
    return metrics;
  }

  /**
   * Partition * Queue Metrics
   *
   * Computes Metrics at Partition (Node Label) * Queue Level.
   *
   * Sample JMX O/P Structure:
   *
   * PartitionQueueMetrics (labelX)
   *  QueueMetrics (A)
   *    metrics
   *    QueueMetrics (A1)
   *      metrics
   *    QueueMetrics (A2)
   *      metrics
   *  QueueMetrics (B)
   *    metrics
   *
   * @param partition Node Partition
   * @return QueueMetrics
   */
  public synchronized QueueMetrics getPartitionQueueMetrics(String partition) {

    String partitionJMXStr = partition;

    if ((partition == null)
        || (partition.equals(RMNodeLabelsManager.NO_LABEL))) {
      partition = DEFAULT_PARTITION;
      partitionJMXStr = DEFAULT_PARTITION_JMX_STR;
    }

    String metricName = partition + METRIC_NAME_DELIMITER + this.queueName;
    QueueMetrics metrics = getQueueMetrics().get(metricName);

    if (metrics == null) {
      QueueMetrics queueMetrics =
          new PartitionQueueMetrics(metricsSystem, this.queueName, parentQueue,
              this.enableUserMetrics, this.conf, partition);
      metricsSystem.register(
          pSourceName(partitionJMXStr).append(qSourceName(this.queueName))
              .toString(),
          "Metrics for queue: " + this.queueName,
          queueMetrics.tag(PARTITION_INFO, partitionJMXStr).tag(QUEUE_INFO,
              this.queueName));
      if (!isConfigurationValidationSet(conf)) {
        getQueueMetrics().put(metricName, queueMetrics);
      }
      registerPartitionMetricsCreation(metricName);
      return queueMetrics;
    } else {
      return metrics;
    }
  }

  /**
   * Check whether we are in a configuration validation mode. INTERNAL ONLY.
   *
   * @param conf the configuration to check
   * @return true if
   */
  public static boolean isConfigurationValidationSet(Configuration conf) {
    return conf.getBoolean(CONFIGURATION_VALIDATION, false);
  }

  /**
   * Set configuration validation mode. INTERNAL ONLY.
   *
   * @param conf the configuration to update
   * @param value the value for the validation mode
   */
  public static void setConfigurationValidation(Configuration conf, boolean value) {
    conf.setBoolean(CONFIGURATION_VALIDATION, value);
  }

  /**
   * Partition Metrics
   *
   * Computes Metrics at Partition (Node Label) Level.
   *
   * Sample JMX O/P Structure:
   *
   * PartitionQueueMetrics (labelX)
   *  metrics
   *
   * @param partition
   * @return QueueMetrics
   */
  private QueueMetrics getPartitionMetrics(String partition) {

    String partitionJMXStr = partition;
    if ((partition == null)
        || (partition.equals(RMNodeLabelsManager.NO_LABEL))) {
      partition = DEFAULT_PARTITION;
      partitionJMXStr = DEFAULT_PARTITION_JMX_STR;
    }

    String metricName = partition + METRIC_NAME_DELIMITER;
    QueueMetrics metrics = getQueueMetrics().get(metricName);
    if (metrics == null) {
      metrics = new PartitionQueueMetrics(metricsSystem, this.queueName, null,
          false, this.conf, partition);

      // Register with the MetricsSystems
      if (metricsSystem != null) {
        metricsSystem.register(pSourceName(partitionJMXStr).toString(),
            "Metrics for partition: " + partitionJMXStr,
            (PartitionQueueMetrics) metrics.tag(PARTITION_INFO,
                partitionJMXStr));
      }
      getQueueMetrics().put(metricName, metrics);
      registerPartitionMetricsCreation(metricName);
    }
    return metrics;
  }

  private ArrayList<Integer> parseInts(String value) {
    ArrayList<Integer> result = new ArrayList<Integer>();
    for(String s: value.split(",")) {
      result.add(Integer.parseInt(s.trim()));
    }
    return result;
  }

  private MutableGaugeInt[] buildBuckets(Configuration conf) {
    ArrayList<Integer> buckets = 
      parseInts(conf.get(YarnConfiguration.RM_METRICS_RUNTIME_BUCKETS,
		        YarnConfiguration.DEFAULT_RM_METRICS_RUNTIME_BUCKETS));
    MutableGaugeInt[] result = new MutableGaugeInt[buckets.size() + 1];
    result[0] = registry.newGauge("running_0", "", 0);
    long[] cuts = new long[buckets.size()];
    for(int i=0; i < buckets.size(); ++i) {
      result[i+1] = registry.newGauge("running_" + buckets.get(i), "", 0);
      cuts[i] = buckets.get(i) * 1000L * 60; // covert from min to ms
    }
    this.runBuckets = new TimeBucketMetrics<ApplicationId>(cuts);
    return result;
  }

  private void updateRunningTime() {
    int[] counts = runBuckets.getBucketCounts(System.currentTimeMillis());
    for(int i=0; i < counts.length; ++i) {
      runningTime[i].set(counts[i]); 
    }
  }

  /**
   * Collects all metrics for this queue into the provided MetricsCollector.
   * Called by the metrics2 framework during periodic metric collection.
   *
   * @param collector the MetricsCollector to receive the metrics
   * @param all if true, include all metrics; if false, include only changed metrics
   *
   * @complexity Time: O(b + m) where b=number of time buckets for updateRunningTime()
   *             and m=number of registered metrics in the registry for snapshot
   *             iteration. Typical case: O(1) as bucket count and metric count are
   *             bounded constants (~50 standard metrics per queue).
   *             Space: O(1) for in-memory retrieval - metrics values already stored.
   *             Source: QueueMetrics.java:448-451
   *
   * @implNote updateRunningTime() iterates through fixed-size runningTime bucket
   *           array (configured via RM_METRICS_RUNTIME_BUCKETS, default ~5 buckets).
   *           registry.snapshot() iterates through all registered MutableMetric
   *           instances but performs constant-time value retrieval for each.
   */
  public void getMetrics(MetricsCollector collector, boolean all) {
    updateRunningTime();
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  /**
   * Records a new application submission to this queue. Increments the
   * appsSubmitted counter and propagates to user metrics and parent queue.
   *
   * @param user the user who submitted the application
   * @param unmanagedAM true if the application uses an unmanaged ApplicationMaster
   *
   * @complexity Time: O(d) where d=queue hierarchy depth due to recursive parent
   *             propagation (parent.submitApp call). Each level performs O(1)
   *             atomic counter increment via MutableCounterInt.incr().
   *             Space: O(d) stack depth for recursive parent calls.
   *             Source: QueueMetrics.java:453-465
   *
   * @implNote Counter increments use MutableCounterInt.incr() which performs
   *           atomic increment without synchronization overhead. User metrics
   *           lookup may trigger synchronized getUserMetrics() on first access.
   */
  public void submitApp(String user, boolean unmanagedAM) {
    // @PerformanceCritical: Counter increment - O(1) atomic operation
    appsSubmitted.incr();
    if(unmanagedAM) {
      unmanagedAppsSubmitted.incr();
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.submitApp(user, unmanagedAM);
    }
    if (parent != null) {
      parent.submitApp(user, unmanagedAM);
    }
  }


  /**
   * Records a new application attempt submission. Increments pending apps
   * gauge and propagates to user metrics and parent queue hierarchy.
   *
   * @param user the user who submitted the application attempt
   * @param unmanagedAM true if the application uses an unmanaged ApplicationMaster
   *
   * @complexity Time: O(d) where d=queue hierarchy depth due to recursive parent
   *             propagation (parent.submitAppAttempt call). Each level performs
   *             O(1) atomic gauge increment via MutableGaugeInt.incr().
   *             Space: O(d) stack depth for recursive parent calls.
   *             Source: QueueMetrics.java:468-480
   *
   * @implNote Gauge increments use MutableGaugeInt.incr() providing thread-safe
   *           atomic updates. appsPending tracks current pending count (gauge)
   *           unlike appsSubmitted which is a monotonic counter.
   */
  public void submitAppAttempt(String user, boolean unmanagedAM) {
    // @PerformanceCritical: Gauge increment - O(1) atomic operation
    appsPending.incr();
    if(unmanagedAM) {
      unmanagedAppsPending.incr();
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.submitAppAttempt(user, unmanagedAM);
    }
    if (parent != null) {
      parent.submitAppAttempt(user, unmanagedAM);
    }
  }

  public void runAppAttempt(ApplicationId appId, String user,
      boolean unmanagedAM) {
    runBuckets.add(appId, System.currentTimeMillis());
    appsRunning.incr();
    appsPending.decr();

    if(unmanagedAM) {
      unmanagedAppsRunning.incr();
      unmanagedAppsPending.decr();
    }

    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.runAppAttempt(appId, user, unmanagedAM);
    }
    if (parent != null) {
      parent.runAppAttempt(appId, user, unmanagedAM);
    }
  }

  public void finishAppAttempt(ApplicationId appId, boolean isPending,
      String user, boolean unmanagedAM) {
    runBuckets.remove(appId);
    if (isPending) {
      appsPending.decr();
    } else {
      appsRunning.decr();
    }

    if(unmanagedAM) {
      if (isPending) {
        unmanagedAppsPending.decr();
      } else {
        unmanagedAppsRunning.decr();
      }
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.finishAppAttempt(appId, isPending, user, unmanagedAM);
    }
    if (parent != null) {
      parent.finishAppAttempt(appId, isPending, user, unmanagedAM);
    }
  }

  public void finishApp(String user, RMAppState rmAppFinalState,
      boolean unmanagedAM) {
    switch (rmAppFinalState) {
      case KILLED: appsKilled.incr(); break;
      case FAILED: appsFailed.incr(); break;
      default: appsCompleted.incr();  break;
    }

    if(unmanagedAM) {
      switch (rmAppFinalState) {
      case KILLED:
        unmanagedAppsKilled.incr();
        break;
      case FAILED:
        unmanagedAppsFailed.incr();
        break;
      default:
        unmanagedAppsCompleted.incr();
        break;
      }
    }

    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.finishApp(user, rmAppFinalState, unmanagedAM);
    }
    if (parent != null) {
      parent.finishApp(user, rmAppFinalState, unmanagedAM);
    }
  }


  public void moveAppFrom(AppSchedulingInfo app, boolean unmanagedAM) {
    if (app.isPending()) {
      appsPending.decr();
    } else {
      appsRunning.decr();
    }
    if(unmanagedAM) {
      if (app.isPending()) {
        unmanagedAppsPending.decr();
      } else {
        unmanagedAppsRunning.decr();
      }
    }

    QueueMetrics userMetrics = getUserMetrics(app.getUser());
    if (userMetrics != null) {
      userMetrics.moveAppFrom(app, unmanagedAM);
    }
    if (parent != null) {
      parent.moveAppFrom(app, unmanagedAM);
    }
  }

  public void moveAppTo(AppSchedulingInfo app, boolean unmanagedAM) {
    if (app.isPending()) {
      appsPending.incr();
    } else {
      appsRunning.incr();
    }
    if(unmanagedAM) {
      if (app.isPending()) {
        unmanagedAppsPending.incr();
      } else {
        unmanagedAppsRunning.incr();
      }
    }
    QueueMetrics userMetrics = getUserMetrics(app.getUser());
    if (userMetrics != null) {
      userMetrics.moveAppTo(app, unmanagedAM);
    }
    if (parent != null) {
      parent.moveAppTo(app, unmanagedAM);
    }
  }


  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   * @param partition Node Partition
   * @param limit resource limit
   */
  public void setAvailableResourcesToQueue(String partition, Resource limit) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      setAvailableResources(limit);
    }

    QueueMetrics partitionQueueMetrics = getPartitionQueueMetrics(partition);
    if (partitionQueueMetrics != null) {
      partitionQueueMetrics.setAvailableResources(limit);

      if(this.queueName.equals("root")) {
        QueueMetrics partitionMetrics = getPartitionMetrics(partition);
        if (partitionMetrics != null) {
          partitionMetrics.setAvailableResources(limit);
        }
      }
    }
  }

  /**
   * Set Available resources with support for resource vectors.
   *
   * @param limit Resource.
   */
  public void setAvailableResources(Resource limit) {
    availableMB.set(limit.getMemorySize());
    availableVCores.set(limit.getVirtualCores());
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.setAvailable(limit);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getAvailableValues(), registry,
          AVAILABLE_RESOURCE_METRIC_PREFIX, AVAILABLE_RESOURCE_METRIC_DESC);
    }
  }

  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   *
   * @param limit resource limit
   */
  public void setAvailableResourcesToQueue(Resource limit) {
    this.setAvailableResourcesToQueue(RMNodeLabelsManager.NO_LABEL, limit);
  }

  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   *
   * @param partition Node Partition
   * @param user Name of the user.
   * @param limit resource limit
   */
  public void setAvailableResourcesToUser(String partition, String user,
      Resource limit) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      QueueMetrics userMetrics = getUserMetrics(user);
      if (userMetrics != null) {
        userMetrics.setAvailableResources(limit);
      }
    }

    QueueMetrics partitionQueueMetrics = getPartitionQueueMetrics(partition);
    if (partitionQueueMetrics != null) {
      QueueMetrics partitionUserMetrics =
          partitionQueueMetrics.getUserMetrics(user);
      if (partitionUserMetrics != null) {
        partitionUserMetrics.setAvailableResources(limit);
      }
    }
  }

  /**
   * Increment pending resource metrics for a partition and user.
   * Updates pending containers, memory, vcores, and custom resources.
   *
   * @param partition Node Partition
   * @param user Name of the user.
   * @param containers containers count.
   * @param res the TOTAL delta of resources note this is different from the
   *          other APIs which use per container resource
   *
   * @complexity Time: O(d + p) where d=queue hierarchy depth for parent propagation
   *             via internalIncrPendingResources, and p=partition metrics lookup/creation.
   *             Each gauge update is O(1) amortized via MutableGaugeLong/MutableGaugeInt.
   *             Custom resource updates add O(r) where r=number of custom resource types.
   *             Space: O(1) for existing metrics; O(m) if new partition metrics created
   *             where m=number of metric fields.
   *             Source: QueueMetrics.java:691-707
   *
   * @implNote Partitioned metrics support enables per-node-label resource tracking.
   *           Internal method propagates updates through queue hierarchy and user
   *           metrics. Gauge operations (incr) are atomic and thread-safe.
   */
  public void incrPendingResources(String partition, String user,
      int containers, Resource res) {

    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      // @PerformanceCritical: Default partition update path - O(d) hierarchy traversal
      internalIncrPendingResources(partition, user, containers, res);
    }

    QueueMetrics partitionQueueMetrics = getPartitionQueueMetrics(partition);
    if (partitionQueueMetrics != null) {
      partitionQueueMetrics.internalIncrPendingResources(partition, user,
          containers, res);
      QueueMetrics partitionMetrics = getPartitionMetrics(partition);
      if (partitionMetrics != null) {
        partitionMetrics.incrementPendingResources(containers, res);
      }
    }
  }

  public void internalIncrPendingResources(String partition, String user,
      int containers, Resource res) {
    incrementPendingResources(containers, res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.internalIncrPendingResources(partition, user, containers,
          res);
    }
    if (parent != null) {
      parent.internalIncrPendingResources(partition, user, containers, res);
    }
  }


  protected void createQueueMetricsForCustomResources() {
    if (ResourceUtils.getNumberOfKnownResourceTypes() > 2) {
      this.queueMetricsForCustomResources =
          new QueueMetricsForCustomResources();
      registerCustomResources();
    }
  }

  protected void registerCustomResources() {
    Map<String, Long> customResources =
        queueMetricsForCustomResources.initAndGetCustomResources();
    queueMetricsForCustomResources
        .registerCustomResources(customResources, this.registry);
    queueMetricsForCustomResources
        .registerCustomResources(customResources, this.registry,
            PENDING_RESOURCE_METRIC_PREFIX, PENDING_RESOURCE_METRIC_DESC);
    queueMetricsForCustomResources
        .registerCustomResources(customResources, this.registry,
            RESERVED_RESOURCE_METRIC_PREFIX, RESERVED_RESOURCE_METRIC_DESC);
    queueMetricsForCustomResources
        .registerCustomResources(customResources, this.registry,
            AGGREGATE_PREEMPTED_SECONDS_METRIC_PREFIX,
            AGGREGATE_PREEMPTED_SECONDS_METRIC_DESC);
  }

  private void incrementPendingResources(int containers, Resource res) {
    pendingContainers.incr(containers);
    pendingMB.incr(res.getMemorySize() * containers);
    pendingVCores.incr(res.getVirtualCores() * containers);
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.increasePending(res, containers);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getPendingValues(), this.registry,
          PENDING_RESOURCE_METRIC_PREFIX, PENDING_RESOURCE_METRIC_DESC);
    }
  }

  public void decrPendingResources(String partition, String user,
      int containers, Resource res) {

    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      internalDecrPendingResources(partition, user, containers, res);
    }

    QueueMetrics partitionQueueMetrics = getPartitionQueueMetrics(partition);
    if (partitionQueueMetrics != null) {
      partitionQueueMetrics.internalDecrPendingResources(partition, user,
          containers, res);
      QueueMetrics partitionMetrics = getPartitionMetrics(partition);
      if (partitionMetrics != null) {
        partitionMetrics.decrementPendingResources(containers, res);
      }
    }
  }

  protected void internalDecrPendingResources(String partition, String user,
      int containers, Resource res) {
    decrementPendingResources(containers, res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.internalDecrPendingResources(partition, user, containers,
          res);
    }
    if (parent != null) {
      parent.internalDecrPendingResources(partition, user, containers, res);
    }
  }

  private void decrementPendingResources(int containers, Resource res) {
    pendingContainers.decr(containers);
    pendingMB.decr(res.getMemorySize() * containers);
    pendingVCores.decr(res.getVirtualCores() * containers);
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.decreasePending(res, containers);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getPendingValues(), this.registry,
          PENDING_RESOURCE_METRIC_PREFIX, PENDING_RESOURCE_METRIC_DESC);
    }
  }

  public void incrNodeTypeAggregations(String user, NodeType type) {
    if (type == NodeType.NODE_LOCAL) {
      aggregateNodeLocalContainersAllocated.incr();
    } else if (type == NodeType.RACK_LOCAL) {
      aggregateRackLocalContainersAllocated.incr();
    } else if (type == NodeType.OFF_SWITCH) {
      aggregateOffSwitchContainersAllocated.incr();
    } else {
      return;
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.incrNodeTypeAggregations(user, type);
    }
    if (parent != null) {
      parent.incrNodeTypeAggregations(user, type);
    }
  }

  /**
   * Records resource allocation for containers. Updates allocated counters
   * and optionally decrements pending resources. Propagates through queue
   * hierarchy and partition-specific metrics.
   *
   * @param partition Node Partition
   * @param user Name of the user
   * @param containers number of containers allocated
   * @param res resource per container (memory, vcores, custom resources)
   * @param decrPending if true, decrement pending resources by allocated amount
   *
   * @complexity Time: O(d + p + r) where d=queue hierarchy depth for parent propagation,
   *             p=partition metrics operations, r=number of custom resource types.
   *             Each counter/gauge operation is O(1) atomic via MutableCounter/MutableGauge.
   *             Space: O(1) for counter operations; no additional allocations.
   *             Source: QueueMetrics.java:822-838
   *
   * @implNote Allocation metrics include both instantaneous gauges (allocatedContainers,
   *           allocatedMB, allocatedVCores) and cumulative counters (aggregateContainersAllocated).
   *           Dual update pattern provides both point-in-time and historical views.
   */
  public void allocateResources(String partition, String user, int containers,
      Resource res, boolean decrPending) {

    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      // @PerformanceCritical: Container allocation hot path - O(d) hierarchy updates
      internalAllocateResources(partition, user, containers, res, decrPending);
    }

    QueueMetrics partitionQueueMetrics = getPartitionQueueMetrics(partition);
    if (partitionQueueMetrics != null) {
      partitionQueueMetrics.internalAllocateResources(partition, user,
          containers, res, decrPending);
      QueueMetrics partitionMetrics = getPartitionMetrics(partition);
      if (partitionMetrics != null) {
        partitionMetrics.computeAllocateResources(containers, res, decrPending);
      }
    }
  }

  /**
   * Internal allocation propagation through queue hierarchy. Updates this queue's
   * metrics and recursively propagates to user metrics and parent queue.
   *
   * @param partition Node Partition
   * @param user Name of the user
   * @param containers number of containers allocated
   * @param res resource per container
   * @param decrPending if true, decrement pending resources
   *
   * @complexity Time: O(d) where d=queue hierarchy depth due to recursive parent
   *             calls. computeAllocateResources is O(r) where r=custom resource count.
   *             Space: O(d) stack depth for recursion.
   *             Source: QueueMetrics.java:840-852
   */
  public void internalAllocateResources(String partition, String user,
      int containers, Resource res, boolean decrPending) {
    computeAllocateResources(containers, res, decrPending);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.internalAllocateResources(partition, user, containers, res,
          decrPending);
    }
    if (parent != null) {
      parent.internalAllocateResources(partition, user, containers, res,
          decrPending);
    }
  }

  /**
   * Computes and updates allocation metrics for containers with support for
   * resource vectors including custom resources.
   *
   * @param containers number of containers
   * @param res resource containing memory size, vcores etc
   * @param decrPending decides whether to decrease pending resource or not
   *
   * @complexity Time: O(1) for standard resources (memory, vcores) + O(r) where
   *             r=number of custom resource types if custom resources enabled.
   *             Each gauge/counter update is O(1) atomic operation.
   *             Space: O(1) - no allocations, only counter/gauge updates.
   *             Source: QueueMetrics.java:861-876
   *
   * @implNote Updates both instantaneous gauges (allocatedContainers, allocatedMB,
   *           allocatedVCores) for current state and cumulative counter
   *           (aggregateContainersAllocated) for total historical count.
   */
  private void computeAllocateResources(int containers, Resource res,
      boolean decrPending) {
    // @PerformanceCritical: Multiple O(1) atomic counter/gauge operations
    allocatedContainers.incr(containers);
    aggregateContainersAllocated.incr(containers);
    allocatedMB.incr(res.getMemorySize() * containers);
    allocatedVCores.incr(res.getVirtualCores() * containers);
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.increaseAllocated(res, containers);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getAllocatedValues(), this.registry,
          ALLOCATED_RESOURCE_METRIC_PREFIX, ALLOCATED_RESOURCE_METRIC_DESC);
    }
    if (decrPending) {
      decrementPendingResources(containers, res);
    }
  }

  /**
   * Allocate Resource for container size change (increase/decrease).
   * Updates both allocated and pending resource gauges atomically and
   * propagates to user metrics and parent queue.
   *
   * @param partition Node Partition
   * @param user Name of the user
   * @param res Resource delta (may be positive for increase, negative for decrease)
   *
   * @complexity Time: O(d + r) where d=queue hierarchy depth for recursive parent
   *             propagation, r=number of custom resource types to update.
   *             Each gauge operation (incr/decr) is O(1) atomic.
   *             Space: O(d) stack depth for recursive calls.
   *             Source: QueueMetrics.java:884-910
   *
   * @implNote Used for container resize operations where container count stays same
   *           but resource allocation changes. Simultaneously updates allocated (increase)
   *           and pending (decrease) to maintain resource accounting consistency.
   */
  public void allocateResources(String partition, String user, Resource res) {
    // @PerformanceCritical: Gauge updates - O(1) atomic operations each
    allocatedMB.incr(res.getMemorySize());
    allocatedVCores.incr(res.getVirtualCores());
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.increaseAllocated(res);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getAllocatedValues(), this.registry,
          ALLOCATED_RESOURCE_METRIC_PREFIX, ALLOCATED_RESOURCE_METRIC_DESC);
    }

    pendingMB.decr(res.getMemorySize());
    pendingVCores.decr(res.getVirtualCores());
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.decreasePending(res);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getPendingValues(), this.registry,
          PENDING_RESOURCE_METRIC_PREFIX, PENDING_RESOURCE_METRIC_DESC);
    }

    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.allocateResources(partition, user, res);
    }
    if (parent != null) {
      parent.allocateResources(partition, user, res);
    }
  }

  /**
   * Records resource release when containers complete or are killed.
   * Decrements allocated gauges and increments release counters.
   * Propagates through queue hierarchy and partition-specific metrics.
   *
   * @param partition Node Partition
   * @param user Name of the user
   * @param containers number of containers released
   * @param res resource per container being released
   *
   * @complexity Time: O(d + p + r) where d=queue hierarchy depth for parent propagation,
   *             p=partition metrics operations, r=number of custom resource types.
   *             Each gauge decrement is O(1) atomic via MutableGaugeLong.decr().
   *             Space: O(1) for counter operations; no additional allocations.
   *             Source: QueueMetrics.java:912-928
   *
   * @implNote Release updates both instantaneous gauges (allocatedContainers decremented)
   *           and cumulative counters (aggregateContainersReleased incremented) to
   *           provide both current state and historical release counts.
   */
  public void releaseResources(String partition, String user, int containers,
      Resource res) {

    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      // @PerformanceCritical: Container release path - O(d) hierarchy updates
      internalReleaseResources(partition, user, containers, res);
    }

    QueueMetrics partitionQueueMetrics = getPartitionQueueMetrics(partition);
    if (partitionQueueMetrics != null) {
      partitionQueueMetrics.internalReleaseResources(partition, user,
          containers, res);
      QueueMetrics partitionMetrics = getPartitionMetrics(partition);
      if (partitionMetrics != null) {
        partitionMetrics.computeReleaseResources(containers, res);
      }
    }
  }

  /**
   * Internal release propagation through queue hierarchy. Updates this queue's
   * release metrics and recursively propagates to user metrics and parent queue.
   *
   * @param partition Node Partition
   * @param user Name of the user
   * @param containers number of containers released
   * @param res resource per container
   *
   * @complexity Time: O(d) where d=queue hierarchy depth due to recursive parent
   *             calls. computeReleaseResources is O(r) where r=custom resource count.
   *             Space: O(d) stack depth for recursion.
   *             Source: QueueMetrics.java:930-941
   */
  public void internalReleaseResources(String partition, String user,
      int containers, Resource res) {

    computeReleaseResources(containers, res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.internalReleaseResources(partition, user, containers, res);
    }
    if (parent != null) {
      parent.internalReleaseResources(partition, user, containers, res);
    }
  }

  /**
   * Computes and updates release metrics for containers with support for
   * resource vectors including custom resources.
   *
   * @param containers number of containers
   * @param res resource containing memory size, vcores etc
   *
   * @complexity Time: O(1) for standard resources (memory, vcores) + O(r) where
   *             r=number of custom resource types if custom resources enabled.
   *             Each gauge decrement and counter increment is O(1) atomic.
   *             Space: O(1) - no allocations, only counter/gauge updates.
   *             Source: QueueMetrics.java:949-960
   *
   * @implNote Decrements instantaneous gauges (allocatedContainers, allocatedMB,
   *           allocatedVCores) while incrementing cumulative counter
   *           (aggregateContainersReleased) for historical release tracking.
   */
  private void computeReleaseResources(int containers, Resource res) {
    // @PerformanceCritical: Multiple O(1) atomic gauge/counter operations
    allocatedContainers.decr(containers);
    aggregateContainersReleased.incr(containers);
    allocatedMB.decr(res.getMemorySize() * containers);
    allocatedVCores.decr(res.getVirtualCores() * containers);
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.decreaseAllocated(res, containers);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getAllocatedValues(), this.registry,
          ALLOCATED_RESOURCE_METRIC_PREFIX, ALLOCATED_RESOURCE_METRIC_DESC);
    }
  }

  public void preemptContainer() {
    aggregateContainersPreempted.incr();
    if (parent != null) {
      parent.preemptContainer();
    }
  }

  public void updatePreemptedMemoryMBSeconds(long mbSeconds) {
    aggregateMemoryMBSecondsPreempted.incr(mbSeconds);
    if (parent != null) {
      parent.updatePreemptedMemoryMBSeconds(mbSeconds);
    }
  }

  public void updatePreemptedVcoreSeconds(long vcoreSeconds) {
    aggregateVcoreSecondsPreempted.incr(vcoreSeconds);
    if (parent != null) {
      parent.updatePreemptedVcoreSeconds(vcoreSeconds);
    }
  }

  public void updatePreemptedResources(Resource res) {
    aggregateMemoryMBPreempted.incr(res.getMemorySize());
    aggregateVcoresPreempted.incr(res.getVirtualCores());
    if (parent != null) {
      parent.updatePreemptedResources(res);
    }
  }

  public void updatePreemptedForCustomResources(Resource res) {
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.increaseAggregatedPreempted(res);
    }
    if (parent != null) {
      parent.updatePreemptedForCustomResources(res);
    }
  }

  public void updatePreemptedSecondsForCustomResources(Resource res,
          long seconds) {
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources
          .increaseAggregatedPreemptedSeconds(res, seconds);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getAggregatePreemptedSeconds()
              .getValues(), this.registry,
          AGGREGATE_PREEMPTED_SECONDS_METRIC_PREFIX,
          AGGREGATE_PREEMPTED_SECONDS_METRIC_DESC);
    }
    if (parent != null) {
      parent.updatePreemptedSecondsForCustomResources(res, seconds);
    }
  }

  public void reserveResource(String partition, String user, Resource res) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      internalReserveResources(partition, user, res);
    }
    QueueMetrics partitionQueueMetrics = getPartitionQueueMetrics(partition);
    if (partitionQueueMetrics != null) {
      partitionQueueMetrics.internalReserveResources(partition, user, res);
      QueueMetrics partitionMetrics = getPartitionMetrics(partition);
      if (partitionMetrics != null) {
        partitionMetrics.incrReserveResources(res);
      }
    }
  }

  protected void internalReserveResources(String partition, String user,
      Resource res) {
    incrReserveResources(res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.internalReserveResources(partition, user, res);
    }
    if (parent != null) {
      parent.internalReserveResources(partition, user, res);
    }
  }

  public void incrReserveResources(Resource res) {
    reservedContainers.incr();
    reservedMB.incr(res.getMemorySize());
    reservedVCores.incr(res.getVirtualCores());
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.increaseReserved(res);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getReservedValues(), this.registry,
          RESERVED_RESOURCE_METRIC_PREFIX, RESERVED_RESOURCE_METRIC_DESC);
    }
  }

  public void unreserveResource(String partition, String user, Resource res) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      internalUnReserveResources(partition, user, res);
    }
    QueueMetrics partitionQueueMetrics = getPartitionQueueMetrics(partition);
    if (partitionQueueMetrics != null) {
      partitionQueueMetrics.internalUnReserveResources(partition, user, res);
      QueueMetrics partitionMetrics = getPartitionMetrics(partition);
      if (partitionMetrics != null) {
        partitionMetrics.decrReserveResource(res);
      }
    }
  }

  protected void internalUnReserveResources(String partition, String user,
      Resource res) {
    decrReserveResource(res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.internalUnReserveResources(partition, user, res);
    }
    if (parent != null) {
      parent.internalUnReserveResources(partition, user, res);
    }
  }

  public void decrReserveResource(Resource res) {
    int containers = 1;
    reservedContainers.decr(containers);
    reservedMB.decr(res.getMemorySize());
    reservedVCores.decr(res.getVirtualCores());
    if (queueMetricsForCustomResources != null) {
      queueMetricsForCustomResources.decreaseReserved(res);
      queueMetricsForCustomResources.registerCustomResources(
          queueMetricsForCustomResources.getReservedValues(), this.registry,
          RESERVED_RESOURCE_METRIC_PREFIX, RESERVED_RESOURCE_METRIC_DESC);
    }
  }

  public void incrActiveUsers() {
    activeUsers.incr();
  }
  
  public void decrActiveUsers() {
    activeUsers.decr();
  }
  
  public void activateApp(String user) {
    activeApplications.incr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.activateApp(user);
    }
    if (parent != null) {
      parent.activateApp(user);
    }
  }
  
  public void deactivateApp(String user) {
    activeApplications.decr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.deactivateApp(user);
    }
    if (parent != null) {
      parent.deactivateApp(user);
    }
  }

  public void addAppAttemptFirstContainerAllocationDelay(long latency) {
    appAttemptFirstContainerAllocationDelay.add(latency);
  }

  public int getAppsSubmitted() {
    return appsSubmitted.value();
  }

  public int getUnmanagedAppsSubmitted() {
    return unmanagedAppsSubmitted.value();
  }

  public int getAppsRunning() {
    return appsRunning.value();
  }

  public int getUnmanagedAppsRunning() {
    return unmanagedAppsRunning.value();
  }

  public int getAppsPending() {
    return appsPending.value();
  }

  public int getUnmanagedAppsPending() {
    return unmanagedAppsPending.value();
  }

  public int getAppsCompleted() {
    return appsCompleted.value();
  }

  public int getUnmanagedAppsCompleted() {
    return unmanagedAppsCompleted.value();
  }

  public int getAppsKilled() {
    return appsKilled.value();
  }

  public int getAppsFailed() {
    return appsFailed.value();
  }

  public int getUnmanagedAppsFailed() {
    return unmanagedAppsFailed.value();
  }

  public Resource getAllocatedResources() {
    if (queueMetricsForCustomResources != null) {
      return Resource.newInstance(allocatedMB.value(), allocatedVCores.value(),
              queueMetricsForCustomResources.getAllocatedValues());
    }
    return Resource.newInstance(allocatedMB.value(),
            allocatedVCores.value());
  }

  public Resource getAvailableResources() {
    if (queueMetricsForCustomResources != null) {
      return Resource.newInstance(availableMB.value(), availableVCores.value(),
          queueMetricsForCustomResources.getAvailableValues());
    }
    return Resource.newInstance(availableMB.value(), availableVCores.value());
  }

  public Resource getPendingResources() {
    if (queueMetricsForCustomResources != null) {
      return Resource.newInstance(pendingMB.value(), pendingVCores.value(),
          queueMetricsForCustomResources.getPendingValues());
    }
    return Resource.newInstance(pendingMB.value(), pendingVCores.value());
  }

  public Resource getReservedResources() {
    if (queueMetricsForCustomResources != null) {
      return Resource.newInstance(reservedMB.value(), reservedVCores.value(),
          queueMetricsForCustomResources.getReservedValues());
    }
    return Resource.newInstance(reservedMB.value(), reservedVCores.value());
  }

  /**
   * Handle this specially as this has a long value and it could be
   * truncated when casted into an int parameter of
   * Resource.newInstance (vCores).
   * @return QueueMetricsCustomResource
   */
  @VisibleForTesting
  public CustomResourceMetricValue getAggregatedPreemptedSecondsResources() {
    return queueMetricsForCustomResources.getAggregatePreemptedSeconds();
  }

  @VisibleForTesting
  public MutableCounterLong getAggregateMemoryMBSecondsPreempted() {
    return aggregateMemoryMBSecondsPreempted;
  }

  @VisibleForTesting
  public MutableCounterLong getAggregateVcoreSecondsPreempted() {
    return aggregateVcoreSecondsPreempted;
  }

  @VisibleForTesting
  public long getAggregateMemoryMBPreempted() {
    return aggregateMemoryMBPreempted.value();
  }

  @VisibleForTesting
  public long getAggregateVcoresPreempted() {
    return aggregateVcoresPreempted.value();
  }

  public long getAllocatedMB() {
    return allocatedMB.value();
  }
  
  public int getAllocatedVirtualCores() {
    return allocatedVCores.value();
  }

  public int getAllocatedContainers() {
    return allocatedContainers.value();
  }

  public long getAvailableMB() {
    return availableMB.value();
  }  
  
  public int getAvailableVirtualCores() {
    return availableVCores.value();
  }

  public long getPendingMB() {
    return pendingMB.value();
  }
  
  public int getPendingVirtualCores() {
    return pendingVCores.value();
  }

  public int getPendingContainers() {
    return pendingContainers.value();
  }
  
  public long getReservedMB() {
    return reservedMB.value();
  }
  
  public int getReservedVirtualCores() {
    return reservedVCores.value();
  }

  public int getReservedContainers() {
    return reservedContainers.value();
  }
  
  public int getActiveUsers() {
    return activeUsers.value();
  }
  
  public int getActiveApps() {
    return activeApplications.value();
  }
  
  public MetricsSystem getMetricsSystem() {
    return metricsSystem;
  }

  public long getAggregateAllocatedContainers() {
    return aggregateContainersAllocated.value();
  }

  public long getAggregateNodeLocalContainersAllocated() {
    return aggregateNodeLocalContainersAllocated.value();
  }

  public long getAggregateRackLocalContainersAllocated() {
    return aggregateRackLocalContainersAllocated.value();
  }

  public long getAggregateOffSwitchContainersAllocated() {
    return aggregateOffSwitchContainersAllocated.value();
  }

  public long getAggegatedReleasedContainers() {
    return aggregateContainersReleased.value();
  }

  public long getAggregatePreemptedContainers() {
    return aggregateContainersPreempted.value();
  }

  /**
   * Fills in Resource values from available metrics values of custom resources
   * to @code{targetResource}, only if the corresponding
   * value of @code{targetResource} is zero.
   * If @code{fromResource} has a value less than the available metrics value
   * for a particular resource, it will be set to the @code{targetResource}
   * instead.
   *
   * @param fromResource The resource to compare available resource values with.
   * @param targetResource The resource to save the values into.
   */
  public void fillInValuesFromAvailableResources(Resource fromResource,
      Resource targetResource) {
    if (queueMetricsForCustomResources != null) {
      CustomResourceMetricValue availableResources =
          queueMetricsForCustomResources.getAvailable();

      // We expect all custom resources contained in availableResources,
      // so we will loop through all of them.
      for (Map.Entry<String, Long> availableEntry : availableResources
          .getValues().entrySet()) {
        String resourceName = availableEntry.getKey();

        // We only update the value if fairshare is 0 for that resource.
        if (targetResource.getResourceValue(resourceName) == 0) {
          Long availableValue = availableEntry.getValue();
          long value = Math.min(availableValue,
              fromResource.getResourceValue(resourceName));
          targetResource.setResourceValue(resourceName, value);
        }
      }
    }
  }

  @VisibleForTesting
  public QueueMetricsForCustomResources getQueueMetricsForCustomResources() {
    return this.queueMetricsForCustomResources;
  }

  protected void setQueueMetricsForCustomResources(
      QueueMetricsForCustomResources metrics) {
    this.queueMetricsForCustomResources = metrics;
  }

  public void setParent(QueueMetrics parent) {
    this.parent = parent;
  }

  public Queue getParentQueue() {
    return parentQueue;
  }

  protected void registerPartitionMetricsCreation(String metricName) {
    if (storedPartitionMetrics != null) {
      storedPartitionMetrics.add(metricName);
    }
  }

  public void setParentQueue(Queue parentQueue) {
    this.parentQueue = parentQueue;

    if (storedPartitionMetrics == null) {
      return;
    }

    for (String partitionMetric : storedPartitionMetrics) {
      QueueMetrics metric = getQueueMetrics().get(partitionMetric);

      if (metric != null && metric.parentQueue != null) {
        metric.parentQueue = parentQueue;
      }
    }
  }
}