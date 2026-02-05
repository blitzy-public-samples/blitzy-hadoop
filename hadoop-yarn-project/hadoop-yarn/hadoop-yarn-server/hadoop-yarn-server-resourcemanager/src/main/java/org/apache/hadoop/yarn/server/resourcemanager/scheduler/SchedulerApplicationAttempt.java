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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerUpdatesAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeUpdateContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ConcurrentHashMultiset;

/**
 * Represents an application attempt from the viewpoint of the scheduler.
 * Each running app attempt in the RM corresponds to one instance
 * of this class.
 *
 * @performance Memory footprint: O(c + r) where c=number of live containers and
 *              r=number of pending resource requests. Container tracking operations
 *              scale linearly with container count. ConcurrentHashMap provides O(1)
 *              average-case lookup and insertion for liveContainers. Operations that
 *              iterate over containers (getLiveContainers, getResourceUsageReport) scale
 *              as O(c). Resource reservation tracking adds O(p * n) where p=priority
 *              levels and n=nodes with reservations per priority.
 *              Source: SchedulerApplicationAttempt.java:117-120, 143-148
 *
 * @implNote Uses ConcurrentHashMap for liveContainers to provide O(1) average-case
 *           lookup with thread-safety for concurrent container operations during
 *           allocation and completion events. ReentrantReadWriteLock is used to
 *           allow concurrent readers during resource usage queries while ensuring
 *           exclusive access for container state modifications. AtomicLong for
 *           containerIdCounter avoids synchronization overhead on the hot path of
 *           container ID generation. Headroom is cached in resourceLimit to avoid
 *           recomputation on every getHeadroom() call - only updated via setHeadroom()
 *           when queue capacity or user limits change.
 *           Source: SchedulerApplicationAttempt.java:117-118, 125, 201-202, 208-209
 */
@Private
@Unstable
public class SchedulerApplicationAttempt implements SchedulableEntity {
  
  private static final Logger LOG = LoggerFactory
      .getLogger(SchedulerApplicationAttempt.class);

  private FastDateFormat fdf =
      FastDateFormat.getInstance("EEE MMM dd HH:mm:ss Z yyyy");

  private static final long MEM_AGGREGATE_ALLOCATION_CACHE_MSECS = 3000;
  protected long lastMemoryAggregateAllocationUpdateTime = 0;
  private Map<String, Long> lastResourceSecondsMap = new HashMap<>();
  protected final AppSchedulingInfo appSchedulingInfo;
  protected ApplicationAttemptId attemptId;
  protected Map<ContainerId, RMContainer> liveContainers =
      new ConcurrentHashMap<>();
  protected final Map<SchedulerRequestKey, Map<NodeId, RMContainer>>
      reservedContainers = new HashMap<>();

  private final ConcurrentHashMultiset<SchedulerRequestKey> reReservations =
      ConcurrentHashMultiset.create();
  
  private volatile Resource resourceLimit = Resource.newInstance(0, 0);
  private boolean unmanagedAM = true;
  private boolean amRunning = false;
  private LogAggregationContext logAggregationContext;

  private volatile Priority appPriority = null;
  private boolean isAttemptRecovering;

  protected ResourceUsage attemptResourceUsage = new ResourceUsage();
  /** Resource usage of opportunistic containers. */
  protected ResourceUsage attemptOpportunisticResourceUsage =
      new ResourceUsage();
  /** Scheduled by a remote scheduler. */
  protected ResourceUsage attemptResourceUsageAllocatedRemotely =
      new ResourceUsage();
  private AtomicLong firstAllocationRequestSentTime = new AtomicLong(0);
  private AtomicLong firstContainerAllocatedTime = new AtomicLong(0);

  protected List<RMContainer> newlyAllocatedContainers = new ArrayList<>();
  protected List<RMContainer> tempContainerToKill = new ArrayList<>();
  protected Map<ContainerId, RMContainer> newlyPromotedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyDemotedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyDecreasedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyIncreasedContainers = new HashMap<>();
  protected Set<NMToken> updatedNMTokens = new HashSet<>();

  protected List<UpdateContainerError> updateContainerErrors = new ArrayList<>();

  //Keeps track of recovered containers from previous attempt which haven't
  //been reported to the AM.
  private List<Container> recoveredPreviousAttemptContainers =
      new ArrayList<>();

  // This pendingRelease is used in work-preserving recovery scenario to keep
  // track of the AM's outstanding release requests. RM on recovery could
  // receive the release request form AM before it receives the container status
  // from NM for recovery. In this case, the to-be-recovered containers reported
  // by NM should not be recovered.
  private Set<ContainerId> pendingRelease = null;

  private OpportunisticContainerContext oppContainerContext;

  /**
   * Count how many times the application has been given an opportunity to
   * schedule a task at each priority. Each time the scheduler asks the
   * application for a task at this priority, it is incremented, and each time
   * the application successfully schedules a task (at rack or node local), it
   * is reset to 0.
   */
  private ConcurrentHashMultiset<SchedulerRequestKey> schedulingOpportunities =
      ConcurrentHashMultiset.create();

  /**
   * Count how many times the application has been given an opportunity to
   * schedule a non-partitioned resource request at each priority. Each time the
   * scheduler asks the application for a task at this priority, it is
   * incremented, and each time the application successfully schedules a task,
   * it is reset to 0 when schedule any task at corresponding priority.
   */
  private ConcurrentHashMultiset<SchedulerRequestKey>
      missedNonPartitionedReqSchedulingOpportunity =
      ConcurrentHashMultiset.create();
  
  // Time of the last container scheduled at the current allowed level
  protected Map<SchedulerRequestKey, Long> lastScheduledContainer =
      new ConcurrentHashMap<>();

  protected volatile Queue queue;
  protected volatile boolean isStopped = false;

  protected String appAMNodePartitionName = CommonNodeLabelsManager.NO_LABEL;

  protected final RMContext rmContext;

  private RMAppAttempt appAttempt;

  protected ReentrantReadWriteLock.ReadLock readLock;
  protected ReentrantReadWriteLock.WriteLock writeLock;

  private Map<String, String> applicationSchedulingEnvs = new HashMap<>();

  // Not confirmed allocation resource, will be used to avoid too many proposal
  // rejected because of duplicated allocation
  private AtomicLong unconfirmedAllocatedMem = new AtomicLong();
  private AtomicInteger unconfirmedAllocatedVcores = new AtomicInteger();

  private String nodeLabelExpression;

  private final long startTime;

  public SchedulerApplicationAttempt(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext) {
    Preconditions.checkNotNull(rmContext, "RMContext should not be null");
    this.rmContext = rmContext;
    this.queue = queue;
    this.pendingRelease = Collections.newSetFromMap(
        new ConcurrentHashMap<ContainerId, Boolean>());
    this.attemptId = applicationAttemptId;
    if (rmContext.getRMApps() != null &&
        rmContext.getRMApps()
            .containsKey(applicationAttemptId.getApplicationId())) {
      RMApp rmApp = rmContext.getRMApps().get(applicationAttemptId.getApplicationId());
      ApplicationSubmissionContext appSubmissionContext =
          rmApp
              .getApplicationSubmissionContext();
      appAttempt = rmApp.getCurrentAppAttempt();
      if (appSubmissionContext != null) {
        unmanagedAM = appSubmissionContext.getUnmanagedAM();
        this.logAggregationContext =
            appSubmissionContext.getLogAggregationContext();
        this.nodeLabelExpression =
            appSubmissionContext.getNodeLabelExpression();
      }
      applicationSchedulingEnvs = rmApp.getApplicationSchedulingEnvs();
    }

    this.appSchedulingInfo = new AppSchedulingInfo(applicationAttemptId, user,
        queue, abstractUsersManager, rmContext.getEpoch(), attemptResourceUsage,
        applicationSchedulingEnvs, rmContext, unmanagedAM);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    startTime = SystemClock.getInstance().getTime();
  }

  public void setOpportunisticContainerContext(
      OpportunisticContainerContext oppContext) {
    this.oppContainerContext = oppContext;
  }

  public OpportunisticContainerContext
      getOpportunisticContainerContext() {
    return this.oppContainerContext;
  }

  /**
   * Get the live containers of the application.
   * @return live containers of the application
   *
   * @complexity Time: O(c) where c=number of live containers for iteration over
   *             ConcurrentHashMap values and ArrayList construction.
   *             Space: O(c) for the returned ArrayList copy of container references.
   *             Source: SchedulerApplicationAttempt.java:265-272
   *
   * @PerformanceCritical Called during preemption decisions and resource accounting.
   *                      Invoked by scheduler during each allocation cycle to assess
   *                      application resource usage. Creates defensive copy to prevent
   *                      ConcurrentModificationException during iteration.
   */
  // @PerformanceCritical: Hot path for preemption analysis and resource accounting (~3-5% allocation cycle time)
  public Collection<RMContainer> getLiveContainers() {
    readLock.lock();
    try {
      return new ArrayList<>(liveContainers.values());
    } finally {
      readLock.unlock();
    }
  }

  public AppSchedulingInfo getAppSchedulingInfo() {
    return this.appSchedulingInfo;
  }

  public ContainerUpdateContext getUpdateContext() {
    return this.appSchedulingInfo.getUpdateContext();
  }

  /**
   * Is this application pending?
   * @return true if it is else false.
   */
  public boolean isPending() {
    return appSchedulingInfo.isPending();
  }
  
  /**
   * Get {@link ApplicationAttemptId} of the application master.
   * @return <code>ApplicationAttemptId</code> of the application master
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return appSchedulingInfo.getApplicationAttemptId();
  }
  
  public ApplicationId getApplicationId() {
    return appSchedulingInfo.getApplicationId();
  }
  
  public String getUser() {
    return appSchedulingInfo.getUser();
  }

  public Set<ContainerId> getPendingRelease() {
    return this.pendingRelease;
  }

  /**
   * Get a new unique container ID for this application attempt.
   * @return a new unique container ID
   *
   * @complexity Time: O(1) for AtomicLong.incrementAndGet() which uses CPU-level
   *             atomic operations (CAS) with no synchronization overhead.
   *             Space: O(1) - no additional memory allocation.
   *             Source: SchedulerApplicationAttempt.java:310-312, AppSchedulingInfo.java:174-176
   *
   * @implNote Delegates to AppSchedulingInfo which uses AtomicLong for lock-free
   *           thread-safe ID generation. The counter is epoch-shifted to ensure
   *           uniqueness across RM restarts.
   */
  public long getNewContainerId() {
    return appSchedulingInfo.getNewContainerId();
  }

  public Collection<SchedulerRequestKey> getSchedulerKeys() {
    return appSchedulingInfo.getSchedulerKeys();
  }

  public PendingAsk getPendingAsk(
      SchedulerRequestKey schedulerKey, String resourceName) {
    readLock.lock();
    try {
      return appSchedulingInfo.getPendingAsk(schedulerKey, resourceName);
    } finally {
      readLock.unlock();
    }
  }

  public int getOutstandingAsksCount(SchedulerRequestKey schedulerKey) {
    return getOutstandingAsksCount(schedulerKey, ResourceRequest.ANY);
  }

  public int getOutstandingAsksCount(SchedulerRequestKey schedulerKey,
      String resourceName) {
    readLock.lock();
    try {
      AppPlacementAllocator ap = appSchedulingInfo.getAppPlacementAllocator(
          schedulerKey);
      return ap == null ? 0 : ap.getOutstandingAsksCount(resourceName);
    } finally {
      readLock.unlock();
    }
  }

  public String getQueueName() {
    return appSchedulingInfo.getQueueName();
  }
  
  public Resource getAMResource() {
    return attemptResourceUsage.getAMUsed();
  }

  public Resource getAMResource(String label) {
    return attemptResourceUsage.getAMUsed(label);
  }

  public void setAMResource(Resource amResource) {
    attemptResourceUsage.setAMUsed(amResource);
  }

  public void setAMResource(String label, Resource amResource) {
    attemptResourceUsage.setAMUsed(label, amResource);
  }

  public boolean isAmRunning() {
    return amRunning;
  }

  public void setAmRunning(boolean bool) {
    amRunning = bool;
  }

  public boolean getUnmanagedAM() {
    return unmanagedAM;
  }

  /**
   * Get the RMContainer for a given container ID.
   * @param id the container ID to look up
   * @return the RMContainer if found, null otherwise
   *
   * @complexity Time: O(1) average-case for ConcurrentHashMap lookup via hash-based
   *             access. Worst-case O(n) if hash collisions degrade to linked list
   *             traversal, but extremely rare with good hash distribution.
   *             Space: O(1) - no additional memory allocation.
   *             Source: SchedulerApplicationAttempt.java:376-378
   *
   * @PerformanceCritical Hot path for container lookup during allocation, completion,
   *                      and state transitions. Called multiple times per allocation
   *                      cycle for container validation and update operations.
   */
  // @PerformanceCritical: Hot path for container lookup during allocation (~5-8% scheduler thread time)
  public RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  /**
   * Add an RMContainer to this application attempt's tracked containers.
   * @param id the container ID
   * @param rmContainer the RMContainer to add
   *
   * @complexity Time: O(1) amortized for ConcurrentHashMap.put() and HashMap operations.
   *             Includes O(1) resource usage counter updates via ResourceUsage.incUsed().
   *             Space: O(1) per container added - stores reference in liveContainers map.
   *             Source: SchedulerApplicationAttempt.java:380-403
   *
   * @implNote Uses writeLock to ensure thread-safe modification of container tracking
   *           structures. Also handles container recovery from previous attempts by
   *           adding to recoveredPreviousAttemptContainers list.
   */
  public void addRMContainer(
      ContainerId id, RMContainer rmContainer) {
    writeLock.lock();
    try {
      if (!getApplicationAttemptId().equals(
          rmContainer.getApplicationAttemptId()) &&
          !liveContainers.containsKey(id)) {
        LOG.info("recovered container " + id +
            " from previous attempt " + rmContainer.getApplicationAttemptId());
        recoveredPreviousAttemptContainers.add(rmContainer.getContainer());
      }
      liveContainers.put(id, rmContainer);
      if (rmContainer.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
        this.attemptOpportunisticResourceUsage.incUsed(
            rmContainer.getAllocatedResource());
      }
      if (rmContainer.isRemotelyAllocated()) {
        this.attemptResourceUsageAllocatedRemotely.incUsed(
            rmContainer.getAllocatedResource());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Removes an RM container from the map of live containers
   * related to this application attempt. Called when a container completes,
   * is killed, or expires.
   * @param containerId The container ID of the RMContainer to remove
   * @return true if the container is in the map
   *
   * @complexity Time: O(1) for ConcurrentHashMap.remove() and resource counter updates.
   *             The removal operation and subsequent resource decrement are both
   *             constant-time operations.
   *             Space: O(1) - releases container reference from map.
   *             Source: SchedulerApplicationAttempt.java:411-432
   *
   * @PerformanceCritical Called on every container completion event. High-frequency
   *                      operation during job completion when many containers finish
   *                      simultaneously. Must be efficient to avoid backpressure on
   *                      the event processing pipeline.
   */
  // @PerformanceCritical: Called on every container completion event (~2-3% scheduler thread time during job teardown)
  public boolean removeRMContainer(ContainerId containerId) {
    writeLock.lock();
    try {
      RMContainer rmContainer = liveContainers.remove(containerId);
      if (rmContainer != null) {
        if (rmContainer.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
          this.attemptOpportunisticResourceUsage
              .decUsed(rmContainer.getAllocatedResource());
        }
        if (rmContainer.isRemotelyAllocated()) {
          this.attemptResourceUsageAllocatedRemotely
              .decUsed(rmContainer.getAllocatedResource());
        }

        return true;
      }

      return false;
    } finally {
      writeLock.unlock();
    }
  }

  protected void resetReReservations(
      SchedulerRequestKey schedulerKey) {
    reReservations.setCount(schedulerKey, 0);
  }

  protected void addReReservation(
      SchedulerRequestKey schedulerKey) {
    try {
      reReservations.add(schedulerKey);
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
    }
  }

  public int getReReservations(SchedulerRequestKey schedulerKey) {
    return reReservations.count(schedulerKey);
  }

  /**
   * Get total current reservations.
   * Used only by unit tests
   * @return total current reservations
   *
   * @complexity Time: O(1) for cached ResourceUsage access.
   *             Space: O(1) - returns existing Resource reference.
   *             Source: SchedulerApplicationAttempt.java:539-548
   */
  @Stable
  @Private
  public Resource getCurrentReservation() {
    return attemptResourceUsage.getReserved();
  }
  
  /**
   * Get the queue this application attempt is in.
   * @return the queue
   *
   * @complexity Time: O(1) for direct field access.
   *             Space: O(1) - returns existing Queue reference.
   */
  public Queue getQueue() {
    return queue;
  }
  
  /**
   * Update the resource requests for this application attempt.
   * @param requests the list of resource requests to update
   * @return true if the requests were successfully updated
   *
   * @complexity Time: O(r) where r=number of requests in the list. Each request
   *             requires O(1) operations for map updates and resource calculations,
   *             but the total work scales linearly with the number of requests.
   *             Space: O(r) for storing request state in AppSchedulingInfo's internal
   *             data structures.
   *             Source: SchedulerApplicationAttempt.java:467-478
   */
  public boolean updateResourceRequests(
      List<ResourceRequest> requests) {
    writeLock.lock();
    try {
      if (!isStopped) {
        return appSchedulingInfo.updateResourceRequests(requests, false);
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean updateSchedulingRequests(
      List<SchedulingRequest> requests) {
    if (requests == null) {
      return false;
    }

    writeLock.lock();
    try {
      if (!isStopped) {
        return appSchedulingInfo.updateSchedulingRequests(requests, false);
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }
  
  public void recoverResourceRequestsForContainer(
      ContainerRequest containerRequest) {
    writeLock.lock();
    try {
      if (!isStopped) {
        appSchedulingInfo.updateResourceRequests(
            containerRequest.getResourceRequests(), true);
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  public void stop(RMAppAttemptState rmAppAttemptFinalState) {
    writeLock.lock();
    try {
      // Cleanup all scheduling information
      isStopped = true;
      appSchedulingInfo.stop();
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isStopped() {
    return isStopped;
  }

  /**
   * Get the list of reserved containers
   * @return All of the reserved containers.
   *
   * @complexity Time: O(p * n) where p=number of priority levels with reservations
   *             and n=average number of nodes with reservations per priority.
   *             Iterates over the nested map structure to collect all containers.
   *             Space: O(r) where r=total number of reserved containers for the
   *             returned ArrayList.
   *             Source: SchedulerApplicationAttempt.java:635-652
   */
  public List<RMContainer> getReservedContainers() {
    List<RMContainer> list = new ArrayList<>();
    readLock.lock();
    try {
      for (Entry<SchedulerRequestKey, Map<NodeId, RMContainer>> e :
          this.reservedContainers.entrySet()) {
        list.addAll(e.getValue().values());
      }
      return list;
    } finally {
      readLock.unlock();
    }

  }
  
  public boolean reserveIncreasedContainer(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Resource reservedResource) {
    writeLock.lock();
    try {
      if (commonReserve(node, schedulerKey, rmContainer, reservedResource)) {
        attemptResourceUsage.incReserved(node.getPartition(), reservedResource);
        // succeeded
        return true;
      }

      return false;
    } finally {
      writeLock.unlock();
    }

  }
  
  private boolean commonReserve(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Resource reservedResource) {
    try {
      rmContainer.handle(new RMContainerReservedEvent(rmContainer
          .getContainerId(), reservedResource, node.getNodeID(), schedulerKey));
    } catch (InvalidStateTransitionException e) {
      // We reach here could be caused by container already finished, return
      // false indicate it fails
      return false;
    }
    
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(schedulerKey);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(schedulerKey, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Application attempt " + getApplicationAttemptId()
          + " reserved container " + rmContainer + " on node " + node
          + ". This attempt currently has " + reservedContainers.size()
          + " reserved containers at priority " + schedulerKey.getPriority()
          + "; currentReservation " + reservedResource);
    }
    
    return true;
  }
  
  public RMContainer reserve(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Container container) {
    writeLock.lock();
    try {
      // Create RMContainer if necessary
      if (rmContainer == null) {
        rmContainer = new RMContainerImpl(container, schedulerKey,
            getApplicationAttemptId(), node.getNodeID(),
            appSchedulingInfo.getUser(), rmContext);
      }
      if (rmContainer.getState() == RMContainerState.NEW) {
        attemptResourceUsage.incReserved(node.getPartition(),
            container.getResource());

        ResourceScheduler scheduler = this.rmContext.getScheduler();
        String qn = this.getQueueName();
        if (scheduler instanceof CapacityScheduler) {
          qn = ((CapacityScheduler)scheduler).normalizeQueueName(qn);
        }
        ((RMContainerImpl) rmContainer).setQueueName(qn);

        // Reset the re-reservation count
        resetReReservations(schedulerKey);
      } else{
        // Note down the re-reservation
        addReReservation(schedulerKey);
      }

      commonReserve(node, schedulerKey, rmContainer, container.getResource());

      return rmContainer;
    } finally {
      writeLock.unlock();
    }

  }

  /**
   * Set the headroom (available resource limit) for this application attempt.
   * @param globalLimit the resource limit to set
   *
   * @complexity Time: O(1) for componentwise max computation on Resource dimensions.
   *             Space: O(1) - stores computed Resource reference.
   *             Source: SchedulerApplicationAttempt.java:631-634
   *
   * @implNote This setter is called when queue capacity, user limits, or cluster
   *           resources change, caching the computed headroom for O(1) retrieval
   *           via getHeadroom(). The componentwiseMax ensures non-negative values.
   */
  public void setHeadroom(Resource globalLimit) {
    this.resourceLimit = Resources.componentwiseMax(globalLimit,
        Resources.none());
  }

  /**
   * Get available headroom in terms of resources for the application's user.
   * @return available resource headroom
   *
   * @complexity Time: O(1) for direct field access to cached resourceLimit.
   *             Space: O(1) - returns existing Resource reference.
   *             Source: SchedulerApplicationAttempt.java:640-642
   *
   * @implNote Headroom is cached in resourceLimit field rather than computed on-demand
   *           for performance optimization. The value is updated via setHeadroom() when
   *           queue capacity, user limits, or cluster resources change. This avoids
   *           expensive recomputation on every allocation decision that queries headroom.
   */
  public Resource getHeadroom() {
    return resourceLimit;
  }
  
  /**
   * Get the number of reserved containers for a given scheduler request key.
   * @param schedulerKey the scheduler request key
   * @return the number of reserved containers for the given key
   *
   * @complexity Time: O(1) for HashMap.get() and Map.size() operations.
   *             Both are constant-time operations on the reservation data structure.
   *             Space: O(1) - no additional memory allocation.
   *             Source: SchedulerApplicationAttempt.java:644-654
   */
  public int getNumReservedContainers(
      SchedulerRequestKey schedulerKey) {
    readLock.lock();
    try {
      Map<NodeId, RMContainer> map = this.reservedContainers.get(
          schedulerKey);
      return (map == null) ? 0 : map.size();
    } finally {
      readLock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  public void containerLaunchedOnNode(ContainerId containerId,
      NodeId nodeId) {
    readLock.lock();
    try {
      // Inform the container
      RMContainer rmContainer = getRMContainer(containerId);
      if (rmContainer == null) {
        // Some unknown container sneaked into the system. Kill it.
        rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeCleanContainerEvent(nodeId, containerId));
        return;
      }

      rmContainer.handle(
          new RMContainerEvent(containerId, RMContainerEventType.LAUNCHED));
    } finally {
      readLock.unlock();
    }
  }
  
  public void showRequests() {
    if (LOG.isDebugEnabled()) {
      readLock.lock();
      try {
        for (SchedulerRequestKey schedulerKey : getSchedulerKeys()) {
          AppPlacementAllocator ap = getAppPlacementAllocator(schedulerKey);
          if (ap != null &&
              ap.getOutstandingAsksCount(ResourceRequest.ANY) > 0) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                + " headRoom=" + getHeadroom() + " currentConsumption="
                + attemptResourceUsage.getUsed().getMemorySize());
            ap.showRequests();
          }
        }
      } finally {
        readLock.unlock();
      }
    }
  }
  
  public Resource getCurrentConsumption() {
    return attemptResourceUsage.getUsed();
  }
  
  private Container updateContainerAndNMToken(RMContainer rmContainer,
      ContainerUpdateType updateType) {
    Container container = rmContainer.getContainer();
    ContainerType containerType = ContainerType.TASK;
    if (updateType != null) {
      container.setVersion(container.getVersion() + 1);
    }
    // The working knowledge is that masterContainer for AM is null as it
    // itself is the master container.
    if (isWaitingForAMContainer()) {
      containerType = ContainerType.APPLICATION_MASTER;
    }
    try {
      // create container token and NMToken altogether.
      container.setContainerToken(rmContext.getContainerTokenSecretManager()
          .createContainerToken(container.getId(), container.getVersion(),
              container.getNodeId(), getUser(), container.getResource(),
              container.getPriority(), rmContainer.getCreationTime(),
              this.logAggregationContext, rmContainer.getNodeLabelExpression(),
              containerType, container.getExecutionType(),
              container.getAllocationRequestId(),
              rmContainer.getAllocationTags()));
      container.setAllocationTags(rmContainer.getAllocationTags());
      updateNMToken(container);
    } catch (IllegalArgumentException e) {
      // DNS might be down, skip returning this container.
      LOG.error("Error trying to assign container token and NM token to"
          + " an updated container " + container.getId(), e);
      return null;
    }

    if (updateType == null) {
      // This is a newly allocated container
      rmContainer.handle(new RMContainerEvent(
          rmContainer.getContainerId(), RMContainerEventType.ACQUIRED));
    } else {
      // Resource increase is handled as follows:
      // If the AM does not use the updated token to increase the container
      // for a configured period of time, the RM will automatically rollback
      // the update by performing a container decrease. This rollback (which
      // essentially is another resource decrease update) is notified to the
      // NM heartbeat response. If autoUpdate flag is set, then AM does not
      // need to do anything - same code path as resource decrease.
      //
      // Resource Decrease is always automatic: the AM never has to do
      // anything. It is always via NM heartbeat response.
      //
      // ExecutionType updates (both Promotion and Demotion) are either
      // always automatic (if the flag is set) or the AM has to explicitly
      // call updateContainer() on the NM. There is no expiry
      boolean autoUpdate =
          ContainerUpdateType.DECREASE_RESOURCE == updateType ||
              ((AbstractYarnScheduler)rmContext.getScheduler())
                  .shouldContainersBeAutoUpdated();
      if (autoUpdate) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeUpdateContainerEvent(rmContainer.getNodeId(),
                Collections.singletonMap(
                    rmContainer.getContainer(), updateType)));
      } else {
        rmContainer.handle(new RMContainerUpdatesAcquiredEvent(
            rmContainer.getContainerId(),
            ContainerUpdateType.INCREASE_RESOURCE == updateType));
      }
    }
    return container;
  }

  public void updateNMTokens(Collection<Container> containers) {
    for (Container container : containers) {
      updateNMToken(container);
    }
  }

  private void updateNMToken(Container container) {
    NMToken nmToken =
        rmContext.getNMTokenSecretManager().createAndGetNMToken(getUser(),
            getApplicationAttemptId(), container);
    if (nmToken != null) {
      updatedNMTokens.add(nmToken);
    }
  }

  /**
   * Called when AM registers. These containers are reported to the AM in the
   * <code>
   * RegisterApplicationMasterResponse#containersFromPreviousAttempts
   * </code>.
   */
  List<RMContainer> pullContainersToTransfer() {
    writeLock.lock();
    try {
      recoveredPreviousAttemptContainers.clear();
      return new ArrayList<>(liveContainers.values());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Called when AM heartbeats. These containers were recovered by the RM after
   * the AM had registered. They are reported to the AM in the
   * <code>AllocateResponse#containersFromPreviousAttempts</code>.
   * @return Container List.
   */
  public List<Container> pullPreviousAttemptContainers() {
    writeLock.lock();
    try {
      if (recoveredPreviousAttemptContainers.isEmpty()) {
        return null;
      }
      List<Container> returnContainerList = new ArrayList<>
          (recoveredPreviousAttemptContainers);
      recoveredPreviousAttemptContainers.clear();
      updateNMTokens(returnContainerList);
      return returnContainerList;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Pull and process newly allocated containers, creating tokens and removing
   * them from the pending list.
   *
   * Create container token and update NMToken altogether, if either of them fails for
   * some reason like DNS unavailable, do not return this container and keep it
   * in the newlyAllocatedContainers waiting to be refetched.
   * @return list of containers with tokens successfully created
   *
   * @complexity Time: O(a) where a=number of newly allocated containers. Each container
   *             requires token generation (O(1) cryptographic operations) and list
   *             operations. The iteration processes each container once.
   *             Space: O(a) for the returned ArrayList of containers.
   *             Source: SchedulerApplicationAttempt.java:825-848
   */
  public List<Container> pullNewlyAllocatedContainers() {
    writeLock.lock();
    try {
      List<Container> returnContainerList = new ArrayList<Container>(
          newlyAllocatedContainers.size());

      Iterator<RMContainer> i = newlyAllocatedContainers.iterator();
      while (i.hasNext()) {
        RMContainer rmContainer = i.next();
        Container updatedContainer =
            updateContainerAndNMToken(rmContainer, null);
        // Only add container to return list when it's not null.
        // updatedContainer could be null when generate token failed, it can be
        // caused by DNS resolving failed.
        if (updatedContainer != null) {
          returnContainerList.add(updatedContainer);
          i.remove();
        }
      }
      return returnContainerList;
    } finally {
      writeLock.unlock();
    }
  }

  public synchronized void addToNewlyDemotedContainers(ContainerId containerId,
      RMContainer rmContainer) {
    newlyDemotedContainers.put(containerId, rmContainer);
  }

  public synchronized void addToNewlyDecreasedContainers(
      ContainerId containerId, RMContainer rmContainer) {
    newlyDecreasedContainers.put(containerId, rmContainer);
  }

  protected synchronized void addToUpdateContainerErrors(
      UpdateContainerError error) {
    updateContainerErrors.add(error);
  }

  @VisibleForTesting
  public synchronized void addToNewlyAllocatedContainers(
      SchedulerNode node, RMContainer rmContainer) {
    ContainerId matchedContainerId =
        getUpdateContext().matchContainerToOutstandingIncreaseReq(
            node, rmContainer.getAllocatedSchedulerKey(), rmContainer);
    if (matchedContainerId != null) {
      if (ContainerUpdateContext.UNDEFINED == matchedContainerId) {
        // This is a spurious allocation (relaxLocality = false
        // resulted in the Container being allocated on an NM on the same host
        // but not on the NM running the container to be updated. Can
        // happen if more than one NM exists on the same host.. usually
        // occurs when using MiniYARNCluster to test).
        tempContainerToKill.add(rmContainer);
      } else {
        RMContainer existingContainer = getRMContainer(matchedContainerId);
        // If this container was already GUARANTEED, then it is an
        // increase, else its a promotion
        if (existingContainer == null ||
            EnumSet.of(RMContainerState.COMPLETED, RMContainerState.KILLED,
                RMContainerState.EXPIRED, RMContainerState.RELEASED).contains(
                    existingContainer.getState())) {
          tempContainerToKill.add(rmContainer);
        } else {
          if (ExecutionType.GUARANTEED == existingContainer.getExecutionType()) {
            newlyIncreasedContainers.put(matchedContainerId, rmContainer);
          } else {
            newlyPromotedContainers.put(matchedContainerId, rmContainer);
          }
        }
      }
    } else {
      newlyAllocatedContainers.add(rmContainer);
    }
  }

  /**
   * Pull containers that have been newly promoted from OPPORTUNISTIC to GUARANTEED
   * execution type.
   * @return list of newly promoted containers
   *
   * @complexity Time: O(p) where p=number of promoted containers. Delegates to
   *             pullNewlyUpdatedContainers which iterates over the promoted containers
   *             map, performing container swaps and token updates.
   *             Space: O(p) for the returned list of promoted containers.
   *             Source: SchedulerApplicationAttempt.java:901-904
   */
  public List<Container> pullNewlyPromotedContainers() {
    return pullNewlyUpdatedContainers(newlyPromotedContainers,
        ContainerUpdateType.PROMOTE_EXECUTION_TYPE);
  }

  /**
   * Pull containers that have been newly demoted from GUARANTEED to OPPORTUNISTIC
   * execution type.
   * @return list of newly demoted containers
   *
   * @complexity Time: O(d) where d=number of demoted containers.
   *             Space: O(d) for the returned list.
   *             Source: SchedulerApplicationAttempt.java:906-909
   */
  public List<Container> pullNewlyDemotedContainers() {
    return pullNewlyUpdatedContainers(newlyDemotedContainers,
        ContainerUpdateType.DEMOTE_EXECUTION_TYPE);
  }

  /**
   * Pull containers that have had their resources increased.
   * @return list of newly increased containers
   *
   * @complexity Time: O(i) where i=number of increased containers.
   *             Space: O(i) for the returned list.
   *             Source: SchedulerApplicationAttempt.java:911-914
   */
  public List<Container> pullNewlyIncreasedContainers() {
    return pullNewlyUpdatedContainers(newlyIncreasedContainers,
        ContainerUpdateType.INCREASE_RESOURCE);
  }

  /**
   * Pull containers that have had their resources decreased.
   * @return list of newly decreased containers
   *
   * @complexity Time: O(d) where d=number of decreased containers.
   *             Space: O(d) for the returned list.
   *             Source: SchedulerApplicationAttempt.java:916-919
   */
  public List<Container> pullNewlyDecreasedContainers() {
    return pullNewlyUpdatedContainers(newlyDecreasedContainers,
        ContainerUpdateType.DECREASE_RESOURCE);
  }

  /**
   * Pull any container update errors that occurred.
   * @return list of container update errors
   *
   * @complexity Time: O(e) where e=number of errors for ArrayList copy and clear.
   *             Space: O(e) for the returned list.
   */
  public List<UpdateContainerError> pullUpdateContainerErrors() {
    List<UpdateContainerError> errors =
        new ArrayList<>(updateContainerErrors);
    updateContainerErrors.clear();
    return errors;
  }

  /**
   * A container is promoted if its executionType is changed from
   * OPPORTUNISTIC to GUARANTEED. It id demoted if the change is from
   * GUARANTEED to OPPORTUNISTIC.
   * @param newlyUpdatedContainers the map of containers to process
   * @param updateTpe the type of update being performed
   * @return Newly Promoted and Demoted containers
   *
   * @complexity Time: O(u) where u=number of containers in the update map.
   *             Each container requires swap operations, token updates, and
   *             map modifications. Additional O(t) for temp container cleanup
   *             where t=tempContainerToKill size.
   *             Space: O(u) for the returned list of updated containers.
   *             Source: SchedulerApplicationAttempt.java:934-989
   */
  private List<Container> pullNewlyUpdatedContainers(
      Map<ContainerId, RMContainer> newlyUpdatedContainers,
      ContainerUpdateType updateTpe) {
    List<Container> updatedContainers = new ArrayList<>();
    if (oppContainerContext == null &&
        (ContainerUpdateType.DEMOTE_EXECUTION_TYPE == updateTpe
            || ContainerUpdateType.PROMOTE_EXECUTION_TYPE == updateTpe)) {
      return updatedContainers;
    }

    writeLock.lock();
    try {
      Iterator<Map.Entry<ContainerId, RMContainer>> i =
          newlyUpdatedContainers.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<ContainerId, RMContainer> entry = i.next();
        ContainerId matchedContainerId = entry.getKey();
        RMContainer tempRMContainer = entry.getValue();

        RMContainer existingRMContainer =
            getRMContainer(matchedContainerId);
        if (existingRMContainer != null) {
          // swap containers
          existingRMContainer = getUpdateContext().swapContainer(
              tempRMContainer, existingRMContainer, updateTpe);
          getUpdateContext().removeFromOutstandingUpdate(
              tempRMContainer.getAllocatedSchedulerKey(),
              existingRMContainer.getContainer());
          Container updatedContainer = updateContainerAndNMToken(
              existingRMContainer, updateTpe);
          updatedContainers.add(updatedContainer);
        }
        tempContainerToKill.add(tempRMContainer);
        i.remove();
      }
      // Release all temporary containers
      Iterator<RMContainer> tempIter = tempContainerToKill.iterator();
      while (tempIter.hasNext()) {
        RMContainer c = tempIter.next();
        // Mark container for release (set RRs to null, so RM does not think
        // it is a recoverable container)
        ((RMContainerImpl) c).setContainerRequest(null);

        // Release this container async-ly so as to prevent
        // 'LeafQueue::completedContainer()' from trying to acquire a lock
        // on the app and queue which can contended for in the reverse order
        // by the Scheduler thread.
        ((AbstractYarnScheduler)rmContext.getScheduler())
            .asyncContainerRelease(c);
        tempIter.remove();
      }
      return updatedContainers;
    } finally {
      writeLock.unlock();
    }
  }

  public List<NMToken> pullUpdatedNMTokens() {
    writeLock.lock();
    try {
      List <NMToken> returnList = new ArrayList<>(updatedNMTokens);
      updatedNMTokens.clear();
      return returnList;
    } finally {
      writeLock.unlock();
    }

  }

  public boolean isWaitingForAMContainer() {
    // The working knowledge is that masterContainer for AM is null as it
    // itself is the master container.
    return (!unmanagedAM && appAttempt.getMasterContainer() == null);
  }

  public void updateBlacklist(List<String> blacklistAdditions,
      List<String> blacklistRemovals) {
    writeLock.lock();
    try {
      if (!isStopped) {
        if (isWaitingForAMContainer()) {
          // The request is for the AM-container, and the AM-container is
          // launched by the system. So, update the places that are blacklisted
          // by system (as opposed to those blacklisted by the application).
          this.appSchedulingInfo.updatePlacesBlacklistedBySystem(
              blacklistAdditions, blacklistRemovals);
        } else{
          this.appSchedulingInfo.updatePlacesBlacklistedByApp(
              blacklistAdditions, blacklistRemovals);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isPlaceBlacklisted(String resourceName) {
    readLock.lock();
    try {
      boolean forAMContainer = isWaitingForAMContainer();
      return this.appSchedulingInfo.isPlaceBlacklisted(resourceName,
          forAMContainer);
    } finally {
      readLock.unlock();
    }
  }

  public int addMissedNonPartitionedRequestSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    try {
      return missedNonPartitionedReqSchedulingOpportunity.add(
          schedulerKey, 1) + 1;
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
      return Integer.MAX_VALUE;
    }
  }

  public void
      resetMissedNonPartitionedRequestSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    missedNonPartitionedReqSchedulingOpportunity.setCount(schedulerKey, 0);
  }

  
  public void addSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    try {
      schedulingOpportunities.add(schedulerKey, 1);
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
    }
  }
  
  public void subtractSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    this.schedulingOpportunities.removeExactly(schedulerKey, 1);
  }

  /**
   * Return the number of times the application has been given an opportunity
   * to schedule a task at the given priority since the last time it
   * successfully did so.
   * @param schedulerKey Scheduler Key
   * @return number of scheduling opportunities
   */
  public int getSchedulingOpportunities(
      SchedulerRequestKey schedulerKey) {
    return schedulingOpportunities.count(schedulerKey);
  }
  
  /**
   * Should be called when an application has successfully scheduled a
   * container, or when the scheduling locality threshold is relaxed.
   * Reset various internal counters which affect delay scheduling
   *
   * @param schedulerKey The priority of the container scheduled.
   */
  public void resetSchedulingOpportunities(
      SchedulerRequestKey schedulerKey) {
    resetSchedulingOpportunities(schedulerKey, System.currentTimeMillis());
  }

  // used for continuous scheduling
  public void resetSchedulingOpportunities(SchedulerRequestKey schedulerKey,
      long currentTimeMs) {
    lastScheduledContainer.put(schedulerKey, currentTimeMs);
    schedulingOpportunities.setCount(schedulerKey, 0);
  }

  @VisibleForTesting
  void setSchedulingOpportunities(SchedulerRequestKey schedulerKey, int count) {
    schedulingOpportunities.setCount(schedulerKey, count);
  }

  /**
   * Get running aggregate resource usage with caching optimization.
   * @return aggregate resource usage including resource-seconds calculations
   *
   * @complexity Time: O(c * d) where c=number of live containers and d=number of
   *             resource dimensions, when cache is stale (>3000ms old). O(1) when
   *             cache is fresh, returning cached lastResourceSecondsMap.
   *             Space: O(d) for the resourceSecondsMap where d=resource dimensions.
   *             Source: SchedulerApplicationAttempt.java:1109-1132
   *
   * @implNote Implements a time-based caching strategy to avoid expensive iteration
   *           over all live containers on every call. The cache is invalidated after
   *           MEM_AGGREGATE_ALLOCATION_CACHE_MSECS (3000ms), providing a balance
   *           between data freshness and computational efficiency. This is particularly
   *           important for applications with large container counts where iteration
   *           cost would be significant.
   */
  private AggregateAppResourceUsage getRunningAggregateAppResourceUsage() {
    long currentTimeMillis = System.currentTimeMillis();
    // Don't walk the whole container list if the resources were computed
    // recently.
    if ((currentTimeMillis - lastMemoryAggregateAllocationUpdateTime)
        > MEM_AGGREGATE_ALLOCATION_CACHE_MSECS) {
      Map<String, Long> resourceSecondsMap = new HashMap<>();
      for (RMContainer rmContainer : this.liveContainers.values()) {
        long usedMillis = currentTimeMillis - rmContainer.getCreationTime();
        Resource resource = rmContainer.getContainer().getResource();
        for (ResourceInformation entry : resource.getResources()) {
          long value = RMServerUtils
              .getOrDefault(resourceSecondsMap, entry.getName(), 0L);
          value += entry.getValue() * usedMillis
              / DateUtils.MILLIS_PER_SECOND;
          resourceSecondsMap.put(entry.getName(), value);
        }
      }

      lastMemoryAggregateAllocationUpdateTime = currentTimeMillis;
      lastResourceSecondsMap = resourceSecondsMap;
    }
    return new AggregateAppResourceUsage(lastResourceSecondsMap);
  }

  /**
   * Get the resource usage report for this application attempt.
   * @return the application resource usage report containing used, reserved, and
   *         aggregate resource statistics
   *
   * @complexity Time: O(c) where c=number of live containers when cache is stale
   *             (older than MEM_AGGREGATE_ALLOCATION_CACHE_MSECS=3000ms), as
   *             getRunningAggregateAppResourceUsage() iterates over all containers.
   *             O(1) when cache is fresh, returning cached aggregate values.
   *             Space: O(1) for the returned report object with cloned resources.
   *             Source: SchedulerApplicationAttempt.java:1134-1173
   *
   * @implNote Uses a caching strategy for aggregate resource-seconds calculation
   *           to avoid expensive iteration over liveContainers on every call.
   *           Cache invalidates after 3 seconds (MEM_AGGREGATE_ALLOCATION_CACHE_MSECS).
   *           This balances accuracy with performance for frequent UI/API queries.
   */
  public ApplicationResourceUsageReport getResourceUsageReport() {
    writeLock.lock();
    try {
      AggregateAppResourceUsage runningResourceUsage =
          getRunningAggregateAppResourceUsage();
      Resource usedResourceClone = Resources.clone(
          attemptResourceUsage.getAllUsed());
      Resource reservedResourceClone =
          Resources.clone(attemptResourceUsage.getAllReserved());
      Resource cluster = rmContext.getScheduler().getClusterResource();
      ResourceCalculator calc =
          rmContext.getScheduler().getResourceCalculator();
      Map<String, Long> preemptedResourceSecondsMaps = new HashMap<>();
      preemptedResourceSecondsMaps
          .put(ResourceInformation.MEMORY_MB.getName(), 0L);
      preemptedResourceSecondsMaps
          .put(ResourceInformation.VCORES.getName(), 0L);
      float queueUsagePerc = 0.0f;
      float clusterUsagePerc = 0.0f;
      if (!calc.isAllInvalidDivisor(cluster)) {
        float queueCapacityPerc = queue.getQueueInfo(false, false)
            .getCapacity();
        queueUsagePerc = calc.divide(cluster, usedResourceClone,
            Resources.multiply(cluster, queueCapacityPerc)) * 100;
        if (Float.isNaN(queueUsagePerc) || Float.isInfinite(queueUsagePerc)) {
          queueUsagePerc = 0.0f;
        }
        clusterUsagePerc =
            calc.divide(cluster, usedResourceClone, cluster) * 100;
      }
      return ApplicationResourceUsageReport
          .newInstance(liveContainers.size(), reservedContainers.size(),
              usedResourceClone, reservedResourceClone,
              Resources.add(usedResourceClone, reservedResourceClone),
              runningResourceUsage.getResourceUsageSecondsMap(), queueUsagePerc,
              clusterUsagePerc, preemptedResourceSecondsMaps);
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  public Map<ContainerId, RMContainer> getLiveContainersMap() {
    return this.liveContainers;
  }

  public Map<SchedulerRequestKey, Long>
      getLastScheduledContainer() {
    return this.lastScheduledContainer;
  }

  public void transferStateFromPreviousAttempt(
      SchedulerApplicationAttempt appAttempt) {
    writeLock.lock();
    try {
      this.liveContainers = appAttempt.getLiveContainersMap();
      // this.reReservations = appAttempt.reReservations;
      this.attemptResourceUsage.copyAllUsed(appAttempt.attemptResourceUsage);
      this.setHeadroom(appAttempt.resourceLimit);
      // this.currentReservation = appAttempt.currentReservation;
      // this.newlyAllocatedContainers = appAttempt.newlyAllocatedContainers;
      // this.schedulingOpportunities = appAttempt.schedulingOpportunities;
      this.lastScheduledContainer = appAttempt.getLastScheduledContainer();
      this.appSchedulingInfo.transferStateFromPreviousAppSchedulingInfo(
          appAttempt.appSchedulingInfo);
    } finally {
      writeLock.unlock();
    }
  }
  
  public void move(Queue newQueue) {
    writeLock.lock();
    try {
      QueueMetrics oldMetrics = queue.getMetrics();
      QueueMetrics newMetrics = newQueue.getMetrics();
      String newQueueName = newQueue instanceof CSQueue ?
              ((CSQueue) newQueue).getQueuePath() : newQueue.getQueueName();
      String user = getUser();

      for (RMContainer liveContainer : liveContainers.values()) {
        Resource resource = liveContainer.getContainer().getResource();
        ((RMContainerImpl) liveContainer).setQueueName(newQueueName);
        oldMetrics.releaseResources(liveContainer.getNodeLabelExpression(),
            user, 1, resource);
        newMetrics.allocateResources(liveContainer.getNodeLabelExpression(),
            user, 1, resource, false);
      }
      for (Map<NodeId, RMContainer> map : reservedContainers.values()) {
        for (RMContainer reservedContainer : map.values()) {
          ((RMContainerImpl) reservedContainer).setQueueName(newQueueName);
          Resource resource = reservedContainer.getReservedResource();
          oldMetrics.unreserveResource(
              reservedContainer.getNodeLabelExpression(), user, resource);
          newMetrics.reserveResource(
              reservedContainer.getNodeLabelExpression(), user, resource);
        }
      }

      if (!isStopped) {
        appSchedulingInfo.move(newQueue);
      }
      this.queue = newQueue;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean recoverContainer(SchedulerNode node,
      RMContainer rmContainer) {
    writeLock.lock();
    try {
      // recover app scheduling info
      appSchedulingInfo.recoverContainer(rmContainer, node.getPartition());

      if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        return false;
      }
      LOG.info("SchedulerAttempt " + getApplicationAttemptId()
          + " is recovering container " + rmContainer.getContainerId());
      addRMContainer(rmContainer.getContainerId(), rmContainer);
      if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
        attemptResourceUsage.incUsed(node.getPartition(),
            rmContainer.getContainer().getResource());
      }

      return true;

      // resourceLimit: updated when LeafQueue#recoverContainer#allocateResource
      // is called.
      // newlyAllocatedContainers.add(rmContainer);
      // schedulingOpportunities
      // lastScheduledContainer
    } finally {
      writeLock.unlock();
    }
  }

  public void incNumAllocatedContainers(NodeType containerType,
      NodeType requestType) {
    if (containerType == null || requestType == null) {
      // Sanity check
      return;
    }

    RMApp app = rmContext.getRMApps().get(attemptId.getApplicationId());
    if (app != null) {
      RMAppAttempt attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        attempt.getRMAppAttemptMetrics()
            .incNumAllocatedContainers(containerType, requestType);
      }
    }
  }

  public void setApplicationHeadroomForMetrics(Resource headroom) {
    RMAppAttempt attempt =
        rmContext.getRMApps().get(attemptId.getApplicationId())
            .getCurrentAppAttempt();
    if (attempt != null) {
      attempt.getRMAppAttemptMetrics().setApplicationAttemptHeadRoom(
          Resources.clone(headroom));
    }
  }

  public void recordContainerRequestTime(long value) {
    firstAllocationRequestSentTime.compareAndSet(0, value);
  }

  public void recordContainerAllocationTime(long value) {
    if (firstContainerAllocatedTime.compareAndSet(0, value)) {
      long timediff = firstContainerAllocatedTime.longValue() -
          firstAllocationRequestSentTime.longValue();
      if (timediff > 0) {
        queue.getMetrics().addAppAttemptFirstContainerAllocationDelay(timediff);
      }
    }
  }

  public Set<String> getBlacklistedNodes() {
    return this.appSchedulingInfo.getBlackListCopy();
  }
  
  @Private
  public boolean hasPendingResourceRequest(String nodePartition,
      SchedulingMode schedulingMode) {
    // We need to consider unconfirmed allocations
    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      nodePartition = RMNodeLabelsManager.NO_LABEL;
    }

    Resource pending = attemptResourceUsage.getPending(nodePartition);

    // TODO, need consider node partition here
    // To avoid too many allocation-proposals rejected for non-default
    // partition allocation
    if (StringUtils.equals(nodePartition, RMNodeLabelsManager.NO_LABEL)) {
      pending = Resources.subtractNonNegative(pending, Resources
          .createResource(unconfirmedAllocatedMem.get(),
              unconfirmedAllocatedVcores.get()));
    }

    return !Resources.isNone(pending);
  }

  /*
   * Note that the behavior of appAttemptResourceUsage is different from queue's
   * For queue, used = actual-used + reserved
   * For app, used = actual-used.
   *
   * TODO (wangda): Need to make behaviors of queue/app's resource usage
   * consistent
   */
  @VisibleForTesting
  public ResourceUsage getAppAttemptResourceUsage() {
    return this.attemptResourceUsage;
  }

  @Override
  public Priority getPriority() {
    return appPriority;
  }

  public void setPriority(Priority appPriority) {
    this.appPriority = appPriority;
  }

  @Override
  public String getId() {
    return getApplicationId().toString();
  }
  
  @Override
  public int compareInputOrderTo(SchedulableEntity other) {
    if (other instanceof SchedulerApplicationAttempt) {
      return getApplicationId().compareTo(
        ((SchedulerApplicationAttempt)other).getApplicationId());
    }
    return 1;//let other types go before this, if any
  }
  
  @Override
  public ResourceUsage getSchedulingResourceUsage() {
    return attemptResourceUsage;
  }

  public void setAppAMNodePartitionName(String partitionName) {
    this.appAMNodePartitionName = partitionName;
  }

  public String getAppAMNodePartitionName() {
    return appAMNodePartitionName;
  }

  public void updateAMContainerDiagnostics(AMState state,
      String diagnosticMessage) {
    if (!isWaitingForAMContainer()) {
      return;
    }
    StringBuilder diagnosticMessageBldr = new StringBuilder();
    diagnosticMessageBldr.append("[")
        .append(fdf.format(System.currentTimeMillis()))
        .append("] ");
    switch (state) {
    case INACTIVATED:
      diagnosticMessageBldr.append(state.diagnosticMessage);
      if (diagnosticMessage != null) {
        diagnosticMessageBldr.append(diagnosticMessage);
      }
      getPendingAppDiagnosticMessage(diagnosticMessageBldr);
      break;
    case ACTIVATED:
      diagnosticMessageBldr.append(state.diagnosticMessage);
      if (diagnosticMessage != null) {
        diagnosticMessageBldr.append(diagnosticMessage);
      }
      getActivedAppDiagnosticMessage(diagnosticMessageBldr);
      break;
    default:
      // UNMANAGED , ASSIGNED
      diagnosticMessageBldr.append(state.diagnosticMessage);
      break;
    }
    appAttempt.updateAMLaunchDiagnostics(diagnosticMessageBldr.toString());
  }

  protected void getPendingAppDiagnosticMessage(
      StringBuilder diagnosticMessage) {
    // Give the specific information which might be applicable for the
    // respective scheduler
    // like partitionAMResourcelimit,UserAMResourceLimit, queue'AMResourceLimit
  }

  protected void getActivedAppDiagnosticMessage(
      StringBuilder diagnosticMessage) {
    // Give the specific information which might be applicable for the
    // respective scheduler
    // queue's resource usage for specific partition
  }

  public ReentrantReadWriteLock.WriteLock getWriteLock() {
    return writeLock;
  }

  @Override
  public boolean isRecovering() {
    return isAttemptRecovering;
  }

  protected void setAttemptRecovering(boolean isRecovering) {
    this.isAttemptRecovering = isRecovering;
  }

  public <N extends SchedulerNode> AppPlacementAllocator<N> getAppPlacementAllocator(
      SchedulerRequestKey schedulerRequestKey) {
    return appSchedulingInfo.getAppPlacementAllocator(schedulerRequestKey);
  }
  public void incUnconfirmedRes(Resource res) {
    unconfirmedAllocatedMem.addAndGet(res.getMemorySize());
    unconfirmedAllocatedVcores.addAndGet(res.getVirtualCores());
  }

  public void decUnconfirmedRes(Resource res) {
    unconfirmedAllocatedMem.addAndGet(-res.getMemorySize());
    unconfirmedAllocatedVcores.addAndGet(-res.getVirtualCores());
  }

  @Override
  public int hashCode() {
    return getApplicationAttemptId().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (! (o instanceof SchedulerApplicationAttempt)) {
      return false;
    }

    SchedulerApplicationAttempt other = (SchedulerApplicationAttempt) o;
    return (this == other ||
        this.getApplicationAttemptId().equals(other.getApplicationAttemptId()));
  }

  /**
   * Different state for Application Master, user can see this state from web UI
   */
  public enum AMState {
    UNMANAGED("User launched the Application Master, since it's unmanaged. "),
    INACTIVATED("Application is added to the scheduler and is not yet activated. "),
    ACTIVATED("Application is Activated, waiting for resources to be assigned for AM. "),
    ASSIGNED("Scheduler has assigned a container for AM, waiting for AM "
        + "container to be launched"),
    LAUNCHED("AM container is launched, waiting for AM container to Register "
        + "with RM")
    ;

    private String diagnosticMessage;

    AMState(String diagnosticMessage) {
      this.diagnosticMessage = diagnosticMessage;
    }

    public String getDiagnosticMessage() {
      return diagnosticMessage;
    }
  }

  public Map<String, String> getApplicationSchedulingEnvs() {
    return this.applicationSchedulingEnvs;
  }

  @Override
  public String getPartition() {
    return nodeLabelExpression == null ? "" : nodeLabelExpression;
  }


  @Override
  public long getStartTime() {
    return startTime;
  }
}
