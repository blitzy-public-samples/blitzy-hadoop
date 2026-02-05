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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.RejectionReason;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ApplicationSchedulingConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PendingAskUpdateResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SingleConstraintAppPlacementAllocator;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;
/**
 * This class keeps track of all the consumption of an application. This also
 * keeps track of current running/completed containers for the application.
 *
 * <p>This class manages resource requests, scheduling keys, and blacklist
 * information for an application attempt during scheduling.</p>
 *
 * @performance Memory footprint: O(p + b) where p=pending requests count and b=blacklist size.
 *              Operations scale linearly with pending request count. Concurrent read access
 *              is optimized via ReentrantReadWriteLock allowing multiple readers during
 *              scheduling decisions while serializing write operations for request updates.
 *
 * @implNote Data structure selection rationale:
 *           <ul>
 *           <li>{@code ConcurrentSkipListSet} for schedulerKeys provides O(log p) ordered
 *               access for priority-based scheduling vs O(p) unsorted iteration with HashSet;
 *               enables efficient first()/last() operations for priority queue semantics.</li>
 *           <li>{@code ConcurrentHashMap} for schedulerKeyToAppPlacementAllocator provides
 *               O(1) average lookup with thread safety for concurrent scheduler access;
 *               chosen over synchronized HashMap for better concurrent read throughput.</li>
 *           <li>{@code HashSet} for placesBlacklistedByApp and placesBlacklistedBySystem
 *               enables O(1) membership checks vs O(n) list iteration; blacklist operations
 *               are synchronized for thread safety during updates.</li>
 *           <li>{@code ReentrantReadWriteLock} separates read and write access patterns;
 *               scheduling decisions (reads) can proceed concurrently while request updates
 *               (writes) are serialized, optimizing for read-heavy scheduling workloads.</li>
 *           </ul>
 *
 * Source: AppSchedulingInfo.java:90-101
 */
@Private
@Unstable
public class AppSchedulingInfo {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(AppSchedulingInfo.class);

  private final ApplicationId applicationId;
  private final ApplicationAttemptId applicationAttemptId;
  private final AtomicLong containerIdCounter;
  private final String user;

  private Queue queue;
  private AbstractUsersManager abstractUsersManager;
  // whether accepted/allocated by scheduler
  private volatile boolean pending = true;
  private ResourceUsage appResourceUsage;

  private AtomicBoolean userBlacklistChanged = new AtomicBoolean(false);
  // Set of places (nodes / racks) blacklisted by the system. Today, this only
  // has places blacklisted for AM containers.
  private final Set<String> placesBlacklistedBySystem = new HashSet<>();
  private Set<String> placesBlacklistedByApp = new HashSet<>();

  private Set<String> requestedPartitions = new HashSet<>();

  private final ConcurrentSkipListSet<SchedulerRequestKey>
      schedulerKeys = new ConcurrentSkipListSet<>();
  private final Map<SchedulerRequestKey, AppPlacementAllocator<SchedulerNode>>
      schedulerKeyToAppPlacementAllocator = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  public final ContainerUpdateContext updateContext;
  private final Map<String, String> applicationSchedulingEnvs = new HashMap<>();
  private final RMContext rmContext;
  private final int retryAttempts;
  private boolean unmanagedAM;

  private final String defaultResourceRequestAppPlacementType;

  public AppSchedulingInfo(ApplicationAttemptId appAttemptId, String user,
      Queue queue, AbstractUsersManager abstractUsersManager, long epoch,
      ResourceUsage appResourceUsage,
      Map<String, String> applicationSchedulingEnvs, RMContext rmContext,
      boolean unmanagedAM) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.user = user;
    this.abstractUsersManager = abstractUsersManager;
    this.containerIdCounter = new AtomicLong(
        epoch << ResourceManager.EPOCH_BIT_SHIFT);
    this.appResourceUsage = appResourceUsage;
    this.applicationSchedulingEnvs.putAll(applicationSchedulingEnvs);
    this.rmContext = rmContext;
    this.retryAttempts = rmContext.getYarnConfiguration().getInt(
         YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS,
         YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS);
    this.unmanagedAM = unmanagedAM;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    updateContext = new ContainerUpdateContext(this);
    readLock = lock.readLock();
    writeLock = lock.writeLock();

    this.defaultResourceRequestAppPlacementType =
        getDefaultResourceRequestAppPlacementType();
  }

  /**
   * Set default App Placement Allocator.
   *
   * @return app placement class.
   */
  public String getDefaultResourceRequestAppPlacementType() {
    if (this.rmContext != null
        && this.rmContext.getYarnConfiguration() != null) {

      String appPlacementClass = applicationSchedulingEnvs.get(
          ApplicationSchedulingConfig.ENV_APPLICATION_PLACEMENT_TYPE_CLASS);
      if (null != appPlacementClass) {
        return appPlacementClass;
      } else {
        Configuration conf = rmContext.getYarnConfiguration();
        return conf.get(
            YarnConfiguration.APPLICATION_PLACEMENT_TYPE_CLASS);
      }
    }
    return null;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public String getUser() {
    return user;
  }

  public long getNewContainerId() {
    return this.containerIdCounter.incrementAndGet();
  }

  public String getQueueName() {
    this.readLock.lock();
    try {
      return queue.getQueueName();
    } finally {
      this.readLock.unlock();
    }
  }

  public boolean isPending() {
    return pending;
  }

  public void setUnmanagedAM(boolean unmanagedAM) {
    this.unmanagedAM = unmanagedAM;
  }

  public boolean isUnmanagedAM() {
    return unmanagedAM;
  }

  public Set<String> getRequestedPartitions() {
    return requestedPartitions;
  }

  /**
   * Clear any pending requests from this application.
   *
   * @complexity Time: O(p) where p=number of pending requests (scheduler keys);
   *             both ConcurrentSkipListSet.clear() and ConcurrentHashMap.clear()
   *             iterate through all entries.
   *             Space: O(p) memory freed from cleared data structures.
   *
   * Source: AppSchedulingInfo.java:206-210
   */
  private void clearRequests() {
    schedulerKeys.clear();
    schedulerKeyToAppPlacementAllocator.clear();
    LOG.info("Application " + applicationId + " requests cleared");
  }

  public ContainerUpdateContext getUpdateContext() {
    return updateContext;
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   *
   * @complexity Time: O(r) where r=number of resource requests being updated;
   *             each request requires HashMap grouping O(1) amortized, then
   *             O(1) AppPlacementAllocator lookup/creation and update.
   *             Space: O(r) for temporary dedup HashMap during request grouping,
   *             plus O(r) for storing new pending requests in AppPlacementAllocator.
   *
   * @param resourceRequests resource requests to be allocated
   * @param recoverPreemptedRequestForAContainer
   *          recover ResourceRequest/SchedulingRequest on preemption
   * @return true if any resource was updated, false otherwise
   *
   * Source: AppSchedulingInfo.java:226-241
   */
  public boolean updateResourceRequests(List<ResourceRequest> resourceRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // Flag to track if any incoming requests update "ANY" requests
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // Update AppPlacementAllocator by requests
      offswitchResourcesUpdated = internalAddResourceRequests(
          recoverPreemptedRequestForAContainer, resourceRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   *
   * @complexity Time: O(k * a) where k=number of scheduler keys in dedupRequests,
   *             a=average asks per key; each key requires O(1) AppPlacementAllocator
   *             lookup via ConcurrentHashMap and O(a) pending ask updates.
   *             Space: O(k * a) for storing pending requests in AppPlacementAllocator.
   *
   * @param dedupRequests (dedup) resource requests to be allocated
   * @param recoverPreemptedRequestForAContainer
   *          recover ResourceRequest/SchedulingRequest on preemption
   * @return true if any resource was updated, false otherwise
   *
   * Source: AppSchedulingInfo.java:253-269
   */
  public boolean updateResourceRequests(
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // Flag to track if any incoming requests update "ANY" requests
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // Update AppPlacementAllocator by requests
      offswitchResourcesUpdated = internalAddResourceRequests(
          recoverPreemptedRequestForAContainer, dedupRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   *
   * @complexity Time: O(s) where s=number of scheduling requests; each request
   *             requires O(1) ConcurrentHashMap lookup for AppPlacementAllocator
   *             and O(1) pending ask update.
   *             Space: O(s) for storing new pending requests in AppPlacementAllocators.
   *
   * @param schedulingRequests resource requests to be allocated
   * @param recoverPreemptedRequestForAContainer
   *          recover ResourceRequest/SchedulingRequest on preemption
   * @return true if any resource was updated, false otherwise
   *
   * Source: AppSchedulingInfo.java:281-297
   */
  public boolean updateSchedulingRequests(
      List<SchedulingRequest> schedulingRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // Flag to track if any incoming requests update "ANY" requests
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // Update AppPlacementAllocator by requests
      offswitchResourcesUpdated = addSchedulingRequests(
          recoverPreemptedRequestForAContainer, schedulingRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  public void removeAppPlacement(SchedulerRequestKey schedulerRequestKey) {
    schedulerKeyToAppPlacementAllocator.remove(schedulerRequestKey);
  }

  private boolean addSchedulingRequests(
      boolean recoverPreemptedRequestForAContainer,
      List<SchedulingRequest> schedulingRequests) {
    // Do we need to update pending resource for app/queue, etc.?
    boolean requireUpdatePendingResource = false;

    for (SchedulingRequest request : schedulingRequests) {
      SchedulerRequestKey schedulerRequestKey = SchedulerRequestKey.create(
          request);

      AppPlacementAllocator appPlacementAllocator =
          getAndAddAppPlacementAllocatorIfNotExist(schedulerRequestKey,
              SingleConstraintAppPlacementAllocator.class.getCanonicalName());

      // Update AppPlacementAllocator
      PendingAskUpdateResult pendingAmountChanges =
          appPlacementAllocator.updatePendingAsk(schedulerRequestKey,
              request, recoverPreemptedRequestForAContainer);

      if (null != pendingAmountChanges) {
        updatePendingResources(pendingAmountChanges, schedulerRequestKey,
            queue.getMetrics());
        requireUpdatePendingResource = true;
      }
    }

    return requireUpdatePendingResource;
  }

  /**
   * Get and insert AppPlacementAllocator if it doesn't exist, this should be
   * protected by write lock.
   * @param schedulerRequestKey schedulerRequestKey
   * @param placementTypeClass placementTypeClass
   * @return AppPlacementAllocator
   */
  private AppPlacementAllocator<SchedulerNode> getAndAddAppPlacementAllocatorIfNotExist(
      SchedulerRequestKey schedulerRequestKey, String placementTypeClass) {
    AppPlacementAllocator<SchedulerNode> appPlacementAllocator;
    if ((appPlacementAllocator = schedulerKeyToAppPlacementAllocator.get(
        schedulerRequestKey)) == null) {
      appPlacementAllocator =
          ApplicationPlacementAllocatorFactory.getAppPlacementAllocator(
              placementTypeClass, this, schedulerRequestKey, rmContext);
      schedulerKeyToAppPlacementAllocator.put(schedulerRequestKey,
          appPlacementAllocator);
    }
    return appPlacementAllocator;
  }

  private boolean internalAddResourceRequests(
      boolean recoverPreemptedRequestForAContainer,
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests) {
    boolean offswitchResourcesUpdated = false;
    for (Map.Entry<SchedulerRequestKey, Map<String, ResourceRequest>> entry :
    dedupRequests.entrySet()) {
      SchedulerRequestKey schedulerRequestKey = entry.getKey();
      AppPlacementAllocator<SchedulerNode> appPlacementAllocator =
          getAndAddAppPlacementAllocatorIfNotExist(schedulerRequestKey,
              defaultResourceRequestAppPlacementType);

      // Update AppPlacementAllocator
      PendingAskUpdateResult pendingAmountChanges =
          appPlacementAllocator.updatePendingAsk(entry.getValue().values(),
              recoverPreemptedRequestForAContainer);

      if (null != pendingAmountChanges) {
        updatePendingResources(pendingAmountChanges, schedulerRequestKey,
            queue.getMetrics());
        offswitchResourcesUpdated = true;
      }
    }
    return offswitchResourcesUpdated;
  }

  private boolean internalAddResourceRequests(boolean recoverPreemptedRequestForAContainer,
      List<ResourceRequest> resourceRequests) {
    if (null == resourceRequests || resourceRequests.isEmpty()) {
      return false;
    }

    // A map to group resource requests and dedup
    Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests =
        new HashMap<>();

    // Group resource request by schedulerRequestKey and resourceName
    for (ResourceRequest request : resourceRequests) {
      SchedulerRequestKey schedulerKey = SchedulerRequestKey.create(request);
      if (!dedupRequests.containsKey(schedulerKey)) {
        dedupRequests.put(schedulerKey, new HashMap<>());
      }
      dedupRequests.get(schedulerKey).put(request.getResourceName(), request);
    }

    return internalAddResourceRequests(recoverPreemptedRequestForAContainer,
        dedupRequests);
  }

  private void updatePendingResources(PendingAskUpdateResult updateResult,
      SchedulerRequestKey schedulerKey, QueueMetrics metrics) {

    PendingAsk lastPendingAsk = updateResult.getLastPendingAsk();
    PendingAsk newPendingAsk = updateResult.getNewPendingAsk();
    String lastNodePartition = updateResult.getLastNodePartition();
    String newNodePartition = updateResult.getNewNodePartition();

    int lastRequestContainers =
        (lastPendingAsk != null) ? lastPendingAsk.getCount() : 0;
    if (newPendingAsk.getCount() <= 0) {
      if (lastRequestContainers >= 0) {
        schedulerKeys.remove(schedulerKey);
        schedulerKeyToAppPlacementAllocator.remove(schedulerKey);
      }
      LOG.info("checking for deactivate of application :"
          + this.applicationId);
      checkForDeactivation();
    } else {
      // Activate application. Metrics activation is done here.
      if (lastRequestContainers <= 0) {
        schedulerKeys.add(schedulerKey);
        abstractUsersManager.activateApplication(user, applicationId);
      }
    }

    if (lastPendingAsk != null) {
      // Deduct resources from metrics / pending resources of queue/app.
      metrics.decrPendingResources(lastNodePartition, user,
          lastPendingAsk.getCount(), lastPendingAsk.getPerAllocationResource());
      Resource decreasedResource = Resources.multiply(
          lastPendingAsk.getPerAllocationResource(), lastRequestContainers);
      queue.decPendingResource(lastNodePartition, decreasedResource);
      appResourceUsage.decPending(lastNodePartition, decreasedResource);
    }

    // Increase resources to metrics / pending resources of queue/app.
    metrics.incrPendingResources(newNodePartition, user,
        newPendingAsk.getCount(), newPendingAsk.getPerAllocationResource());
    Resource increasedResource = Resources.multiply(
        newPendingAsk.getPerAllocationResource(), newPendingAsk.getCount());
    queue.incPendingResource(newNodePartition, increasedResource);
    appResourceUsage.incPending(newNodePartition, increasedResource);
  }

  public void addRequestedPartition(String partition) {
    requestedPartitions.add(partition);
  }

  public void decPendingResource(String partition, Resource toDecrease) {
    queue.decPendingResource(partition, toDecrease);
    appResourceUsage.decPending(partition, toDecrease);
  }

  /**
   * The ApplicationMaster is updating the placesBlacklistedByApp used for
   * containers other than AMs.
   *
   * @complexity Time: O(a + r) where a=blacklist additions count, r=removals count;
   *             HashSet.addAll() is O(a) and HashSet.removeAll() is O(r) for
   *             membership checks and modifications.
   *             Space: O(a) for storing new blacklist entries in HashSet.
   *
   * @implNote Uses HashSet for blacklist storage enabling O(1) membership checks
   *           during scheduling vs O(n) with list-based storage; synchronized
   *           block ensures thread-safe updates during concurrent scheduling.
   *
   * @param blacklistAdditions
   *          resources to be added to the userBlacklist
   * @param blacklistRemovals
   *          resources to be removed from the userBlacklist
   *
   * Source: AppSchedulingInfo.java:455-470
   */
  public void updatePlacesBlacklistedByApp(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    if (updateBlacklistedPlaces(placesBlacklistedByApp, blacklistAdditions,
        blacklistRemovals)) {
      userBlacklistChanged.set(true);
    }
  }

  /**
   * Update the list of places that are blacklisted by the system. Today the
   * system only blacklists places when it sees that AMs failed there
   *
   * @complexity Time: O(a + r) where a=blacklist additions count, r=removals count;
   *             HashSet.addAll() is O(a) and HashSet.removeAll() is O(r).
   *             Space: O(a) for storing new blacklist entries in HashSet.
   *
   * @param blacklistAdditions
   *          resources to be added to placesBlacklistedBySystem
   * @param blacklistRemovals
   *          resources to be removed from placesBlacklistedBySystem
   *
   * Source: AppSchedulingInfo.java:481-485
   */
  public void updatePlacesBlacklistedBySystem(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    updateBlacklistedPlaces(placesBlacklistedBySystem, blacklistAdditions,
        blacklistRemovals);
  }

  private static boolean updateBlacklistedPlaces(Set<String> blacklist,
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    boolean changed = false;
    synchronized (blacklist) {
      if (blacklistAdditions != null) {
        changed = blacklist.addAll(blacklistAdditions);
      }

      if (blacklistRemovals != null) {
        changed = blacklist.removeAll(blacklistRemovals) || changed;
      }
    }
    return changed;
  }

  public boolean getAndResetBlacklistChanged() {
    return userBlacklistChanged.getAndSet(false);
  }

  /**
   * Returns the collection of scheduler keys for pending requests.
   *
   * @complexity Time: O(1) for returning reference to internal ConcurrentSkipListSet.
   *             Space: O(1) - no additional allocation, returns view of existing data.
   *
   * @implNote Returns the underlying ConcurrentSkipListSet directly providing
   *           O(log p) ordered access for priority-based scheduling. Iteration
   *           over returned collection is O(p) where p=pending requests.
   *
   * @return collection of scheduler request keys
   *
   * Source: AppSchedulingInfo.java:506-508
   */
  public Collection<SchedulerRequestKey> getSchedulerKeys() {
    return schedulerKeys;
  }

  /**
   * Used by REST API to fetch ResourceRequest
   *
   * @complexity Time: O(k * a) where k=number of scheduler keys (AppPlacementAllocators),
   *             a=average resource requests per allocator; iterates all allocators and
   *             collects their resource requests.
   *             Space: O(p) where p=total pending requests for ArrayList allocation
   *             to hold all ResourceRequest objects.
   *
   * @return All pending ResourceRequests.
   *
   * Source: AppSchedulingInfo.java:514-526
   */
  public List<ResourceRequest> getAllResourceRequests() {
    List<ResourceRequest> ret = new ArrayList<>();
    this.readLock.lock();
    try {
      for (AppPlacementAllocator ap : schedulerKeyToAppPlacementAllocator
          .values()) {
        ret.addAll(ap.getResourceRequests().values());
      }
    } finally {
      this.readLock.unlock();
    }
    return ret;
  }

  /**
   * Fetch SchedulingRequests.
   *
   * @complexity Time: O(k) where k=number of scheduler keys (AppPlacementAllocators);
   *             iterates all allocators and filters for non-null scheduling requests.
   *             Space: O(s) where s=scheduling requests with non-null values for
   *             ArrayList allocation.
   *
   * @return All pending SchedulingRequests.
   *
   * Source: AppSchedulingInfo.java:532-543
   */
  public List<SchedulingRequest> getAllSchedulingRequests() {
    List<SchedulingRequest> ret = new ArrayList<>();
    this.readLock.lock();
    try {
      schedulerKeyToAppPlacementAllocator.values().stream()
          .filter(ap -> ap.getSchedulingRequest() != null)
          .forEach(ap -> ret.add(ap.getSchedulingRequest()));
    } finally {
      this.readLock.unlock();
    }
    return ret;
  }

  public List<RejectedSchedulingRequest> getRejectedRequest() {
    this.readLock.lock();
    try {
      return schedulerKeyToAppPlacementAllocator.values().stream()
          .filter(ap -> ap.getPlacementAttempt() >= retryAttempts)
          .map(ap -> RejectedSchedulingRequest.newInstance(
              RejectionReason.COULD_NOT_SCHEDULE_ON_NODE,
              ap.getSchedulingRequest()))
          .collect(Collectors.toList());
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Returns the next pending ask based on priority ordering.
   *
   * @complexity Time: O(log p) for ConcurrentSkipListSet.first() to get highest
   *             priority key, plus O(1) for ConcurrentHashMap lookup.
   *             Space: O(1) - no additional allocation.
   *
   * @implNote Uses ConcurrentSkipListSet.first() which provides O(log p) access
   *           to the minimum (highest priority) scheduler key, enabling efficient
   *           priority-based scheduling without full iteration.
   *
   * @return the pending ask for the highest priority request, or null if none
   *
   * Source: AppSchedulingInfo.java:559-571
   */
  public PendingAsk getNextPendingAsk() {
    readLock.lock();
    try {
      if (!schedulerKeys.isEmpty()) {
        SchedulerRequestKey firstRequestKey = schedulerKeys.first();
        return getPendingAsk(firstRequestKey, ResourceRequest.ANY);
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the pending ask for a scheduler key with ANY resource name.
   *
   * @complexity Time: O(1) for ConcurrentHashMap lookup of AppPlacementAllocator.
   *             Space: O(1) - no additional allocation.
   *
   * @param schedulerKey the scheduler request key
   * @return the pending ask for ANY resource, or ZERO if not found
   *
   * Source: AppSchedulingInfo.java:573-575
   */
  public PendingAsk getPendingAsk(SchedulerRequestKey schedulerKey) {
    return getPendingAsk(schedulerKey, ResourceRequest.ANY);
  }

  /**
   * Returns the pending ask for a scheduler key and specific resource name.
   *
   * @complexity Time: O(1) for ConcurrentHashMap lookup of AppPlacementAllocator,
   *             plus O(1) for internal pending ask retrieval.
   *             Space: O(1) - no additional allocation.
   *
   * @implNote Uses ConcurrentHashMap for O(1) average-case lookup with thread safety,
   *           avoiding synchronization overhead during read-heavy scheduling operations.
   *
   * @param schedulerKey the scheduler request key
   * @param resourceName the resource name (node, rack, or ANY)
   * @return the pending ask, or ZERO if not found
   *
   * Source: AppSchedulingInfo.java:577-587
   */
  public PendingAsk getPendingAsk(SchedulerRequestKey schedulerKey,
      String resourceName) {
    this.readLock.lock();
    try {
      AppPlacementAllocator ap = schedulerKeyToAppPlacementAllocator.get(
          schedulerKey);
      return (ap == null) ? PendingAsk.ZERO : ap.getPendingAsk(resourceName);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Returns if the place (node/rack today) is either blacklisted by the
   * application (user) or the system.
   *
   * @complexity Time: O(1) for HashSet.contains() lookup in either blacklist.
   *             Space: O(1) - no additional allocation.
   *
   * @implNote Uses HashSet for blacklist storage enabling O(1) membership checks
   *           during scheduling decisions. Synchronized access ensures consistent
   *           reads while blacklist may be concurrently updated. This is called
   *           frequently during node selection, so O(1) lookup is critical.
   *
   * @param resourceName
   *          the resourcename
   * @param blacklistedBySystem
   *          true if it should check amBlacklist
   * @return true if its blacklisted
   *
   * Source: AppSchedulingInfo.java:599-610
   */
  public boolean isPlaceBlacklisted(String resourceName,
      boolean blacklistedBySystem) {
    if (blacklistedBySystem){
      synchronized (placesBlacklistedBySystem) {
        return placesBlacklistedBySystem.contains(resourceName);
      }
    } else {
      synchronized (placesBlacklistedByApp) {
        return placesBlacklistedByApp.contains(resourceName);
      }
    }
  }

  /**
   * Allocates a container for the specified scheduler key and node.
   *
   * @complexity Time: O(1) amortized for ConcurrentHashMap lookup and
   *             AppPlacementAllocator.allocate() which decrements pending count.
   *             Metrics update is O(1) for counter increments.
   *             Space: O(1) - ContainerRequest returned is existing object reference.
   *
   * @implNote ConcurrentHashMap.get() provides O(1) average lookup. The allocate
   *           operation on AppPlacementAllocator decrements the pending ask count
   *           which is an O(1) operation. Write lock ensures atomic allocation
   *           preventing double-allocation of same request.
   *
   * @param type the node type (NODE_LOCAL, RACK_LOCAL, OFF_SWITCH)
   * @param node the scheduler node for allocation
   * @param schedulerKey the scheduler request key
   * @param containerAllocated the allocated container (may be null)
   * @return the container request that was satisfied
   *
   * Source: AppSchedulingInfo.java:612-626
   */
  public ContainerRequest allocate(NodeType type,
      SchedulerNode node, SchedulerRequestKey schedulerKey,
      RMContainer containerAllocated) {
    writeLock.lock();
    try {
      if (null != containerAllocated) {
        updateMetricsForAllocatedContainer(type, node, containerAllocated);
      }

      return schedulerKeyToAppPlacementAllocator.get(schedulerKey).allocate(
          schedulerKey, type, node);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Checks if the application should be deactivated (no pending requests).
   *
   * @complexity Time: O(1) for ConcurrentSkipListSet.isEmpty() check.
   *             Space: O(1) - no additional allocation.
   *
   * Source: AppSchedulingInfo.java:628-632
   */
  public void checkForDeactivation() {
    if (schedulerKeys.isEmpty()) {
      abstractUsersManager.deactivateApplication(user, applicationId);
    }
  }
  
  /**
   * Moves application's pending resources from old queue to new queue.
   *
   * @complexity Time: O(p) where p=number of pending requests (AppPlacementAllocators);
   *             iterates all allocators to transfer pending resource metrics.
   *             Space: O(1) - no additional allocation beyond temporary Resource objects.
   *
   * @param newQueue the queue to move the application to
   *
   * Source: AppSchedulingInfo.java:634-672
   */
  public void move(Queue newQueue) {
    this.writeLock.lock();
    try {
      QueueMetrics oldMetrics = queue.getMetrics();
      QueueMetrics newMetrics = newQueue.getMetrics();
      for (AppPlacementAllocator ap : schedulerKeyToAppPlacementAllocator
          .values()) {
        PendingAsk ask = ap.getPendingAsk(ResourceRequest.ANY);
        if (ask.getCount() > 0) {
          oldMetrics.decrPendingResources(
              ap.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());
          newMetrics.incrPendingResources(
              ap.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());

          Resource delta = Resources.multiply(ask.getPerAllocationResource(),
              ask.getCount());
          // Update Queue
          queue.decPendingResource(
              ap.getPrimaryRequestedNodePartition(), delta);
          newQueue.incPendingResource(
              ap.getPrimaryRequestedNodePartition(), delta);
        }
      }

      oldMetrics.moveAppFrom(this, isUnmanagedAM());
      newMetrics.moveAppTo(this, isUnmanagedAM());

      abstractUsersManager.deactivateApplication(user, applicationId);
      abstractUsersManager = newQueue.getAbstractUsersManager();
      if (!schedulerKeys.isEmpty()) {
        abstractUsersManager.activateApplication(user, applicationId);
      }
      this.queue = newQueue;
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * Stops the application and clears all pending resource metrics.
   *
   * @complexity Time: O(p) where p=number of pending requests (AppPlacementAllocators);
   *             iterates all allocators to clear metrics, then calls clearRequests()
   *             which is also O(p).
   *             Space: O(p) memory freed from cleared data structures.
   *
   * Source: AppSchedulingInfo.java:674-700
   */
  public void stop() {
    // clear pending resources metrics for the application
    this.writeLock.lock();
    try {
      QueueMetrics metrics = queue.getMetrics();
      for (AppPlacementAllocator ap : schedulerKeyToAppPlacementAllocator
          .values()) {
        PendingAsk ask = ap.getPendingAsk(ResourceRequest.ANY);
        if (ask.getCount() > 0) {
          metrics.decrPendingResources(ap.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());

          // Update Queue
          queue.decPendingResource(
              ap.getPrimaryRequestedNodePartition(),
              Resources.multiply(ask.getPerAllocationResource(),
                  ask.getCount()));
        }
      }

      metrics.finishAppAttempt(applicationId, pending, user, unmanagedAM);

      // Clear requests themselves
      clearRequests();
    } finally {
      this.writeLock.unlock();
    }
  }

  public void setQueue(Queue queue) {
    this.writeLock.lock();
    try {
      this.queue = queue;
    } finally {
      this.writeLock.unlock();
    }
  }

  private Set<String> getBlackList() {
    return this.placesBlacklistedByApp;
  }

  /**
   * Returns a copy of the application blacklist.
   *
   * @complexity Time: O(b) where b=blacklist size; HashSet copy constructor
   *             iterates all elements.
   *             Space: O(b) for the new HashSet allocation.
   *
   * @return copy of the application blacklist set
   *
   * Source: AppSchedulingInfo.java:716-720
   */
  public Set<String> getBlackListCopy() {
    synchronized (placesBlacklistedByApp) {
      return new HashSet<>(this.placesBlacklistedByApp);
    }
  }

  public void transferStateFromPreviousAppSchedulingInfo(
      AppSchedulingInfo appInfo) {
    // This should not require locking the placesBlacklistedByApp since it will
    // not be used by this instance until after setCurrentAppAttempt.
    this.placesBlacklistedByApp = appInfo.getBlackList();
  }

  public void recoverContainer(RMContainer rmContainer, String partition) {
    if (rmContainer.getExecutionType() != ExecutionType.GUARANTEED) {
      return;
    }
    this.writeLock.lock();
    try {
      QueueMetrics metrics = queue.getMetrics();
      if (pending) {
        // If there was any container to recover, the application was
        // running from scheduler's POV.
        pending = false;
        metrics.runAppAttempt(applicationId, user, isUnmanagedAM());
      }

      // Container is completed. Skip recovering resources.
      if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        return;
      }

      metrics.allocateResources(partition, user, 1,
          rmContainer.getAllocatedResource(), false);
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * In async environment, pending resource request could be updated during
   * scheduling, this method checks pending request before allocating.
   *
   * @complexity Time: O(1) for ConcurrentHashMap.get() lookup and
   *             canAllocate() check on AppPlacementAllocator.
   *             Space: O(1) - no additional allocation.
   *
   * @param type the node type (NODE_LOCAL, RACK_LOCAL, OFF_SWITCH)
   * @param node the scheduler node to check
   * @param schedulerKey the scheduler request key
   * @return true if allocation can proceed, false otherwise
   *
   * Source: AppSchedulingInfo.java:759-772
   */
  public boolean checkAllocation(NodeType type, SchedulerNode node,
      SchedulerRequestKey schedulerKey) {
    readLock.lock();
    try {
      AppPlacementAllocator ap = schedulerKeyToAppPlacementAllocator.get(
          schedulerKey);
      if (null == ap) {
        return false;
      }
      return ap.canAllocate(type, node);
    } finally {
      readLock.unlock();
    }
  }

  private void updateMetricsForAllocatedContainer(NodeType type,
      SchedulerNode node, RMContainer containerAllocated) {
    QueueMetrics metrics = queue.getMetrics();
    if (pending) {
      // once an allocation is done we assume the application is
      // running from scheduler's POV.
      pending = false;
      metrics.runAppAttempt(applicationId, user, isUnmanagedAM());
    }

    updateMetrics(applicationId, type, node, containerAllocated, user, queue);
  }

  public static void updateMetrics(ApplicationId applicationId, NodeType type,
      SchedulerNode node, RMContainer containerAllocated, String user,
      Queue queue) {
    LOG.debug("allocate: applicationId={} container={} host={} user={}"
        + " resource={} type={}", applicationId,
        containerAllocated.getContainer().getId(),
        containerAllocated.getNodeId(), user,
        containerAllocated.getContainer().getResource(),
        type);
    if(node != null) {
      queue.getMetrics().allocateResources(node.getPartition(), user, 1,
          containerAllocated.getContainer().getResource(), false);
      queue.getMetrics().decrPendingResources(
          containerAllocated.getNodeLabelExpression(), user, 1,
          containerAllocated.getContainer().getResource());
    }
    queue.getMetrics().incrNodeTypeAggregations(user, type);
    ClusterMetrics.getMetrics().incrNumContainerAssigned();
  }

  /**
   * Get AppPlacementAllocator by specified schedulerKey.
   *
   * @complexity Time: O(1) for ConcurrentHashMap.get() lookup.
   *             Space: O(1) - returns existing reference.
   *
   * @param schedulerkey the scheduler request key
   * @return the AppPlacementAllocator for the key, or null if not found
   *
   * Source: AppSchedulingInfo.java:807-812
   */
  public <N extends SchedulerNode> AppPlacementAllocator<N> getAppPlacementAllocator(
      SchedulerRequestKey schedulerkey) {
    return (AppPlacementAllocator<N>) schedulerKeyToAppPlacementAllocator.get(
        schedulerkey);
  }

  /**
   * Can delay to next?.
   *
   * @complexity Time: O(1) for ConcurrentHashMap.get() lookup and
   *             canDelayTo() check.
   *             Space: O(1) - no additional allocation.
   *
   * @param schedulerKey schedulerKey
   * @param resourceName resourceName
   *
   * @return If request exists, return {relaxLocality}
   *         Otherwise, return true.
   *
   * Source: AppSchedulingInfo.java:823-833
   */
  public boolean canDelayTo(
      SchedulerRequestKey schedulerKey, String resourceName) {
    this.readLock.lock();
    try {
      AppPlacementAllocator ap =
          schedulerKeyToAppPlacementAllocator.get(schedulerKey);
      return (ap == null) || ap.canDelayTo(resourceName);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Pre-check node to see if it satisfy the given schedulerKey and
   * scheduler mode.
   *
   * @complexity Time: O(1) for ConcurrentHashMap.get() lookup plus O(c)
   *             for precheckNode() constraint evaluation where c=number of
   *             placement constraints.
   *             Space: O(1) - no additional allocation beyond optional diagnostics.
   *
   * @param schedulerKey schedulerKey
   * @param schedulerNode schedulerNode
   * @param schedulingMode schedulingMode
   * @param dcOpt optional diagnostics collector
   * @return can use the node or not.
   *
   * Source: AppSchedulingInfo.java:845-857
   */
  public boolean precheckNode(SchedulerRequestKey schedulerKey,
      SchedulerNode schedulerNode, SchedulingMode schedulingMode,
      Optional<DiagnosticsCollector> dcOpt) {
    this.readLock.lock();
    try {
      AppPlacementAllocator ap =
          schedulerKeyToAppPlacementAllocator.get(schedulerKey);
      return (ap != null) && (ap.getPlacementAttempt() < retryAttempts) &&
          ap.precheckNode(schedulerNode, schedulingMode, dcOpt);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Get scheduling envs configured for this application.
   *
   * @return a map of applicationSchedulingEnvs
   */
  public Map<String, String> getApplicationSchedulingEnvs() {
    return applicationSchedulingEnvs;
  }

  /**
   * Get the defaultNodeLabelExpression for the application's current queue.
   *
   * @return defaultNodeLabelExpression
   */
  public String getDefaultNodeLabelExpression() {
    try {
      this.readLock.lock();
      return queue.getDefaultNodeLabelExpression();
    } finally {
      this.readLock.unlock();
    }
  }

  public RMContext getRMContext() {
    return this.rmContext;
  }
}
