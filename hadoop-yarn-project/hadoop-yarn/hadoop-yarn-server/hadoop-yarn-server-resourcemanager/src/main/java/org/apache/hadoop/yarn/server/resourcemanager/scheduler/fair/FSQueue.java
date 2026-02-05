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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Abstract base class for Fair Scheduler queue implementations.
 * Provides common functionality for resource management, fair share computation,
 * and queue hierarchy traversal.
 *
 * @performance Base queue abstraction with O(1) cached fair share and resource usage access.
 *              Resource usage updates propagate O(d) through parent hierarchy where d=queue depth.
 *              Scheduling policy configurable per queue via policy field.
 *
 * @implNote Fair share values (fairShare, steadyFairShare) are precomputed and cached for O(1) access.
 *           Trade-off: Precompute vs on-demand - precomputation occurs at update cycle intervals
 *           (not per allocation) to amortize O(q × d) computation cost where q=number of queues
 *           and d=hierarchy depth. Cached values may be slightly stale between update cycles but
 *           avoids expensive recomputation on every access. Resource usage is maintained
 *           incrementally via incUsedResource/decUsedResource propagating to parent queues.
 *           Source: FSQueue.java:57-60
 */
@Private
@Unstable
public abstract class FSQueue implements Queue, Schedulable {
  private static final Logger LOG = LoggerFactory.getLogger(
      FSQueue.class.getName());

  private Resource fairShare = Resources.createResource(0, 0);
  private Resource steadyFairShare = Resources.createResource(0, 0);
  private Resource reservedResource = Resources.createResource(0, 0);
  private final Resource resourceUsage = Resource.newInstance(0, 0);
  private final String name;
  protected final FairScheduler scheduler;
  private final YarnAuthorizationProvider authorizer;
  private final PrivilegedEntity queueEntity;
  private final FSQueueMetrics metrics;
  
  protected final FSParentQueue parent;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  protected SchedulingPolicy policy = SchedulingPolicy.DEFAULT_POLICY;

  protected float weights;
  protected Resource minShare;
  private ConfigurableResource maxShare;
  protected int maxRunningApps;
  private ConfigurableResource maxChildQueueResource;

  // maxAMShare is a value between 0 and 1.
  protected float maxAMShare;

  private long fairSharePreemptionTimeout = Long.MAX_VALUE;
  private long minSharePreemptionTimeout = Long.MAX_VALUE;
  private float fairSharePreemptionThreshold = 0.5f;
  private boolean preemptable = true;
  private boolean isDynamic = true;
  protected Resource maxContainerAllocation;

  public FSQueue(String name, FairScheduler scheduler, FSParentQueue parent) {
    this.name = name;
    this.scheduler = scheduler;
    this.authorizer =
        YarnAuthorizationProvider.getInstance(scheduler.getConf());
    this.queueEntity = new PrivilegedEntity(EntityType.QUEUE, name);
    this.metrics = FSQueueMetrics.forQueue(getName(), parent, true, scheduler.getConf());
    this.parent = parent;
    setPolicy(scheduler.getAllocationConfiguration().getSchedulingPolicy(name));
    reinit(false);
  }

  /**
   * Initialize a queue by setting its queue-specific properties and its
   * metrics. This method is invoked when creating a new queue or reloading
   * the allocation file.
   * This method does not set policies for queues when reloading the allocation
   * file since we need to either set all new policies or nothing, which is
   * handled by method {@link #verifyAndSetPolicyFromConf}.
   *
   * @param recursive whether child queues should be reinitialized recursively
   * @complexity Time: O(1) when recursive=false; O(c × d) when recursive=true,
   *             where c=total child queues in subtree, d=hierarchy depth for
   *             deepest child. Each queue reinit performs O(1) property initialization.
   *             Space: O(d) for recursive call stack when recursive=true; O(1) otherwise.
   *             Source: FSQueue.java:111-121
   */
  public final void reinit(boolean recursive) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    allocConf.initFSQueue(this);
    updatePreemptionVariables();

    if (recursive) {
      for (FSQueue child : getChildQueues()) {
        child.reinit(recursive);
      }
    }
  }

  public String getName() {
    return name;
  }

  @Override
  public String getQueueName() {
    return name;
  }

  public SchedulingPolicy getPolicy() {
    return policy;
  }

  public FSParentQueue getParent() {
    return parent;
  }

  public void setPolicy(SchedulingPolicy policy) {
    policy.initialize(scheduler.getContext());
    this.policy = policy;
  }

  public void setWeights(float weights) {
    this.weights = weights;
  }

  @Override
  public float getWeight() {
    return weights;
  }

  public void setMinShare(Resource minShare){
    this.minShare = minShare;
  }

  @Override
  public Resource getMinShare() {
    return minShare;
  }

  public void setMaxShare(ConfigurableResource maxShare){
    this.maxShare = maxShare;
  }

  public void setMaxContainerAllocation(Resource maxContainerAllocation){
    this.maxContainerAllocation = maxContainerAllocation;
  }

  public abstract Resource getMaximumContainerAllocation();

  @Override
  public Resource getMaxShare() {
    Resource maxResource = maxShare.getResource(scheduler.getClusterResource());

    // Max resource should be greater than or equal to min resource
    Resource result = Resources.componentwiseMax(maxResource, minShare);

    if (!Resources.equals(maxResource, result)) {
      LOG.warn(String.format("Queue %s has max resources %s less than "
          + "min resources %s", getName(), maxResource, minShare));
    }
    return result;
  }

  public ConfigurableResource getRawMaxShare() {
    return maxShare;
  }

  public Resource getReservedResource() {
    reservedResource.setMemorySize(metrics.getReservedMB());
    reservedResource.setVirtualCores(metrics.getReservedVirtualCores());
    return reservedResource;
  }

  public void setMaxChildQueueResource(ConfigurableResource maxChildShare){
    this.maxChildQueueResource = maxChildShare;
  }

  public ConfigurableResource getMaxChildQueueResource() {
    return maxChildQueueResource;
  }

  public void setMaxRunningApps(int maxRunningApps){
    this.maxRunningApps = maxRunningApps;
  }

  public int getMaxRunningApps() {
    return maxRunningApps;
  }

  @VisibleForTesting
  public float getMaxAMShare() {
    return maxAMShare;
  }

  public void setMaxAMShare(float maxAMShare){
    this.maxAMShare = maxAMShare;
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public Priority getPriority() {
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }
  
  @Override
  public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setSchedulerType("FairScheduler");
    queueInfo.setQueueName(getQueueName());

    if (scheduler.getClusterResource().getMemorySize() == 0) {
      queueInfo.setCapacity(0.0f);
    } else {
      queueInfo.setCapacity((float) getFairShare().getMemorySize() /
          scheduler.getClusterResource().getMemorySize());
    }

    if (getFairShare().getMemorySize() == 0) {
      queueInfo.setCurrentCapacity(0.0f);
    } else {
      queueInfo.setCurrentCapacity((float) getResourceUsage().getMemorySize() /
          getFairShare().getMemorySize());
    }

    // set Weight
    queueInfo.setWeight(getWeight());

    // set MinShareResource
    Resource minShareResource = getMinShare();
    queueInfo.setMinResourceVCore(minShareResource.getVirtualCores());
    queueInfo.setMinResourceMemory(minShareResource.getMemorySize());

    // set MaxShareResource
    Resource maxShareResource =
        Resources.componentwiseMin(getMaxShare(), scheduler.getClusterResource());
    queueInfo.setMaxResourceVCore(maxShareResource.getVirtualCores());
    queueInfo.setMaxResourceMemory(maxShareResource.getMemorySize());

    // set ReservedResource
    Resource newReservedResource = getReservedResource();
    queueInfo.setReservedResourceVCore(newReservedResource.getVirtualCores());
    queueInfo.setReservedResourceMemory(newReservedResource.getMemorySize());

    // set SteadyFairShare
    Resource newSteadyFairShare = getSteadyFairShare();
    queueInfo.setSteadyFairShareVCore(newSteadyFairShare.getVirtualCores());
    queueInfo.setSteadyFairShareMemory(newSteadyFairShare.getMemorySize());

    // set MaxRunningApp
    queueInfo.setMaxRunningApp(getMaxRunningApps());

    // set Preemption
    queueInfo.setPreemptionDisabled(isPreemptable());

    ArrayList<QueueInfo> childQueueInfos = new ArrayList<>();
    if (includeChildQueues) {
      Collection<FSQueue> childQueues = getChildQueues();
      for (FSQueue child : childQueues) {
        childQueueInfos.add(child.getQueueInfo(recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueueInfos);
    queueInfo.setQueueState(QueueState.RUNNING);
    queueInfo.setQueueStatistics(getQueueStatistics());
    return queueInfo;
  }

  public QueueStatistics getQueueStatistics() {
    QueueStatistics stats =
        recordFactory.newRecordInstance(QueueStatistics.class);
    stats.setNumAppsSubmitted(getMetrics().getAppsSubmitted());
    stats.setNumAppsRunning(getMetrics().getAppsRunning());
    stats.setNumAppsPending(getMetrics().getAppsPending());
    stats.setNumAppsCompleted(getMetrics().getAppsCompleted());
    stats.setNumAppsKilled(getMetrics().getAppsKilled());
    stats.setNumAppsFailed(getMetrics().getAppsFailed());
    stats.setNumActiveUsers(getMetrics().getActiveUsers());
    stats.setAvailableMemoryMB(getMetrics().getAvailableMB());
    stats.setAllocatedMemoryMB(getMetrics().getAllocatedMB());
    stats.setPendingMemoryMB(getMetrics().getPendingMB());
    stats.setReservedMemoryMB(getMetrics().getReservedMB());
    stats.setAvailableVCores(getMetrics().getAvailableVirtualCores());
    stats.setAllocatedVCores(getMetrics().getAllocatedVirtualCores());
    stats.setPendingVCores(getMetrics().getPendingVirtualCores());
    stats.setReservedVCores(getMetrics().getReservedVirtualCores());
    stats.setAllocatedContainers(getMetrics().getAllocatedContainers());
    stats.setPendingContainers(getMetrics().getPendingContainers());
    stats.setReservedContainers(getMetrics().getReservedContainers());
    return stats;
  }
  
  @Override
  public FSQueueMetrics getMetrics() {
    return metrics;
  }

  /**
   * Get the fair share assigned to this Schedulable.
   *
   * @return the fair share assigned to this Schedulable.
   * @complexity Time: O(1) for precomputed cached value access.
   *             Space: O(1).
   *             Source: FSQueue.java:327-329
   */
  public Resource getFairShare() {
    return fairShare;
  }

  /**
   * Assign a fair share to this Schedulable.
   *
   * @param fairShare the fair share to assign.
   * @complexity Time: O(1) for value assignment and metrics update.
   *             Space: O(1).
   *             Source: FSQueue.java:332-336
   */
  @Override
  public void setFairShare(Resource fairShare) {
    this.fairShare = fairShare;
    metrics.setFairShare(fairShare);
    LOG.debug("The updated fairShare for {} is {}", getName(), fairShare);
  }

  /**
   * Get the steady fair share assigned to this Schedulable.
   *
   * @return the steady fair share assigned to this Schedulable.
   * @complexity Time: O(1) for precomputed cached value access.
   *             Space: O(1).
   *             Source: FSQueue.java:342-344
   */
  public Resource getSteadyFairShare() {
    return steadyFairShare;
  }

  void setSteadyFairShare(Resource steadyFairShare) {
    this.steadyFairShare = steadyFairShare;
    metrics.setSteadyFairShare(steadyFairShare);
  }

  /**
   * Check if the user has access to this queue with the specified ACL.
   *
   * @param acl the queue ACL to check.
   * @param user the user to check access for.
   * @return true if user has access, false otherwise.
   * @complexity Time: O(a) where a=number of ACL entries for authorization check.
   *             The YarnAuthorizationProvider iterates through configured ACL rules
   *             to determine access permission.
   *             Space: O(1) for AccessRequest creation.
   *             Source: FSQueue.java:351-356
   */
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return authorizer.checkPermission(
        new AccessRequest(queueEntity, user,
            SchedulerUtils.toAccessType(acl), null, null,
            Server.getRemoteAddress(), null));
  }

  long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  void setFairSharePreemptionTimeout(long fairSharePreemptionTimeout) {
    this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
  }

  long getMinSharePreemptionTimeout() {
    return minSharePreemptionTimeout;
  }

  void setMinSharePreemptionTimeout(long minSharePreemptionTimeout) {
    this.minSharePreemptionTimeout = minSharePreemptionTimeout;
  }

  float getFairSharePreemptionThreshold() {
    return fairSharePreemptionThreshold;
  }

  void setFairSharePreemptionThreshold(float fairSharePreemptionThreshold) {
    this.fairSharePreemptionThreshold = fairSharePreemptionThreshold;
  }

  @Override
  public boolean isPreemptable() {
    return preemptable;
  }

  /**
   * Recomputes the shares for all child queues and applications based on this
   * queue's current share.
   *
   * To be called holding the scheduler writelock.
   */
  abstract void updateInternal();

  /**
   * Set the queue's fairshare and update the demand/fairshare of child
   * queues/applications.
   *
   * To be called holding the scheduler writelock.
   *
   * @param fairShare queue's fairshare.
   * @complexity Time: O(subclass) - delegates to updateInternal() which varies by queue type.
   *             FSLeafQueue: O(a log a) where a=applications for sorting and fair share distribution.
   *             FSParentQueue: O(c × d × a) where c=child queues, d=hierarchy depth, a=apps per leaf.
   *             Space: O(1) for setFairShare; O(subclass) for updateInternal.
   *             Source: FSQueue.java:403-406
   */
  public void update(Resource fairShare) {
    setFairShare(fairShare);
    updateInternal();
  }

  /**
   * Update the min/fair share preemption timeouts, threshold and preemption
   * disabled flag for this queue.
   */
  private void updatePreemptionVariables() {
    // For min share timeout
    minSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getMinSharePreemptionTimeout(getName());
    if (minSharePreemptionTimeout == -1 && parent != null) {
      minSharePreemptionTimeout = parent.getMinSharePreemptionTimeout();
    }
    // For fair share timeout
    fairSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionTimeout(getName());
    if (fairSharePreemptionTimeout == -1 && parent != null) {
      fairSharePreemptionTimeout = parent.getFairSharePreemptionTimeout();
    }
    // For fair share preemption threshold
    fairSharePreemptionThreshold = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionThreshold(getName());
    if (fairSharePreemptionThreshold < 0 && parent != null) {
      fairSharePreemptionThreshold = parent.getFairSharePreemptionThreshold();
    }
    // For option whether allow preemption from this queue.
    // If the parent is non-preemptable, this queue is non-preemptable as well,
    // otherwise get the value from the allocation file.
    if (parent != null && !parent.isPreemptable()) {
      preemptable = false;
    } else {
      preemptable = scheduler.getAllocationConfiguration()
          .isPreemptable(getName());
    }
  }

  /**
   * Gets the children of this queue, if any.
   *
   * @return the children of this queue.
   */
  public abstract List<FSQueue> getChildQueues();
  
  /**
   * Adds all applications in the queue and its subqueues to the given collection.
   * @param apps the collection to add the applications to
   */
  public abstract void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps);
  
  /**
   * Return the number of apps for which containers can be allocated.
   * Includes apps in subqueues.
   *
   * @return the number of apps.
   */
  public abstract int getNumRunnableApps();
  
  /**
   * Helper method to check if the queue should attempt assigning resources.
   *
   * @param node the scheduler node to check.
   * @return true if check passes (can assign) or false otherwise.
   * @complexity Time: O(r) where r=number of resource types for Resource comparison
   *             via Resources.fitsIn(). Checks reserved container (O(1)) and compares
   *             resource usage against max share dimension-by-dimension.
   *             Space: O(1) - no additional allocations.
   *             Source: FSQueue.java:469-495
   */
  boolean assignContainerPreCheck(FSSchedulerNode node) {
    if (node.getReservedContainer() != null) {
      LOG.debug("Assigning container failed on node '{}' because it has"
          + " reserved containers.", node.getNodeName());
      return false;
    } else if (!Resources.fitsIn(getResourceUsage(), getMaxShare())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Assigning container failed on node '" + node.getNodeName()
            + " because queue resource usage is larger than MaxShare: "
            + dumpState());
      }
      return false;
    } else {
      return true;
    }
  }

  /**
   * Returns true if queue has at least one app running.
   *
   * @return true, if queue has at least one app running; otherwise, false;
   */
  public boolean isActive() {
    return getNumRunnableApps() > 0;
  }

  /** Convenient toString implementation for debugging. */
  @Override
  public String toString() {
    return String.format("[%s, demand=%s, running=%s, share=%s, w=%s]",
        getName(), getDemand(), getResourceUsage(), fairShare, getWeight());
  }
  
  @Override
  public Set<String> getAccessibleNodeLabels() {
    // TODO, add implementation for FS
    return null;
  }
  
  @Override
  public String getDefaultNodeLabelExpression() {
    // TODO, add implementation for FS
    return null;
  }
  
  @Override
  public void incPendingResource(String nodeLabel, Resource resourceToInc) {
  }
  
  @Override
  public void decPendingResource(String nodeLabel, Resource resourceToDec) {
  }

  @Override
  public void incReservedResource(String nodeLabel, Resource resourceToInc) {
  }

  @Override
  public void decReservedResource(String nodeLabel, Resource resourceToDec) {
  }

  /**
   * Get the aggregate amount of resources consumed by this schedulable.
   *
   * @return aggregate amount of resources used.
   * @complexity Time: O(1) for cached usage access.
   *             Space: O(1).
   *             Note: Resource usage is maintained incrementally via incUsedResource/decUsedResource
   *             rather than computed on-demand, enabling constant-time access.
   *             Source: FSQueue.java:531-533
   */
  @Override
  public Resource getResourceUsage() {
    return resourceUsage;
  }

  /**
   * Increase resource usage for this queue and all parent queues.
   *
   * @param res the resource to increase.
   * @complexity Time: O(d) where d=queue hierarchy depth for parent chain traversal.
   *             Each level performs O(1) synchronized addTo operation.
   *             Space: O(1) per call; O(d) total stack depth for parent propagation.
   *             Note: synchronized block ensures thread-safe resource accounting across
   *             concurrent container allocation/release operations.
   *             Source: FSQueue.java:540-547
   */
  public void incUsedResource(Resource res) {
    synchronized (resourceUsage) {
      Resources.addTo(resourceUsage, res);
      if (parent != null) {
        parent.incUsedResource(res);
      }
    }
  }

  /**
   * Decrease resource usage for this queue and all parent queues.
   *
   * @param res the resource to decrease.
   * @complexity Time: O(d) where d=queue hierarchy depth for parent chain traversal.
   *             Each level performs O(1) synchronized subtractFrom operation.
   *             Space: O(1) per call; O(d) total stack depth for parent propagation.
   *             Note: synchronized block ensures thread-safe resource accounting across
   *             concurrent container allocation/release operations.
   *             Source: FSQueue.java:554-561
   */
  protected void decUsedResource(Resource res) {
    synchronized (resourceUsage) {
      Resources.subtractFrom(resourceUsage, res);
      if (parent != null) {
        parent.decUsedResource(res);
      }
    }
  }

  @Override
  public Priority getDefaultApplicationPriority() {
    // TODO add implementation for FSParentQueue
    return null;
  }

  /**
   * Check if the additional resource fits within this queue's and all parent
   * queues' max share limits.
   *
   * @param additionalResource the additional resource to check.
   * @return true if the resource fits within all max share constraints.
   * @complexity Time: O(d × r) where d=hierarchy depth for parent traversal,
   *             r=number of resource types for Resource comparison.
   *             Each level performs O(r) resource addition and comparison operations.
   *             Space: O(d) for recursive call stack traversing parent chain.
   *             Source: FSQueue.java:569-587
   */
  boolean fitsInMaxShare(Resource additionalResource) {
    Resource usagePlusAddition =
        Resources.add(getResourceUsage(), additionalResource);

    if (!Resources.fitsIn(usagePlusAddition, getMaxShare())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resource usage plus resource request: " + usagePlusAddition
            + " exceeds maximum resource allowed:" + getMaxShare()
            + " in queue " + getName());
      }
      return false;
    }

    FSQueue parentQueue = getParent();
    if (parentQueue != null) {
      return parentQueue.fitsInMaxShare(additionalResource);
    }
    return true;
  }

  /**
   * Recursively check policies for queues in pre-order. Get queue policies
   * from the allocation file instead of properties of {@link FSQueue} objects.
   * Set the policy for current queue if there is no policy violation for its
   * children. This method is invoked while reloading the allocation file.
   *
   * @param queueConf allocation configuration
   * @return true if no policy violation and successfully set polices
   *         for queues; false otherwise
   */
  public boolean verifyAndSetPolicyFromConf(AllocationConfiguration queueConf) {
    SchedulingPolicy queuePolicy = queueConf.getSchedulingPolicy(getName());

    for (FSQueue child : getChildQueues()) {
      if (!queuePolicy.isChildPolicyAllowed(
          queueConf.getSchedulingPolicy(child.getName()))) {
        return false;
      }
      boolean success = child.verifyAndSetPolicyFromConf(queueConf);
      if (!success) {
        return false;
      }
    }

    // Set the policy if no policy violation for all children
    setPolicy(queuePolicy);
    return true;
  }

  /**
   * Recursively dump states of all queues.
   *
   * @return a string which holds all queue states
   */
  public String dumpState() {
    StringBuilder sb = new StringBuilder();
    dumpStateInternal(sb);
    return sb.toString();
  }


  /**
   * Recursively dump states of all queues.
   *
   * @param sb the {code StringBuilder} which holds queue states
   */
  protected abstract void dumpStateInternal(StringBuilder sb);

  public boolean isDynamic() {
    return isDynamic;
  }

  public void setDynamic(boolean dynamic) {
    this.isDynamic = dynamic;
  }

  public abstract boolean isEmpty();
}
