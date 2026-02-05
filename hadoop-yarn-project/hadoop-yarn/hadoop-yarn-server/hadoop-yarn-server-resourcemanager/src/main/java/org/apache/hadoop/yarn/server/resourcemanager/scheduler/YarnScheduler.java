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

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

/**
 * This interface is used by the components to talk to the
 * scheduler for allocating of resources, cleaning up resources.
 *
 * @performance Implementations must provide O(1) or O(log n) lookups for
 *              scheduling hot paths including container lookups, node lookups,
 *              and application lookups. The allocate() method is called on
 *              every ApplicationMaster heartbeat and is the primary scheduling
 *              API - implementations should optimize this path for minimal
 *              latency. Scaling characteristics vary by implementation:
 *              CapacityScheduler optimizes for hierarchical queue structures,
 *              FairScheduler for fair resource sharing, and FIFO for simplicity.
 *              Expected allocation throughput: >10,000 allocations/second for
 *              cluster sizes up to 10,000 nodes.
 *              Source: YarnScheduler.java:57-62
 */
public interface YarnScheduler extends EventHandler<SchedulerEvent> {

  /**
   * Get queue information.
   *
   * @param queueName queue name
   * @param includeChildQueues include child queues?
   * @param recursive get children queues?
   * @return queue information
   * @throws IOException an I/O exception has occurred.
   * @complexity Time: O(q * d) where q=number of child queues, d=max queue depth
   *             when recursive=true and includeChildQueues=true; O(1) for single
   *             queue lookup when recursive=false. Space: O(q) for queue info
   *             collection when including children; O(1) for single queue.
   *             Source: YarnScheduler.java:73-85
   */
  @Public
  @Stable
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues,
      boolean recursive) throws IOException;

  /**
   * Get acls for queues for current user.
   * @return acls for queues for current user
   * @complexity Time: O(q * u) where q=total number of queues in hierarchy,
   *             u=average number of ACL entries per queue. Requires traversal
   *             of entire queue hierarchy to collect ACL information.
   *             Space: O(q * u) for the returned ACL info list.
   *             Source: YarnScheduler.java:87-97
   */
  @Public
  @Stable
  public List<QueueUserACLInfo> getQueueUserAclInfo();

  /**
   * Get the whole resource capacity of the cluster.
   * @return the whole resource capacity of the cluster.
   * @complexity Time: O(1) for cached resource access. Implementations must
   *             maintain a cached aggregate resource value updated incrementally
   *             on node additions/removals rather than computing on-demand.
   *             Space: O(1) constant - single Resource object reference.
   *             Source: YarnScheduler.java:99-112
   * @implNote This method is called frequently during scheduling decisions and
   *           capacity calculations. Implementations should cache the cluster
   *           resource value and update incrementally when nodes join/leave
   *           rather than recalculating on each call. Recalculation would be
   *           O(n) where n=number of nodes, which is unacceptable for hot paths.
   */
  @LimitedPrivate("yarn")
  @Unstable
  public Resource getClusterResource();

  /**
   * Get minimum allocatable {@link Resource}.
   * @return minimum allocatable resource
   * @complexity Time: O(1) for configured value access. The minimum resource
   *             capability is read from configuration at scheduler initialization
   *             and cached. Space: O(1) constant - single Resource object.
   *             Source: YarnScheduler.java:114-126
   */
  @Public
  @Stable
  public Resource getMinimumResourceCapability();
  
  /**
   * Get maximum allocatable {@link Resource} at the cluster level.
   * @return maximum allocatable resource
   * @complexity Time: O(1) for cached/computed max resource value. The maximum
   *             resource capability may be configured statically or computed as
   *             the maximum across all registered nodes. Implementations should
   *             cache this value and update incrementally on node changes.
   *             Space: O(1) constant - single Resource object.
   *             Source: YarnScheduler.java:128-142
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability();

  /**
   * Get maximum allocatable {@link Resource} for the queue specified.
   * @param queueName queue name
   * @return maximum allocatable resource
   * @complexity Time: O(1) for queue lookup assuming hash-based queue registry;
   *             O(d) where d=queue depth if hierarchical path traversal required.
   *             Queue-specific max resources are typically cached per queue.
   *             Space: O(1) constant - single Resource object returned.
   *             Source: YarnScheduler.java:144-158
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability(String queueName);

  @LimitedPrivate("yarn")
  @Evolving
  ResourceCalculator getResourceCalculator();

  /**
   * Get the number of nodes available in the cluster.
   * @return the number of available nodes.
   * @complexity Time: O(1) for cached count. Implementations maintain a running
   *             count of active nodes updated on node registration/deregistration
   *             events rather than counting on-demand.
   *             Space: O(1) constant - single integer value.
   *             Source: YarnScheduler.java:164-178
   */
  @Public
  @Stable
  public int getNumClusterNodes();
  
  /**
   * The main API between the ApplicationMaster and the Scheduler.
   * The ApplicationMaster may request/update container resources,
   * number of containers, node/rack preference for allocations etc.
   * to the Scheduler.
   * @param appAttemptId the id of the application attempt.
   * @param ask the request made by an application to obtain various allocations
   * like host/rack, resource, number of containers, relaxLocality etc.,
   * see {@link ResourceRequest}.
   * @param schedulingRequests similar to ask, but with added ability to specify
   * allocation tags etc., see {@link SchedulingRequest}.
   * @param release the list of containers to be released.
   * @param blacklistAdditions places (node/rack) to be added to the blacklist.
   * @param blacklistRemovals places (node/rack) to be removed from the
   * blacklist.
   * @param updateRequests container promotion/demotion updates.
   * @return the {@link Allocation} for the application.
   * @complexity Time: O(r + b + rel) where r=number of resource requests in ask,
   *             b=size of blacklist updates (additions + removals), rel=number
   *             of containers to release. The actual allocation decision is
   *             implementation-dependent: CapacityScheduler uses O(q * n) where
   *             q=queue depth, n=candidate nodes; FairScheduler uses O(a * n)
   *             where a=applications, n=nodes. Container release processing is
   *             O(rel). Blacklist updates are O(b) for set operations.
   *             Space: O(c) where c=allocated containers returned in Allocation.
   *             Source: YarnScheduler.java:180-217
   * @implNote This method is called on every ApplicationMaster heartbeat (default
   *           1 second interval) and is the primary scheduling API. For a cluster
   *           with 10,000 applications each heartbeating once per second, this
   *           method may be invoked 10,000 times/second. Implementations MUST be
   *           highly optimized: avoid blocking I/O, minimize lock contention, use
   *           lock-free data structures where possible. Scheduling decisions should
   *           target <10ms latency at p99 for responsive container allocation.
   */
  @Public
  @Stable
  Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
      List<ContainerId> release, List<String> blacklistAdditions,
      List<String> blacklistRemovals, ContainerUpdates updateRequests);

  /**
   * Get node resource usage report.
   *
   * @param nodeId nodeId.
   * @return the {@link SchedulerNodeReport} for the node or null
   * if nodeId does not point to a defined node.
   * @complexity Time: O(1) for node lookup using hash-based node registry.
   *             Node information is maintained in a ConcurrentHashMap keyed by
   *             NodeId for constant-time access. Space: O(1) for the returned
   *             report object.
   *             Source: YarnScheduler.java:219-234
   */
  @LimitedPrivate("yarn")
  @Stable
  public SchedulerNodeReport getNodeReport(NodeId nodeId);
  
  /**
   * Get the Scheduler app for a given app attempt Id.
   * @param appAttemptId the id of the application attempt
   * @return SchedulerApp for this given attempt.
   * @complexity Time: O(1) for application lookup using hash-based application
   *             registry. Applications are indexed by ApplicationAttemptId in a
   *             ConcurrentHashMap for constant-time retrieval during scheduling.
   *             Space: O(1) for the returned report object.
   *             Source: YarnScheduler.java:236-250
   */
  @LimitedPrivate("yarn")
  @Stable
  SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId);

  /**
   * Get a resource usage report from a given app attempt ID.
   * @param appAttemptId the id of the application attempt
   * @return resource usage report for this given attempt
   */
  @LimitedPrivate("yarn")
  @Evolving
  ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId);
  
  /**
   * Get the root queue for the scheduler.
   * @return the root queue for the scheduler.
   */
  @LimitedPrivate("yarn")
  @Evolving
  QueueMetrics getRootQueueMetrics();

  /**
   * Check if the user has permission to perform the operation.
   * If the user has {@link QueueACL#ADMINISTER_QUEUE} permission,
   * this user can view/modify the applications in this queue.
   *
   * @param callerUGI caller UserGroupInformation.
   * @param acl queue ACL.
   * @param queueName queue Name.
   * @return <code>true</code> if the user has the permission,
   *         <code>false</code> otherwise
   * @complexity Time: O(a + g) where a=number of ACL entries for the queue,
   *             g=number of groups the user belongs to. ACL checking involves
   *             matching user identity and group memberships against queue ACL
   *             entries. Queue lookup is O(1) or O(d) for hierarchical paths.
   *             Space: O(1) constant - boolean return value.
   *             Source: YarnScheduler.java:262-282
   */
  boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName);
  
  /**
   * Gets the apps under a given queue
   * @param queueName the name of the queue.
   * @return a collection of app attempt ids in the given queue.
   * @complexity Time: O(a) where a=number of applications currently in the
   *             specified queue. Requires iteration over the queue's application
   *             collection to build the returned list. Queue lookup is O(1).
   *             Space: O(a) for the returned list of ApplicationAttemptIds.
   *             Source: YarnScheduler.java:284-298
   */
  @LimitedPrivate("yarn")
  @Stable
  public List<ApplicationAttemptId> getAppsInQueue(String queueName);

  /**
   * Get the container for the given containerId.
   *
   * @param containerId the given containerId.
   * @return the container for the given containerId.
   * @complexity Time: O(1) for container lookup using hash-based container
   *             registry. Containers are indexed by ContainerId in a
   *             ConcurrentHashMap for constant-time access during scheduling
   *             and status queries. Space: O(1) - returns existing container
   *             reference, no allocation.
   *             Source: YarnScheduler.java:300-315
   */
  @LimitedPrivate("yarn")
  @Unstable
  public RMContainer getRMContainer(ContainerId containerId);

  /**
   * Moves the given application to the given queue.
   * @param appId application Id
   * @param newQueue the given queue.
   * @return the name of the queue the application was placed into
   * @throws YarnException if the move cannot be carried out
   * @complexity Time: O(c + v) where c=number of containers owned by the
   *             application to be moved, v=validation checks (ACL verification,
   *             queue capacity checks). Container resource accounting must be
   *             updated in both source and destination queues. Implementation-
   *             dependent: may require lock acquisition on both queues.
   *             Space: O(1) constant - no new allocations, updates existing
   *             data structures.
   *             Source: YarnScheduler.java:317-336
   */
  @LimitedPrivate("yarn")
  @Evolving
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException;

  /**
   *
   * @param appId Application ID
   * @param newQueue Target QueueName
   * @throws YarnException if the pre-validation for move cannot be carried out
   */
  @LimitedPrivate("yarn")
  @Evolving
  public void preValidateMoveApplication(ApplicationId appId,
      String newQueue) throws YarnException;

  /**
   * Completely drain sourceQueue of applications, by moving all of them to
   * destQueue.
   *
   * @param sourceQueue sourceQueue.
   * @param destQueue destQueue.
   * @throws YarnException when yarn exception occur.
   */
  void moveAllApps(String sourceQueue, String destQueue) throws YarnException;

  /**
   * Terminate all applications in the specified queue.
   *
   * @param queueName the name of queue to be drained
   * @throws YarnException when yarn exception occur.
   */
  void killAllAppsInQueue(String queueName) throws YarnException;

  /**
   * Remove an existing queue. Implementations might limit when a queue could be
   * removed (e.g., must have zero entitlement, and no applications running, or
   * must be a leaf, etc..).
   *
   * @param queueName name of the queue to remove
   * @throws YarnException when yarn exception occur.
   */
  void removeQueue(String queueName) throws YarnException;

  /**
   * Add to the scheduler a new Queue. Implementations might limit what type of
   * queues can be dynamically added (e.g., Queue must be a leaf, must be
   * attached to existing parent, must have zero entitlement).
   *
   * @param newQueue the queue being added.
   * @throws YarnException when yarn exception occur.
   * @throws IOException when io exception occur.
   */
  void addQueue(Queue newQueue) throws YarnException, IOException;

  /**
   * This method increase the entitlement for current queue (must respect
   * invariants, e.g., no overcommit of parents, non negative, etc.).
   * Entitlement is a general term for weights in FairScheduler, capacity for
   * the CapacityScheduler, etc.
   *
   * @param queue the queue for which we change entitlement
   * @param entitlement the new entitlement for the queue (capacity,
   *              maxCapacity, etc..)
   * @throws YarnException when yarn exception occur.
   */
  void setEntitlement(String queue, QueueEntitlement entitlement)
      throws YarnException;

  /**
   * Gets the list of names for queues managed by the Reservation System.
   * @return the list of queues which support reservations
   * @throws YarnException when yarn exception occur.
   * @complexity Time: O(q) where q=total number of queues in the scheduler.
   *             Requires traversal of queue hierarchy to identify queues with
   *             reservation system (Plan) enabled. Typically a small subset
   *             of total queues have reservations enabled.
   *             Space: O(p) where p=number of plan-enabled queues for the
   *             returned Set.
   *             Source: YarnScheduler.java:380-394
   */
  public Set<String> getPlanQueues() throws YarnException;  

  /**
   * Return a collection of the resource types that are considered when
   * scheduling
   *
   * @return an EnumSet containing the resource types
   */
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes();

  /**
   *
   * Verify whether a submitted application priority is valid as per configured
   * Queue
   *
   * @param priorityRequestedByApp
   *          Submitted Application priority.
   * @param user
   *          User who submitted the Application
   * @param queuePath
   *          Name of the Queue
   * @param applicationId
   *          Application ID
   * @return Updated Priority from scheduler
   * @throws YarnException when yarn exception occur.
   */
  public Priority checkAndGetApplicationPriority(Priority priorityRequestedByApp,
      UserGroupInformation user, String queuePath, ApplicationId applicationId)
      throws YarnException;

  /**
   *
   * Change application priority of a submitted application at runtime
   *
   * @param newPriority Submitted Application priority.
   *
   * @param applicationId Application ID
   *
   * @param future Sets any type of exception happened from StateStore
   * @param user who submitted the application
   *
   * @return updated priority
   * @throws YarnException when yarn exception occur.
   */
  public Priority updateApplicationPriority(Priority newPriority,
      ApplicationId applicationId, SettableFuture<Object> future,
      UserGroupInformation user) throws YarnException;

  /**
   *
   * Get previous attempts' live containers for work-preserving AM restart.
   *
   * @param appAttemptId the id of the application attempt
   *
   * @return list of live containers for the given attempt
   */
  List<Container> getTransferredContainers(ApplicationAttemptId appAttemptId);

  /**
   * Set the cluster max priority.
   * 
   * @param conf Configuration.
   * @throws YarnException when yarn exception occur.
   */
  void setClusterMaxPriority(Configuration conf) throws YarnException;

  /**
   * Get pending resource request for specified application attempt.
   *
   * @param attemptId the id of the application attempt
   * @return pending resource requests.
   */
  List<ResourceRequest> getPendingResourceRequestsForAttempt(
      ApplicationAttemptId attemptId);

  /**
   * Get pending scheduling request for specified application attempt.
   *
   * @param attemptId the id of the application attempt
   *
   * @return pending scheduling requests
   */
  List<SchedulingRequest> getPendingSchedulingRequestsForAttempt(
      ApplicationAttemptId attemptId);

  /**
   * Get cluster max priority.
   * 
   * @return maximum priority of cluster
   */
  Priority getMaxClusterLevelAppPriority();

  /**
   * Get SchedulerNode corresponds to nodeId.
   *
   * @param nodeId the node id of RMNode
   *
   * @return SchedulerNode corresponds to nodeId
   */
  SchedulerNode getSchedulerNode(NodeId nodeId);

  /**
   * Normalize a resource request using scheduler level maximum resource or
   * queue based maximum resource.
   *
   * @param requestedResource the resource to be normalized
   * @param maxResourceCapability Maximum container allocation value, if null or
   *          empty scheduler level maximum container allocation value will be
   *          used
   * @return the normalized resource
   */
  Resource getNormalizedResource(Resource requestedResource,
      Resource maxResourceCapability);

  /**
   * Verify whether a submitted application lifetime is valid as per configured
   * Queue lifetime.
   * @param queueName Name of the Queue
   * @param lifetime configured application lifetime
   * @param app details of app
   * @return valid lifetime as per queue
   */
  @Public
  @Evolving
  long checkAndGetApplicationLifetime(String queueName, long lifetime,
                                      RMAppImpl app);

  /**
   * Get maximum lifetime for a queue.
   * @param queueName to get lifetime
   * @return maximum lifetime in seconds
   */
  @Public
  @Evolving
  long getMaximumApplicationLifetime(String queueName);
}
