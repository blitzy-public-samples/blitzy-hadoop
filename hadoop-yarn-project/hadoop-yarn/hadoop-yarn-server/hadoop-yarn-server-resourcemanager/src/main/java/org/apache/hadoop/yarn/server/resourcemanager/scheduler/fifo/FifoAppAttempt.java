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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;


import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

/**
 * Scheduler-side representation of an application attempt for the FIFO scheduler.
 * Extends FiCaSchedulerApp to provide FIFO-specific container allocation behavior.
 * 
 * @performance Container tracking operations are O(c) where c=live containers.
 *              Simple FIFO ordering requires no fair share calculations - O(1) priority comparisons
 *              compared to FairScheduler's O(log a) weighted fair share computations.
 * @implNote Minimal extension of FiCaSchedulerApp - relies on parent class for most attempt
 *           tracking. FIFO ordering is maintained by FifoScheduler's ConcurrentSkipListMap,
 *           not by per-attempt logic.
 */
public class FifoAppAttempt extends FiCaSchedulerApp {
  private static final Logger LOG =
      LoggerFactory.getLogger(FifoAppAttempt.class);

  /**
   * Constructs a new FIFO application attempt.
   * 
   * @param appAttemptId the application attempt ID
   * @param user the submitting user
   * @param queue the queue (always DEFAULT_QUEUE for FIFO)
   * @param activeUsersManager manager for tracking active users
   * @param rmContext the RM context
   * @complexity Time: O(1) for direct field initialization via super constructor.
   *             Space: O(1) fixed overhead per attempt plus parent class allocations.
   */
  FifoAppAttempt(ApplicationAttemptId appAttemptId, String user,
      Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    super(appAttemptId, user, queue, activeUsersManager, rmContext);
  }

  /**
   * Allocates a container to this application attempt on the specified node.
   * 
   * @param type the locality type (NODE_LOCAL, RACK_LOCAL, OFF_SWITCH)
   * @param node the scheduler node for allocation
   * @param schedulerKey the scheduler request key
   * @param container the container being allocated
   * @return RMContainer if allocation succeeded, null otherwise
   * 
   * @complexity Time: O(1) amortized for container allocation:
   *             - O(1) for isStopped check
   *             - O(1) for getOutstandingAsksCount lookup (ConcurrentHashMap)
   *             - O(1) for RMContainerImpl construction
   *             - O(1) for liveContainers.put (ConcurrentHashMap)
   *             - O(1) for appSchedulingInfo.allocate
   *             - O(1) for attemptResourceUsage.incUsed
   *             Space: O(1) per container - creates RMContainerImpl object
   * 
   * @implNote Simple FIFO allocation with no fair share calculations or queue hierarchy traversal.
   *           Contrast with FairScheduler which requires O(log a) weighted fair share computation
   *           and CapacityScheduler which requires O(q × d) queue capacity checks.
   *           Uses writeLock for thread-safety during allocation - ensures atomic container assignment.
   * 
   * @PerformanceCritical: Called for every container allocation in the FIFO scheduler.
   *                       Hot path during scheduling cycles (~1-10s per node heartbeat).
   */
  public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, Container container) {

    writeLock.lock();
    try {
      // @PerformanceCritical: Container allocation hot path - executed for every container assignment.
      // O(1) operations: state check, outstanding ask lookup, RMContainer creation, map insertion.
      if (isStopped) {
        return null;
      }

      // Required sanity check - AM can call 'allocate' to update resource
      // request without locking the scheduler, hence we need to check
      if (getOutstandingAsksCount(schedulerKey) <= 0) {
        return null;
      }

      // @implNote: RMContainerImpl creation is O(1) - simple field initialization.
      // Container tracking via liveContainers (ConcurrentHashMap) provides O(1) put/get.
      // appSchedulingInfo.allocate updates pending request counts in O(1) amortized.
      // Create RMContainer
      RMContainer rmContainer = new RMContainerImpl(container,
          schedulerKey, this.getApplicationAttemptId(), node.getNodeID(),
          appSchedulingInfo.getUser(), this.rmContext, node.getPartition());
      ((RMContainerImpl) rmContainer).setQueueName(this.getQueueName());

      updateAMContainerDiagnostics(AMState.ASSIGNED, null);

      // Add it to allContainers list.
      addToNewlyAllocatedContainers(node, rmContainer);

      ContainerId containerId = container.getId();
      liveContainers.put(containerId, rmContainer);

      // Update consumption and track allocations
      ContainerRequest containerRequest = appSchedulingInfo.allocate(
            type, node, schedulerKey, rmContainer);

      attemptResourceUsage.incUsed(node.getPartition(),
          container.getResource());

      // Update resource requests related to "request" and store in RMContainer
      ((RMContainerImpl) rmContainer).setContainerRequest(containerRequest);

      // Inform the container
      rmContainer.handle(
          new RMContainerEvent(containerId, RMContainerEventType.START));

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: applicationAttemptId=" + containerId
            .getApplicationAttemptId() + " container=" + containerId + " host="
            + container.getNodeId().getHost() + " type=" + type);
      }
      // In order to save space in the audit log, only include the partition
      // if it is not the default partition.
      String partition = null;
      if (appAMNodePartitionName != null &&
            !appAMNodePartitionName.isEmpty()) {
        partition = appAMNodePartitionName;
      }
      RMAuditLogger.logSuccess(getUser(),
          RMAuditLogger.AuditConstants.ALLOC_CONTAINER, "SchedulerApp",
          getApplicationId(), containerId, container.getResource(),
          getQueueName(), partition);

      return rmContainer;
    } finally {
      writeLock.unlock();
    }
  }
}
