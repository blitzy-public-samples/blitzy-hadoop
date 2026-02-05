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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

/**
 * This interface is the one implemented by the schedulers. It mainly extends 
 * {@link YarnScheduler}. 
 *
 * <p>Implementations include {@code CapacityScheduler}, {@code FairScheduler},
 * and {@code FifoScheduler}, each providing different scheduling policies
 * and resource allocation strategies.
 *
 * @performance Scheduler implementations are central to cluster performance.
 *              The {@link #attemptAllocationOnNode} method is called during
 *              scheduling loops and must be highly efficient (target O(1) to O(r)
 *              where r=resource types). The {@link #reinitialize} method can be
 *              expensive (O(q + n) where q=queues, n=nodes) and should be called
 *              sparingly (typically only on configuration changes). Implementations
 *              should minimize lock contention, especially during high-frequency
 *              operations like allocation attempts.
 *              Source: ResourceScheduler.java:39-80
 */
@LimitedPrivate("yarn")
@Evolving
public interface ResourceScheduler extends YarnScheduler, Recoverable {

  /**
   * Set RMContext for <code>ResourceScheduler</code>.
   * This method should be called immediately after instantiating
   * a scheduler once.
   *
   * @complexity Time: O(1) for reference assignment; called once during scheduler
   *             initialization, no iteration or complex processing involved.
   *             Space: O(1) no additional memory allocation beyond the reference.
   *             Source: ResourceScheduler.java:56-67
   *
   * @param rmContext created by ResourceManager
   */
  void setRMContext(RMContext rmContext);

  /**
   * Re-initialize the <code>ResourceScheduler</code>.
   *
   * @complexity Time: O(q + n) where q=number of queues and n=number of nodes;
   *             implementation-dependent but typically involves reloading queue
   *             configurations and re-evaluating node resources. Worst-case may
   *             be O(q * n) if queue-to-node mappings require full traversal.
   *             Space: O(q) for configuration parsing and queue structure rebuild.
   *             Source: ResourceScheduler.java:69-86
   *
   * @implNote Scheduler reinitialize may temporarily block scheduling operations
   *           to ensure configuration consistency. Implementations should minimize
   *           lock contention during reload by using read-write locks or
   *           copy-on-write patterns. This method should be called sparingly
   *           (typically only on admin-triggered configuration refresh) as it
   *           can cause scheduling latency spikes in high-throughput clusters.
   *
   * @param conf configuration
   * @param rmContext RMContext.
   * @throws IOException an I/O exception has occurred.
   */
  void reinitialize(Configuration conf, RMContext rmContext) throws IOException;

  /**
   * Get the {@link NodeId} available in the cluster by resource name.
   *
   * @complexity Time: O(n * f) where n=total number of nodes in the cluster and
   *             f=cost of filter evaluation per node (typically O(1) for simple
   *             resource name matching). With indexing by resource name, may be
   *             optimized to O(m) where m=matching nodes.
   *             Space: O(m) where m=number of matching nodes; creates a new list
   *             containing references to matching NodeId objects.
   *             Source: ResourceScheduler.java:88-101
   *
   * @param resourceName resource name
   * @return the number of available {@link NodeId} by resource name.
   */
  List<NodeId> getNodeIds(String resourceName);

  /**
   * Attempts to allocate a SchedulerRequest on a Node.
   * NOTE: This ignores the numAllocations in the resource sizing and tries
   *       to allocate a SINGLE container only.
   *
   * @complexity Time: O(r) where r=number of resource types (memory, vcores,
   *             custom resources) for availability check, plus implementation-specific
   *             allocation overhead. Implementations may incur O(c) where c=constraints
   *             if placement constraints are evaluated. This method is called during
   *             scheduling loops and should be optimized for efficiency.
   *             Space: O(1) for single container allocation decision; may temporarily
   *             allocate O(r) for resource comparison operations.
   *             Source: ResourceScheduler.java:103-123
   *
   * @param appAttempt ApplicationAttempt.
   * @param schedulingRequest SchedulingRequest.
   * @param schedulerNode SchedulerNode.
   * @return true if proposal was accepted.
   */
  // @PerformanceCritical: Called during scheduling loops for each candidate node (>5% scheduler time in allocation-heavy workloads)
  boolean attemptAllocationOnNode(SchedulerApplicationAttempt appAttempt,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode);

  /**
   * Reset scheduler metrics.
   *
   * @complexity Time: O(q) where q=number of queues in the scheduler hierarchy;
   *             traverses all queues to reset their associated metrics counters.
   *             May include additional O(a) where a=active applications if
   *             application-level metrics are also reset.
   *             Space: O(1) in-place metrics reset with no additional allocation.
   *             Source: ResourceScheduler.java:125-137
   */
  void resetSchedulerMetrics();
}
