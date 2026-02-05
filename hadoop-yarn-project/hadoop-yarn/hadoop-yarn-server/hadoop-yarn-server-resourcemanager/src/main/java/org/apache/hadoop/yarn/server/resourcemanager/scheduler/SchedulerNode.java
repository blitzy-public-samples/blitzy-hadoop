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
import java.util.Collections;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;


/**
 * Represents a YARN Cluster Node from the viewpoint of the scheduler.
 *
 * @performance Memory footprint: O(c) where c=number of allocated containers per node.
 *              Node state operations scale with container count. Container tracking uses
 *              HashMap (launchedContainers) providing O(1) average-case lookup/insert/delete.
 *              Resource tracking maintains pre-computed unallocatedResource for O(1)
 *              availability checks during scheduling decisions.
 *              Source: SchedulerNode.java:79-80, 66-68
 * @implNote Uses HashMap for launchedContainers to provide O(1) average-case container
 *           lookup instead of O(n) list scan. This trade-off accepts higher memory overhead
 *           (HashMap entry objects) for faster container access during high-frequency
 *           scheduling operations. Volatile numContainers provides thread-safe reads without
 *           synchronization overhead for container count queries. Resource objects
 *           (unallocatedResource, allocatedResource, totalResource) are maintained
 *           in-memory rather than querying RM state to avoid RPC latency during
 *           scheduling hot paths.
 */
@Private
@Unstable
public abstract class SchedulerNode {

  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulerNode.class);

  private Resource unallocatedResource = Resource.newInstance(0, 0);
  private Resource allocatedResource = Resource.newInstance(0, 0);
  private Resource totalResource;
  private RMContainer reservedContainer;
  private volatile int numContainers;
  private volatile ResourceUtilization containersUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);
  private volatile ResourceUtilization nodeUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);
  /** Time stamp for overcommitted resources to time out. */
  private long overcommitTimeout = -1;

  /* set of containers that are allocated containers */
  private final Map<ContainerId, ContainerInfo> launchedContainers =
      new HashMap<>();

  private final RMNode rmNode;
  private final String nodeName;
  private final RMContext rmContext;

  private volatile Set<String> labels = null;

  private volatile Set<NodeAttribute> nodeAttributes = null;

  // Last updated time
  private volatile long lastHeartbeatMonotonicTime;

  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> labels) {
    this.rmNode = node;
    this.rmContext = node.getRMContext();
    this.unallocatedResource = Resources.clone(node.getTotalCapability());
    this.totalResource = Resources.clone(node.getTotalCapability());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
    this.labels = ImmutableSet.copyOf(labels);
    this.lastHeartbeatMonotonicTime = Time.monotonicNow();
  }

  public SchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET);
  }

  public RMNode getRMNode() {
    return this.rmNode;
  }

  /**
   * Set total resources on the node.
   *
   * @complexity Time: O(1) for field assignment and Resource arithmetic operations.
   *             Space: O(1) for new Resource object creation in subtract operation.
   *             Source: SchedulerNode.java:120-124
   * @implNote Recalculates unallocatedResource as (totalResource - allocatedResource)
   *           rather than incrementally adjusting. Ensures consistency after dynamic
   *           resource updates at cost of object allocation.
   * @param resource Total resources on the node.
   */
  public synchronized void updateTotalResource(Resource resource){
    this.totalResource = resource;
    this.unallocatedResource = Resources.subtract(totalResource,
        this.allocatedResource);
  }

  /**
   * Set the timeout for the node to stop overcommitting the resources. After
   * this time the scheduler will start killing containers until the resources
   * are not overcommitted anymore. This may reset a previous timeout.
   * @param timeOut Time out in milliseconds.
   */
  public synchronized void setOvercommitTimeOut(long timeOut) {
    if (timeOut >= 0) {
      if (this.overcommitTimeout != -1) {
        LOG.debug("The overcommit timeout for {} was already set to {}",
            getNodeID(), this.overcommitTimeout);
      }
      this.overcommitTimeout = Time.now() + timeOut;
    }
  }

  /**
   * Check if the time out has passed.
   * @return If the node is overcommitted.
   */
  public synchronized boolean isOvercommitTimedOut() {
    return this.overcommitTimeout >= 0 && Time.now() >= this.overcommitTimeout;
  }

  /**
   * Check if the node has a time out for overcommit resources.
   * @return If the node has a time out for overcommit resources.
   */
  public synchronized boolean isOvercommitTimeOutSet() {
    return this.overcommitTimeout >= 0;
  }

  /**
   * Get the ID of the node which contains both its hostname and port.
   * @return The ID of the node.
   */
  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  /**
   * Get HTTP address for the node.
   * @return HTTP address for the node.
   */
  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  /**
   * Get the name of the node for scheduling matching decisions.
   * <p>
   * Typically this is the 'hostname' reported by the node, but it could be
   * configured to be 'hostname:port' reported by the node via the
   * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
   * The main usecase of this is YARN minicluster to be able to differentiate
   * node manager instances by their port number.
   * @return Name of the node for scheduling matching decisions.
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Get rackname.
   * @return rackname
   */
  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   *
   * @complexity Time: O(1) average-case for HashMap put operation on launchedContainers;
   *             O(n) worst-case if hash collision triggers rehashing where n=container count.
   *             Space: O(1) per container (ContainerInfo object allocation).
   *             Source: SchedulerNode.java:219-220
   * @param rmContainer Allocated container
   */
  public void allocateContainer(RMContainer rmContainer) {
    allocateContainer(rmContainer, false);
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   *
   * @complexity Time: O(1) average-case for HashMap put operation; includes O(1) resource
   *             arithmetic operations (Resources.subtractFrom, Resources.addTo).
   *             O(n) worst-case if rehashing occurs where n=container count.
   *             Space: O(1) per container - allocates single ContainerInfo wrapper object.
   *             Source: SchedulerNode.java:219-220
   * @implNote Guaranteed containers trigger resource deduction and counter increment;
   *           opportunistic containers are tracked but do not affect resource accounting.
   *           HashMap provides O(1) average container insertion vs O(n) list append with
   *           lookup benefits.
   * @param rmContainer Allocated container
   * @param launchedOnNode True if the container has been launched
   */
  protected synchronized void allocateContainer(RMContainer rmContainer,
      boolean launchedOnNode) {
    Container container = rmContainer.getContainer();
    if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
      deductUnallocatedResource(container.getResource());
      ++numContainers;
    }

    launchedContainers.put(container.getId(),
        new ContainerInfo(rmContainer, launchedOnNode));
  }

  /**
   * Get unallocated resources on the node.
   *
   * @complexity Time: O(1) for direct field access to cached resource object.
   *             Space: O(1) - returns reference to existing Resource object, no allocation.
   *             Source: SchedulerNode.java:227-229
   * @implNote Unallocated resource is pre-computed and maintained incrementally during
   *           allocate/release operations rather than calculated on-demand. This design
   *           trades O(c) space for O(1) availability checks where c=container count,
   *           critical for high-frequency scheduling decisions.
   * @return Unallocated resources on the node
   */
  public synchronized Resource getUnallocatedResource() {
    return this.unallocatedResource;
  }

  /**
   * Get allocated resources on the node.
   *
   * @complexity Time: O(1) for direct field access to cached resource object.
   *             Space: O(1) - returns reference to existing Resource object, no allocation.
   *             Source: SchedulerNode.java:235-237
   * @implNote Allocated resource is pre-computed and maintained incrementally during
   *           allocate/release operations. Paired with unallocatedResource for O(1)
   *           resource availability calculations.
   * @return Allocated resources on the node
   */
  public synchronized Resource getAllocatedResource() {
    return this.allocatedResource;
  }

  /**
   * Get total resources on the node.
   *
   * @complexity Time: O(1) for direct field access to cached resource object.
   *             Space: O(1) - returns reference to existing Resource object, no allocation.
   *             Source: SchedulerNode.java:243-245
   * @implNote Total resource is cached at node construction and updated only on
   *           updateTotalResource() calls (e.g., dynamic resource changes). This avoids
   *           repeated RMNode queries during scheduling, trading memory for latency.
   * @return Total resources on the node.
   */
  public synchronized Resource getTotalResource() {
    return this.totalResource;
  }

  /**
   * Check if a container is launched by this node.
   *
   * @complexity Time: O(1) average-case for HashMap containsKey operation;
   *             O(n) worst-case with hash collisions where n=container count.
   *             Space: O(1) - no allocations, returns primitive boolean.
   *             Source: SchedulerNode.java:253-258
   * @implNote HashMap provides O(1) average-case container existence check vs O(n)
   *           for list-based linear search. Critical for frequent validation during
   *           container state updates.
   * @param containerId containerId.
   * @return If the container is launched by the node.
   */
  public synchronized boolean isValidContainer(ContainerId containerId) {
    if (launchedContainers.containsKey(containerId)) {
      return true;
    }
    return false;
  }

  /**
   * Update the resources of the node when releasing a container.
   * @param container Container to release.
   */
  protected synchronized void updateResourceForReleasedContainer(
      Container container) {
    if (container.getExecutionType() == ExecutionType.GUARANTEED) {
      addUnallocatedResource(container.getResource());
      --numContainers;
    }
  }

  /**
   * Release an allocated container on this node.
   *
   * @complexity Time: O(1) average-case for HashMap get and remove operations;
   *             O(n) worst-case with hash collisions where n=container count.
   *             Includes O(1) resource arithmetic for updateResourceForReleasedContainer.
   *             Space: O(1) - reclaims ContainerInfo object, no new allocations.
   *             Source: SchedulerNode.java:277-311
   * @implNote HashMap remove provides O(1) average container lookup and removal vs O(n)
   *           for list-based storage. Allocation tag removal via AllocationTagsManager
   *           is deferred to actual NM release to handle AM release/NM delay race conditions.
   * @param containerId ID of container to be released.
   * @param releasedByNode whether the release originates from a node update.
   */
  public synchronized void releaseContainer(ContainerId containerId,
      boolean releasedByNode) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info == null) {
      return;
    }
    if (!releasedByNode && info.launchedOnNode) {
      // wait until node reports container has completed
      return;
    }

    launchedContainers.remove(containerId);
    Container container = info.container.getContainer();

    // We remove allocation tags when a container is actually
    // released on NM. This is to avoid running into situation
    // when AM releases a container and NM has some delay to
    // actually release it, then the tag can still be visible
    // at RM so that RM can respect it during scheduling new containers.
    if (rmContext != null && rmContext.getAllocationTagsManager() != null) {
      rmContext.getAllocationTagsManager()
          .removeContainer(container.getNodeId(),
              container.getId(), container.getAllocationTags());
    }

    updateResourceForReleasedContainer(container);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Released container " + container.getId() + " of capacity "
              + container.getResource() + " on host " + rmNode.getNodeAddress()
              + ", which currently has " + numContainers + " containers, "
              + getAllocatedResource() + " used and " + getUnallocatedResource()
              + " available" + ", release resources=" + true);
    }
  }

  /**
   * Inform the node that a container has launched.
   * @param containerId ID of the launched container
   */
  public synchronized void containerStarted(ContainerId containerId) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info != null) {
      info.launchedOnNode = true;
    }
  }

  /**
   * Add unallocated resources to the node. This is used when unallocating a
   * container.
   * @param resource Resources to add.
   */
  private synchronized void addUnallocatedResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.addTo(unallocatedResource, resource);
    Resources.subtractFrom(allocatedResource, resource);
  }

  /**
   * Deduct unallocated resources from the node. This is used when allocating a
   * container.
   * @param resource Resources to deduct.
   */
  @VisibleForTesting
  public synchronized void deductUnallocatedResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.subtractFrom(unallocatedResource, resource);
    Resources.addTo(allocatedResource, resource);
  }

  /**
   * Reserve container for the attempt on this node.
   *
   * @complexity Time: O(1) for single reservation tracking (only one reservation per node).
   *             Space: O(1) - stores single RMContainer reference in reservedContainer field.
   *             Source: SchedulerNode.java:69 (reservedContainer field)
   * @implNote Node supports only one reservation at a time (reservedContainer field).
   *           This simplifies reservation management to O(1) operations but limits
   *           concurrent reservation attempts. Subclass implementations may add validation
   *           but core operation remains O(1).
   * @param attempt Application attempt asking for the reservation.
   * @param schedulerKey Priority of the reservation.
   * @param container Container reserving resources for.
   */
  public abstract void reserveResource(SchedulerApplicationAttempt attempt,
      SchedulerRequestKey schedulerKey, RMContainer container);

  /**
   * Unreserve resources on this node.
   *
   * @complexity Time: O(1) for clearing single reservation reference.
   *             Space: O(1) - releases reference to reserved container, no allocation.
   *             Source: SchedulerNode.java:69 (reservedContainer field)
   * @implNote Reservation release is O(1) as node maintains single reservedContainer
   *           reference. No iteration over containers required. Resource accounting
   *           updates (if any) in subclass implementations are also O(1) arithmetic.
   * @param attempt Application attempt that had done the reservation.
   */
  public abstract void unreserveResource(SchedulerApplicationAttempt attempt);

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers="
        + getNumContainers() + " available=" + getUnallocatedResource()
        + " used=" + getAllocatedResource();
  }

  /**
   * Get number of active containers on the node.
   *
   * @complexity Time: O(1) for volatile int field read.
   *             Space: O(1) - returns primitive int, no allocation.
   *             Source: SchedulerNode.java:70 (numContainers field)
   * @implNote Uses volatile int for thread-safe reads without synchronization overhead.
   *           Counter is maintained incrementally during allocate/release operations
   *           rather than computing launchedContainers.size() which would require
   *           synchronization. Trade-off: slight risk of momentary inconsistency
   *           between numContainers and actual map size during concurrent modifications.
   * @return Number of active containers on the node.
   */
  public int getNumContainers() {
    return numContainers;
  }

  /**
   * Get the containers running on the node.
   *
   * @complexity Time: O(c) where c=number of containers for iteration over launchedContainers.
   *             Space: O(c) for ArrayList allocation holding container references.
   *             Source: SchedulerNode.java:389-395
   * @implNote Returns defensive copy to prevent external modification of internal state.
   *           ArrayList pre-sized to launchedContainers.size() to avoid resizing overhead.
   *           O(c) iteration is unavoidable for copy; callers should cache result if
   *           multiple accesses needed within same scheduling cycle.
   * @return A copy of containers running on the node.
   */
  public synchronized List<RMContainer> getCopiedListOfRunningContainers() {
    List<RMContainer> result = new ArrayList<>(launchedContainers.size());
    for (ContainerInfo info : launchedContainers.values()) {
      result.add(info.container);
    }
    return result;
  }

  /**
   * Get the containers running on the node with AM containers at the end.
   *
   * @complexity Time: O(c) where c=number of containers for iteration and LinkedList
   *             addFirst/addLast operations (both O(1) per insertion).
   *             Space: O(c) for LinkedList allocation holding container references.
   *             Source: SchedulerNode.java:401-411
   * @implNote Uses LinkedList for O(1) addFirst/addLast operations to partition
   *           containers by AM status during single iteration. Non-AM containers
   *           added to front, AM containers to end for kill ordering preference.
   * @return A copy of running containers with AM containers at the end.
   */
  public synchronized List<RMContainer> getRunningContainersWithAMsAtTheEnd() {
    LinkedList<RMContainer> result = new LinkedList<>();
    for (ContainerInfo info : launchedContainers.values()) {
      if(info.container.isAMContainer()) {
        result.addLast(info.container);
      } else {
        result.addFirst(info.container);
      }
    }
    return result;
  }

  /**
   * Get the containers running on the node ordered by which to kill first. It
   * tries to kill AMs last, then GUARANTEED containers, and it kills
   * OPPORTUNISTIC first. If the same time, it uses the creation time.
   *
   * @complexity Time: O(c log c) where c=number of containers for TimSort-based
   *             Collections.sort. Includes O(c) for getLaunchedContainers() copy.
   *             Space: O(c) for ArrayList allocation plus O(log c) stack for TimSort.
   *             Source: SchedulerNode.java:419-429
   * @implNote Uses Java's TimSort (stable, adaptive) via Collections.sort. CompareToBuilder
   *           provides clean multi-key comparison at cost of object allocation per comparison.
   *           Kill order: OPPORTUNISTIC first, then GUARANTEED, AMs last within each category.
   * @return A copy of the running containers ordered by which to kill first.
   */
  public List<RMContainer> getContainersToKill() {
    List<RMContainer> result = getLaunchedContainers();
    Collections.sort(result, (c1, c2) -> {
      return new CompareToBuilder()
          .append(c1.isAMContainer(), c2.isAMContainer())
          .append(c2.getExecutionType(), c1.getExecutionType()) // reversed
          .append(c2.getCreationTime(), c1.getCreationTime()) // reversed
          .toComparison();
    });
    return result;
  }

  /**
   * Get the launched containers in the node.
   *
   * @complexity Time: O(c) where c=number of containers for iteration over launchedContainers.
   *             Space: O(c) for ArrayList allocation holding container references.
   *             Source: SchedulerNode.java:435-441
   * @implNote Returns new ArrayList (defensive copy). Unlike getCopiedListOfRunningContainers(),
   *           does not pre-size ArrayList. Consider pre-sizing for large container counts.
   * @return List of launched containers.
   */
  protected synchronized List<RMContainer> getLaunchedContainers() {
    List<RMContainer> result = new ArrayList<>();
    for (ContainerInfo info : launchedContainers.values()) {
      result.add(info.container);
    }
    return result;
  }

  /**
   * Get the container for the specified container ID.
   *
   * @complexity Time: O(1) average-case for HashMap get operation;
   *             O(n) worst-case with hash collisions where n=container count.
   *             Space: O(1) - returns existing reference, no allocation.
   *             Source: SchedulerNode.java:448-455
   * @implNote HashMap provides O(1) average-case container lookup. Returns null
   *           for non-existent containers rather than throwing exception.
   * @param containerId The container ID
   * @return The container for the specified container ID
   */
  protected synchronized RMContainer getContainer(ContainerId containerId) {
    RMContainer container = null;
    ContainerInfo info = launchedContainers.get(containerId);
    if (info != null) {
      container = info.container;
    }
    return container;
  }

  /**
   * Get the reserved container in the node.
   *
   * @complexity Time: O(1) for direct field access.
   *             Space: O(1) - returns existing reference, no allocation.
   *             Source: SchedulerNode.java:461-463
   * @implNote Single reservation per node design allows O(1) reservation lookup.
   *           Returns null if no reservation exists.
   * @return Reserved container in the node.
   */
  public synchronized RMContainer getReservedContainer() {
    return reservedContainer;
  }

  /**
   * Set the reserved container in the node.
   *
   * @complexity Time: O(1) for direct field assignment.
   *             Space: O(1) - stores single reference.
   *             Source: SchedulerNode.java:469-472
   * @implNote Single reservation model - setting a new reservation replaces any
   *           existing one. Callers should check/unreserve existing reservation first
   *           if needed.
   * @param reservedContainer Reserved container in the node.
   */
  public synchronized void
  setReservedContainer(RMContainer reservedContainer) {
    this.reservedContainer = reservedContainer;
  }

  /**
   * Recover a container.
   * @param rmContainer Container to recover.
   */
  public synchronized void recoverContainer(RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    allocateContainer(rmContainer, true);
  }

  /**
   * Get the labels for the node.
   * @return Set of labels for the node.
   */
  public Set<String> getLabels() {
    return labels;
  }

  /**
   * Update the labels for the node.
   * @param labels Set of labels for the node.
   */
  public void updateLabels(Set<String> labels) {
    this.labels = labels;
  }

  /**
   * Get partition of which the node belongs to, if node-labels of this node is
   * empty or null, it belongs to NO_LABEL partition. And since we only support
   * one partition for each node (YARN-2694), first label will be its partition.
   * @return Partition for the node.
   */
  public String getPartition() {
    if (this.labels == null || this.labels.isEmpty()) {
      return RMNodeLabelsManager.NO_LABEL;
    } else {
      return this.labels.iterator().next();
    }
  }

  /**
   * Set the resource utilization of the containers in the node.
   * @param containersUtilization Resource utilization of the containers.
   */
  public void setAggregatedContainersUtilization(
      ResourceUtilization containersUtilization) {
    this.containersUtilization = containersUtilization;
  }

  /**
   * Get the resource utilization of the containers in the node.
   * @return Resource utilization of the containers.
   */
  public ResourceUtilization getAggregatedContainersUtilization() {
    return this.containersUtilization;
  }

  /**
   * Set the resource utilization of the node. This includes the containers.
   * @param nodeUtilization Resource utilization of the node.
   */
  public void setNodeUtilization(ResourceUtilization nodeUtilization) {
    this.nodeUtilization = nodeUtilization;
  }

  /**
   * Get the resource utilization of the node.
   * @return Resource utilization of the node.
   */
  public ResourceUtilization getNodeUtilization() {
    return this.nodeUtilization;
  }

  public long getLastHeartbeatMonotonicTime() {
    return lastHeartbeatMonotonicTime;
  }

  /**
   * This will be called for each node heartbeat.
   */
  public void notifyNodeUpdate() {
    this.lastHeartbeatMonotonicTime = Time.monotonicNow();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchedulerNode)) {
      return false;
    }

    SchedulerNode that = (SchedulerNode) o;

    return getNodeID().equals(that.getNodeID());
  }

  @Override
  public int hashCode() {
    return getNodeID().hashCode();
  }

  public Set<NodeAttribute> getNodeAttributes() {
    return nodeAttributes;
  }

  public void updateNodeAttributes(Set<NodeAttribute> attributes) {
    this.nodeAttributes = attributes;
  }

  private static class ContainerInfo {
    private final RMContainer container;
    private boolean launchedOnNode;

    public ContainerInfo(RMContainer container, boolean launchedOnNode) {
      this.container = container;
      this.launchedOnNode = launchedOnNode;
    }
  }
}
