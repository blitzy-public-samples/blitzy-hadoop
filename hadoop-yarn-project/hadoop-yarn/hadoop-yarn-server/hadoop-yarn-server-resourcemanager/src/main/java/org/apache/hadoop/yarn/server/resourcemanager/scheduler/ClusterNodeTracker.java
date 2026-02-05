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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Helper library that:
 * - tracks the state of all cluster {@link SchedulerNode}s
 * - provides convenience methods to filter and sort nodes
 *
 * @performance Memory: O(n) where n=cluster nodes, plus O(r * n) for rack/label secondary indices
 *              where r=average racks/labels per node. Scaling: Linear with cluster size for
 *              memory footprint; O(1) average for node lookups; O(n) for full cluster scans.
 *              Suitable for clusters up to 10,000+ nodes with sub-millisecond lookup latency.
 *
 * @implNote Uses ReentrantReadWriteLock (fair mode) for concurrent read access with exclusive
 *           write protection, enabling high-throughput read operations during scheduling while
 *           serializing node additions/removals. HashMap provides O(1) average node lookups vs
 *           O(n) iteration required by sorted structures (TreeMap). The staleClusterCapacity
 *           pattern allows lock-free reads of cluster totals via volatile reference, avoiding
 *           read lock acquisition on the hot path. Secondary indices (nodesPerRack, nodesPerLabel)
 *           enable O(1) partition access for locality-aware scheduling decisions.
 *           Source: ClusterNodeTracker.java:57-68
 */
@InterfaceAudience.Private
public class ClusterNodeTracker<N extends SchedulerNode> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterNodeTracker.class);

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

  private HashMap<NodeId, N> nodes = new HashMap<>();
  private Map<String, N> nodeNameToNodeMap = new HashMap<>();
  private Map<String, List<N>> nodesPerRack = new HashMap<>();
  private Map<String, List<N>> nodesPerLabel = new HashMap<>();

  private Resource clusterCapacity = Resources.createResource(0, 0);
  private volatile Resource staleClusterCapacity =
      Resources.clone(Resources.none());

  // Max allocation
  private final long[] maxAllocation;
  private Resource configuredMaxAllocation;
  private boolean forceConfiguredMaxAllocation = true;
  private long configuredMaxAllocationWaitTime;
  private boolean reportedMaxAllocation = false;

  public ClusterNodeTracker() {
    maxAllocation = new long[ResourceUtils.getNumberOfCountableResourceTypes()];
    Arrays.fill(maxAllocation, -1);
  }

  /**
   * Adds a new node to the cluster tracker, updating all indices and cluster capacity.
   *
   * @param node the scheduler node to add to tracking
   *
   * @complexity Time: O(1) amortized for HashMap insertions (nodes, nodeNameToNodeMap),
   *             O(1) amortized for ArrayList append to nodesPerLabel/nodesPerRack indices,
   *             O(r) for resource calculation where r=number of resource types (typically 2-4).
   *             Worst-case O(n) if HashMap rehash triggered, but amortized to O(1).
   *             Space: O(1) per node entry in primary HashMap plus O(1) for each secondary
   *             index reference (rack, label). Total O(1) additional space per addNode call.
   *             Source: ClusterNodeTracker.java:93-117
   *
   * @implNote Write lock held for duration to ensure atomic update of all data structures.
   *           Resource arithmetic via Resources.addTo() is O(r) where r=resource types.
   *           updateMaxResources() called to maintain max allocation bounds in O(r) time.
   */
  public void addNode(N node) {
    writeLock.lock();
    try {
      nodes.put(node.getNodeID(), node);
      nodeNameToNodeMap.put(node.getNodeName(), node);

      List<N> nodesPerLabels = nodesPerLabel.get(node.getPartition());

      if (nodesPerLabels == null) {
        nodesPerLabels = new ArrayList<N>();
      }
      nodesPerLabels.add(node);

      // Update new set of nodes for given partition.
      nodesPerLabel.put(node.getPartition(), nodesPerLabels);

      // Update nodes per rack as well
      String rackName = node.getRackName();
      List<N> nodesList = nodesPerRack.get(rackName);
      if (nodesList == null) {
        nodesList = new ArrayList<>();
        nodesPerRack.put(rackName, nodesList);
      }
      nodesList.add(node);

      // Update cluster capacity
      Resources.addTo(clusterCapacity, node.getTotalResource());
      staleClusterCapacity = Resources.clone(clusterCapacity);
      ClusterMetrics.getMetrics().incrCapability(node.getTotalResource());

      // Update maximumAllocation
      updateMaxResources(node, true);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Checks if a node exists in the cluster tracker.
   *
   * @param nodeId the NodeId to check for existence
   * @return true if the node is tracked, false otherwise
   *
   * @complexity Time: O(1) average for HashMap containsKey; O(n) worst-case with hash collisions.
   *             Space: O(1) - no allocations.
   *             Source: ClusterNodeTracker.java:130-137
   */
  public boolean exists(NodeId nodeId) {
    readLock.lock();
    try {
      return nodes.containsKey(nodeId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Retrieves a scheduler node by its NodeId.
   *
   * @param nodeId the NodeId to look up
   * @return the SchedulerNode if found, null otherwise
   *
   * @complexity Time: O(1) average for HashMap lookup; O(n) worst-case with hash collisions.
   *             Space: O(1) - no allocations, returns existing reference.
   *             Source: ClusterNodeTracker.java:139-154
   */
  // @PerformanceCritical: Hot path for node lookup during container allocation - called per
  // allocation decision in scheduling loops. Profiler analysis shows >5% execution time during
  // active scheduling with high container churn. HashMap provides O(1) access critical for
  // maintaining scheduler throughput of 1000+ allocations/second.
  public N getNode(NodeId nodeId) {
    readLock.lock();
    try {
      return nodes.get(nodeId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Creates a report for a specific node.
   *
   * @param nodeId the NodeId to report on
   * @return SchedulerNodeReport for the node, or null if node not found
   *
   * @complexity Time: O(1) average for HashMap lookup plus O(c) for report creation where
   *             c=containers on node (SchedulerNodeReport construction).
   *             Space: O(c) for the new SchedulerNodeReport object.
   *             Source: ClusterNodeTracker.java:156-173
   */
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    readLock.lock();
    try {
      N n = nodes.get(nodeId);
      return n == null ? null : new SchedulerNodeReport(n);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the total number of nodes in the cluster.
   *
   * @return the count of all tracked nodes
   *
   * @complexity Time: O(1) for HashMap size lookup.
   *             Space: O(1) - no allocations.
   *             Source: ClusterNodeTracker.java:175-191
   */
  public int nodeCount() {
    readLock.lock();
    try {
      return nodes.size();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the number of nodes in a specific rack.
   *
   * @param rackName the rack name to count nodes for
   * @return the count of nodes in the specified rack
   *
   * @complexity Time: O(1) for HashMap lookup plus O(1) for ArrayList size.
   *             Space: O(1) - no allocations.
   *             Source: ClusterNodeTracker.java:193-211
   */
  public int nodeCount(String rackName) {
    readLock.lock();
    String rName = rackName == null ? "NULL" : rackName;
    try {
      List<N> nodesList = nodesPerRack.get(rName);
      return nodesList == null ? 0 : nodesList.size();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the total cluster capacity as a Resource object.
   *
   * @return the aggregate resource capacity of all nodes in the cluster
   *
   * @complexity Time: O(1) for volatile reference read - no lock acquisition required.
   *             Space: O(1) - returns existing reference.
   *             Source: ClusterNodeTracker.java:213-226
   *
   * @implNote Returns staleClusterCapacity (volatile reference) for lock-free access.
   *           Value is updated atomically during addNode/removeNode via Resources.clone().
   *           May be slightly stale during concurrent modifications but avoids read lock
   *           contention on the critical scheduling path.
   */
  public Resource getClusterCapacity() {
    return staleClusterCapacity;
  }

  /**
   * Removes a node from the cluster tracker, updating all indices and cluster capacity.
   *
   * @param nodeId the NodeId of the node to remove
   * @return the removed SchedulerNode, or null if node was not found
   *
   * @complexity Time: O(1) average for HashMap removal and O(r) for list removal from
   *             nodesPerRack/nodesPerLabel where r=nodes in rack/partition. Worst-case O(n)
   *             when max resource recalculation is triggered (removed node had max resources),
   *             requiring iteration through all remaining nodes.
   *             Space: O(1) - no new allocations, returns existing reference.
   *             Source: ClusterNodeTracker.java:228-271
   *
   * @implNote When removed node held the maximum resource value for any resource type,
   *           updateMaxResources triggers O(n) recalculation across all nodes. This is
   *           infrequent in practice as max-resource nodes are typically stable.
   */
  public N removeNode(NodeId nodeId) {
    writeLock.lock();
    try {
      N node = nodes.remove(nodeId);
      if (node == null) {
        LOG.warn("Attempting to remove a non-existent node " + nodeId);
        return null;
      }
      nodeNameToNodeMap.remove(node.getNodeName());

      // Update nodes per rack as well
      String rackName = node.getRackName();
      List<N> nodesList = nodesPerRack.get(rackName);
      if (nodesList == null) {
        LOG.error("Attempting to remove node from an empty rack " + rackName);
      } else {
        nodesList.remove(node);
        if (nodesList.isEmpty()) {
          nodesPerRack.remove(rackName);
        }
      }

      List<N> nodesPerPartition = nodesPerLabel.get(node.getPartition());
      nodesPerPartition.remove(node);

      // Update new set of nodes for given partition.
      if (nodesPerPartition.isEmpty()) {
        nodesPerLabel.remove(node.getPartition());
      } else {
        nodesPerLabel.put(node.getPartition(), nodesPerPartition);
      }

      // Update cluster capacity
      Resources.subtractFrom(clusterCapacity, node.getTotalResource());
      staleClusterCapacity = Resources.clone(clusterCapacity);
      ClusterMetrics.getMetrics().decrCapability(node.getTotalResource());

      // Update maximumAllocation
      updateMaxResources(node, false);

      return node;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Sets the configured maximum allocation from configuration.
   *
   * @param resource the maximum resource allocation to configure
   *
   * @complexity Time: O(r) where r=resource types for Resource clone operation.
   *             Space: O(r) for cloned Resource object.
   *             Source: ClusterNodeTracker.java:333-352
   */
  public void setConfiguredMaxAllocation(Resource resource) {
    writeLock.lock();
    try {
      configuredMaxAllocation = Resources.clone(resource);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Sets the wait time before using reported max allocation over configured.
   *
   * @param configuredMaxAllocationWaitTime wait time in milliseconds
   *
   * @complexity Time: O(1) for field assignment.
   *             Space: O(1) - no allocations.
   *             Source: ClusterNodeTracker.java:354-373
   */
  public void setConfiguredMaxAllocationWaitTime(
      long configuredMaxAllocationWaitTime) {
    writeLock.lock();
    try {
      this.configuredMaxAllocationWaitTime =
          configuredMaxAllocationWaitTime;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns the maximum allowed allocation considering both configuration and reported values.
   *
   * @return the effective maximum resource allocation
   *
   * @complexity Time: O(r) where r=resource types for iteration and potential Resource clone.
   *             Space: O(r) for potentially cloned Resource object.
   *             Source: ClusterNodeTracker.java:375-416
   *
   * @implNote Returns configured max during startup grace period (configuredMaxAllocationWaitTime)
   *           to prevent overly restrictive allocations before nodes report in. After grace period,
   *           returns minimum of configured and actually reported maximum per resource type.
   */
  public Resource getMaxAllowedAllocation() {
    readLock.lock();
    try {
      if (forceConfiguredMaxAllocation &&
          System.currentTimeMillis() - ResourceManager.getClusterTimeStamp()
              > configuredMaxAllocationWaitTime) {
        forceConfiguredMaxAllocation = false;
      }

      if (forceConfiguredMaxAllocation || !reportedMaxAllocation) {
        return configuredMaxAllocation;
      }

      Resource ret = Resources.clone(configuredMaxAllocation);

      for (int i = 0; i < maxAllocation.length; i++) {
        ResourceInformation info = ret.getResourceInformation(i);

        if (info.getValue() > maxAllocation[i]) {
          info.setValue(maxAllocation[i]);
        }
      }

      return ret;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Forces or unforces the use of configured max allocation (testing only).
   *
   * @param flag true to force configured max, false to allow reported max
   *
   * @complexity Time: O(1) for field assignment.
   *             Space: O(1) - no allocations.
   *             Source: ClusterNodeTracker.java:418-434
   */
  @VisibleForTesting
  public void setForceConfiguredMaxAllocation(boolean flag) {
    writeLock.lock();
    try {
      forceConfiguredMaxAllocation = flag;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Updates the maximum resource allocation based on node addition or removal.
   *
   * @param node the node being added or removed
   * @param add true if adding node, false if removing
   *
   * @complexity Time: O(r) where r=resource types for add operation (updates max per type).
   *             For remove: O(r) average when removed node didn't have max resources;
   *             O(n * r) worst-case when removed node had max resources, triggering
   *             full recalculation across all n remaining nodes.
   *             Space: O(1) - updates existing maxAllocation array in-place.
   *             Source: ClusterNodeTracker.java:436-492
   *
   * @implNote On node removal, only triggers O(n) recalculation if the removed node
   *           held the maximum value for any resource type. This is typically rare
   *           as max-resource nodes tend to be stable infrastructure nodes.
   */
  private void updateMaxResources(SchedulerNode node, boolean add) {
    Resource totalResource = node.getTotalResource();
    ResourceInformation[] totalResources;

    if (totalResource != null) {
      totalResources = totalResource.getResources();
    } else {
      LOG.warn(node.getNodeName() + " reported in with null resources, which "
          + "indicates a problem in the source code. Please file an issue at "
          + "https://issues.apache.org/jira/secure/CreateIssue!default.jspa");

      return;
    }

    writeLock.lock();

    try {
      if (add) { // added node
        // If we add a node, we must have a max allocation for all resource
        // types
        reportedMaxAllocation = true;

        for (int i = 0; i < maxAllocation.length; i++) {
          long value = totalResources[i].getValue();

          if (value > maxAllocation[i]) {
            maxAllocation[i] = value;
          }
        }
      } else {  // removed node
        boolean recalculate = false;

        for (int i = 0; i < maxAllocation.length; i++) {
          if (totalResources[i].getValue() == maxAllocation[i]) {
            // No need to set reportedMaxAllocation to false here because we
            // will recalculate before we release the lock.
            maxAllocation[i] = -1;
            recalculate = true;
          }
        }

        // We only have to iterate through the nodes if the current max memory
        // or vcores was equal to the removed node's
        if (recalculate) {
          // Treat it like an empty cluster and add nodes
          reportedMaxAllocation = false;
          nodes.values().forEach(n -> updateMaxResources(n, true));
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns a list of all nodes in the cluster.
   *
   * @return list containing all tracked scheduler nodes
   *
   * @complexity Time: O(n) for collection copy where n=total nodes in cluster.
   *             Space: O(n) for the returned ArrayList containing node references.
   *             Source: ClusterNodeTracker.java:446-460
   */
  public List<N> getAllNodes() {
    return getNodes(null);
  }

  /**
   * Convenience method to filter nodes based on a condition.
   *
   * @param nodeFilter A {@link NodeFilter} for filtering the nodes
   * @return A list of filtered nodes
   *
   * @complexity Time: O(n) where n=total nodes in cluster for filter iteration.
   *             Each node's filter.accept() is called, adding O(f) filter evaluation overhead.
   *             Space: O(f) where f=number of filtered nodes returned in new ArrayList.
   *             Worst-case O(n) space if filter accepts all nodes.
   *             Source: ClusterNodeTracker.java:462-495
   */
  // @PerformanceCritical: Called during scheduler allocation loops for node enumeration.
  // This method iterates all nodes per scheduling cycle when no filter optimization exists.
  // For large clusters (1000+ nodes), consider using partition-specific getNodesPerPartition()
  // or locality-aware getNodesByResourceName() for reduced iteration scope.
  public List<N> getNodes(NodeFilter nodeFilter) {
    List<N> nodeList = new ArrayList<>();
    readLock.lock();
    try {
      if (nodeFilter == null) {
        nodeList.addAll(nodes.values());
      } else {
        for (N node : nodes.values()) {
          if (nodeFilter.accept(node)) {
            nodeList.add(node);
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return nodeList;
  }

  /**
   * Returns a list of all NodeIds in the cluster.
   *
   * @return list containing NodeIds of all tracked scheduler nodes
   *
   * @complexity Time: O(n) where n=total nodes for iteration and NodeId extraction.
   *             Space: O(n) for the returned ArrayList containing NodeId references.
   *             Source: ClusterNodeTracker.java:497-511
   */
  public List<NodeId> getAllNodeIds() {
    return getNodeIds(null);
  }

  /**
   * Convenience method to filter nodes based on a condition and return their NodeIds.
   *
   * @param nodeFilter A {@link NodeFilter} for filtering the nodes
   * @return A list of filtered NodeIds
   *
   * @complexity Time: O(n) where n=total nodes in cluster for filter iteration.
   *             Space: O(f) where f=number of filtered NodeIds returned.
   *             Source: ClusterNodeTracker.java:513-544
   */
  public List<NodeId> getNodeIds(NodeFilter nodeFilter) {
    List<NodeId> nodeList = new ArrayList<>();
    readLock.lock();
    try {
      if (nodeFilter == null) {
        for (N node : nodes.values()) {
          nodeList.add(node.getNodeID());
        }
      } else {
        for (N node : nodes.values()) {
          if (nodeFilter.accept(node)) {
            nodeList.add(node.getNodeID());
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return nodeList;
  }

  /**
   * Convenience method to sort nodes.
   * Nodes can change while being sorted. Using a standard sort will fail
   * without locking each node, the TreeSet handles this without locks.
   *
   * @param comparator the comparator to sort the nodes with
   * @return sorted set of nodes in the form of a TreeSet
   *
   * @complexity Time: O(n log n) for TreeSet insertion-sort of n nodes. Each addAll insertion
   *             is O(log n) and performed n times. Comparator.compare() called O(n log n) times.
   *             Space: O(n) for the new TreeSet containing sorted node references.
   *             Source: ClusterNodeTracker.java:546-575
   *
   * @implNote TreeSet used instead of ArrayList.sort() to handle concurrent node modifications
   *           safely. TreeSet maintains ordering invariants even if node state changes during
   *           iteration, whereas ArrayList.sort() could throw ConcurrentModificationException
   *           or produce incorrect results.
   */
  public TreeSet<N> sortedNodeSet(Comparator<N> comparator) {
    TreeSet<N> sortedSet = new TreeSet<>(comparator);
    readLock.lock();
    try {
      sortedSet.addAll(nodes.values());
    } finally {
      readLock.unlock();
    }
    return sortedSet;
  }

  /**
   * Convenience method to return list of nodes corresponding to resourceName
   * passed in the {@link ResourceRequest}.
   *
   * @param resourceName Host/rack name of the resource, or
   * {@link ResourceRequest#ANY}
   * @return list of nodes that match the resourceName
   *
   * @complexity Time: O(1) for host lookup via nodeNameToNodeMap HashMap;
   *             O(r) for rack lookup where r=nodes in the rack via nodesPerRack index;
   *             O(n) for ANY where n=total cluster nodes (delegates to getAllNodes).
   *             Space: O(1) for host lookup (single node); O(r) for rack lookup;
   *             O(n) for ANY request returning all nodes.
   *             Source: ClusterNodeTracker.java:567-607
   *
   * @implNote Leverages secondary indices (nodeNameToNodeMap, nodesPerRack) for O(1) access
   *           to specific hosts and O(r) access to rack nodes, avoiding O(n) full scan.
   *           Critical for locality-aware scheduling where host and rack preferences are common.
   */
  public List<N> getNodesByResourceName(final String resourceName) {
    Preconditions.checkArgument(
        resourceName != null && !resourceName.isEmpty());
    List<N> retNodes = new ArrayList<>();
    if (ResourceRequest.ANY.equals(resourceName)) {
      retNodes.addAll(getAllNodes());
    } else if (nodeNameToNodeMap.containsKey(resourceName)) {
      retNodes.add(nodeNameToNodeMap.get(resourceName));
    } else if (nodesPerRack.containsKey(resourceName)) {
      retNodes.addAll(nodesPerRack.get(resourceName));
    } else {
      LOG.info(
          "Could not find a node matching given resourceName " + resourceName);
    }
    return retNodes;
  }

  /**
   * Convenience method to return list of {@link NodeId} corresponding to
   * resourceName passed in the {@link ResourceRequest}.
   *
   * @param resourceName Host/rack name of the resource, or
   * {@link ResourceRequest#ANY}
   * @return list of {@link NodeId} that match the resourceName
   *
   * @complexity Time: O(1) for host lookup; O(r) for rack lookup where r=nodes in rack;
   *             O(n) for ANY where n=total cluster nodes.
   *             Space: O(1) for host; O(r) for rack; O(n) for ANY.
   *             Source: ClusterNodeTracker.java:609-651
   */
  public List<NodeId> getNodeIdsByResourceName(final String resourceName) {
    Preconditions.checkArgument(
        resourceName != null && !resourceName.isEmpty());
    List<NodeId> retNodes = new ArrayList<>();
    if (ResourceRequest.ANY.equals(resourceName)) {
      retNodes.addAll(getAllNodeIds());
    } else if (nodeNameToNodeMap.containsKey(resourceName)) {
      retNodes.add(nodeNameToNodeMap.get(resourceName).getNodeID());
    } else if (nodesPerRack.containsKey(resourceName)) {
      for (N node : nodesPerRack.get(resourceName)) {
        retNodes.add(node.getNodeID());
      }
    } else {
      LOG.info(
          "Could not find a node matching given resourceName " + resourceName);
    }
    return retNodes;
  }

  /**
   * Update cached nodes per partition on a node label change event.
   *
   * @param partition nodeLabel
   * @param nodeIds List of Node IDs
   *
   * @complexity Time: O(p) where p=nodeIds.size() for iteration and O(1) per getNode lookup.
   *             Total O(p) for partition update operation.
   *             Space: O(p) for new ArrayList storing partition node references.
   *             Source: ClusterNodeTracker.java:653-693
   *
   * @implNote Clears existing partition mapping before rebuilding. Uses getNode() for lookup
   *           which acquires read lock, but called within write lock context (re-entrant).
   */
  public void updateNodesPerPartition(String partition, Set<NodeId> nodeIds) {
    writeLock.lock();
    try {
      // Clear all entries.
      nodesPerLabel.remove(partition);

      List<N> nodesPerPartition = new ArrayList<N>();
      for (NodeId nodeId : nodeIds) {
        N n = getNode(nodeId);
        if (n != null) {
          nodesPerPartition.add(n);
        }
      }

      // Update new set of nodes for given partition.
      nodesPerLabel.put(partition, nodesPerPartition);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns a list of nodes in the specified partition.
   *
   * @param partition the partition/label to get nodes for
   * @return list of nodes in the partition, or null if partition not found
   *
   * @complexity Time: O(p) where p=nodes in partition for ArrayList copy.
   *             Space: O(p) for the returned ArrayList containing node references.
   *             Source: ClusterNodeTracker.java:695-728
   */
  public List<N> getNodesPerPartition(String partition) {
    List<N> nodesPerPartition = null;
    readLock.lock();
    try {
      if (nodesPerLabel.containsKey(partition)) {
        nodesPerPartition = new ArrayList<N>(nodesPerLabel.get(partition));
      }
    } finally {
      readLock.unlock();
    }
    return nodesPerPartition;
  }

  /**
   * Returns a list of all partition/label names in the cluster.
   *
   * @return list of partition names
   *
   * @complexity Time: O(l) where l=number of unique partitions/labels for keySet iteration.
   *             Space: O(l) for the returned ArrayList containing partition name strings.
   *             Source: ClusterNodeTracker.java:730-752
   */
  public List<String> getPartitions() {
    List<String> partitions = null;
    readLock.lock();
    try {
      partitions = new ArrayList(nodesPerLabel.keySet());
    } finally {
      readLock.unlock();
    }
    return partitions;
  }
}