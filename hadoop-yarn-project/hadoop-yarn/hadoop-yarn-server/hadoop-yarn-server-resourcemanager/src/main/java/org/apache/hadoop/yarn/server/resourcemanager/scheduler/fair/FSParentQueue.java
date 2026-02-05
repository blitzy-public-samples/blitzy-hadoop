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
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

/**
 * Represents a parent queue in the fair scheduler queue hierarchy.
 * Parent queues contain child queues and aggregate resource demands
 * from their descendants.
 *
 * @performance Scaling: O(c × d) where c=child queues at each level, d=hierarchy depth.
 *              Memory: O(c) where c=child queues stored in ArrayList.
 *              Concurrency: ReentrantReadWriteLock provides concurrent read access
 *              during queue hierarchy traversal while ensuring exclusive write access
 *              for modifications. Read operations (getDemand, getChildQueues, getNumRunnableApps)
 *              scale linearly with concurrent readers.
 *              Source: FSParentQueue.java:44-332
 */
@Private
@Unstable
public class FSParentQueue extends FSQueue {
  private static final Logger LOG = LoggerFactory.getLogger(
      FSParentQueue.class.getName());

  private final List<FSQueue> childQueues = new ArrayList<>();
  private Resource demand = Resources.createResource(0);
  private int runnableApps;

  private ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private Lock readLock = rwLock.readLock();
  private Lock writeLock = rwLock.writeLock();

  public FSParentQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
  }

  @Override
  public Resource getMaximumContainerAllocation() {
    if (getName().equals("root")) {
      return maxContainerAllocation;
    }
    if (maxContainerAllocation.equals(Resources.unbounded())
        && getParent() != null) {
      return getParent().getMaximumContainerAllocation();
    } else {
      return maxContainerAllocation;
    }
  }

  void addChildQueue(FSQueue child) {
    writeLock.lock();
    try {
      childQueues.add(child);
    } finally {
      writeLock.unlock();
    }
  }

  void removeChildQueue(FSQueue child) {
    writeLock.lock();
    try {
      childQueues.remove(child);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Updates fair shares for this queue and recursively for all child queues.
   * Calls policy.computeShares() to calculate fair share distribution among
   * children, then propagates updates down the hierarchy.
   *
   * @complexity Time: O(c × d × a) worst-case where c=child queues at this level,
   *             d=max depth of subtree, a=apps in leaf queues.
   *             Calls policy.computeShares() O(c log c) for sorting children by
   *             weight/demand, then recursively calls childQueue.updateInternal()
   *             for each child queue.
   *             Space: O(d) stack depth for recursive traversal through queue hierarchy.
   *             Source: FSParentQueue.java:95-106
   */
  @Override
  void updateInternal() {
    readLock.lock();
    try {
      policy.computeShares(childQueues, getFairShare());
      for (FSQueue childQueue : childQueues) {
        childQueue.getMetrics().setFairShare(childQueue.getFairShare());
        childQueue.updateInternal();
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Recomputes steady fair shares for this queue and recursively for all
   * child parent queues. Steady shares represent the share a queue would
   * receive if all queues were active.
   *
   * @complexity Time: O(c × d) where c=total child queues across all levels,
   *             d=hierarchy depth. Performs recursive traversal calling
   *             computeSteadyShares at each parent level, which involves
   *             O(c log c) sorting per level.
   *             Space: O(d) stack depth for recursive traversal.
   *             Source: FSParentQueue.java:108-122
   */
  void recomputeSteadyShares() {
    readLock.lock();
    try {
      policy.computeSteadyShares(childQueues, getSteadyFairShare());
      for (FSQueue childQueue : childQueues) {
        childQueue.getMetrics()
            .setSteadyFairShare(childQueue.getSteadyFairShare());
        if (childQueue instanceof FSParentQueue) {
          ((FSParentQueue) childQueue).recomputeSteadyShares();
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the cached demand resource for this queue.
   * Creates a new Resource instance to prevent external modification.
   *
   * @complexity Time: O(1) for cached demand access with defensive copy creation.
   *             Space: O(1) for single Resource object allocation.
   *             Source: FSParentQueue.java:125-132
   *
   * @return a new Resource instance containing this queue's demand
   */
  @Override
  public Resource getDemand() {
    readLock.lock();
    try {
      return Resource.newInstance(demand.getMemorySize(), demand.getVirtualCores());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Updates the demand for this queue by aggregating demands from all
   * child queues. Recursively propagates updateDemand() calls down the
   * hierarchy and sums up child demands, capped by maxShare.
   *
   * @complexity Time: O(c × d × a) where c=children at this level, d=depth of subtree,
   *             a=apps in leaf queues. Each child.updateDemand() recurses into subtree,
   *             aggregating demands from all descendant applications.
   *             Space: O(d) stack depth for recursive traversal through queue hierarchy.
   *             Source: FSParentQueue.java:135-160
   */
  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    writeLock.lock();
    try {
      demand = Resources.createResource(0);
      for (FSQueue childQueue : childQueues) {
        childQueue.updateDemand();
        Resource toAdd = childQueue.getDemand();
        demand = Resources.add(demand, toAdd);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Counting resource from " + childQueue.getName() + " " +
              toAdd + "; Total resource demand for " + getName() +
              " now " + demand);
        }
      }
      // Cap demand to maxShare to limit allocation to maxShare
      demand = Resources.componentwiseMin(demand, getMaxShare());
    } finally {
      writeLock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand +
          "; the max is " + getMaxShare());
    }    
  }
  
  private QueueUserACLInfo getUserAclInfo(UserGroupInformation user) {
    List<QueueACL> operations = new ArrayList<>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      } 
    }
    return QueueUserACLInfo.newInstance(getQueueName(), operations);
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
    List<QueueUserACLInfo> userAcls = new ArrayList<>();
    
    // Add queue acls
    userAcls.add(getUserAclInfo(user));
    
    // Add children queue acls
    readLock.lock();
    try {
      for (FSQueue child : childQueues) {
        userAcls.addAll(child.getQueueUserAclInfo(user));
      }
    } finally {
      readLock.unlock();
    }
 
    return userAcls;
  }

  /**
   * Attempts to assign a container to one of the child queues on the given node.
   * Child queues are sorted by the scheduling policy comparator and tried in order
   * until one succeeds or all fail.
   *
   * @complexity Time: O(c × a) worst-case where c=child queues, a=average apps per leaf queue
   *             for recursive child queue traversal during container assignment.
   *             O(c log c) for TreeSet sorting of child queues by policy comparator.
   *             O(c) for iteration through sorted children until assignment succeeds.
   *             Total: O(c log c) + O(c × a) = O(c × a) dominated by recursive traversal.
   *             Note: Iteration stops on first successful allocation (break at line 236).
   *             Space: O(c) for TreeSet copy of child queues used for sorted iteration.
   *             Source: FSParentQueue.java:193-231
   *
   * @implNote Child queues are sorted using policy.getComparator() before iteration.
   *           TreeSet ensures queues furthest below fair share are tried first,
   *           implementing fair scheduling semantics. Trade-off: O(c log c) sorting
   *           overhead per assignment vs O(c) unsorted iteration. The sorting cost
   *           is accepted to ensure fairness in resource allocation order.
   *           Child queue locking is intentionally avoided (lines 205-212) to prevent
   *           performance degradation from lock contention during high-throughput scheduling.
   *
   * @param node the scheduler node to assign a container on
   * @return the resources assigned, or Resources.none() if no assignment was made
   */
  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();

    // If this queue is over its limit, reject
    if (!assignContainerPreCheck(node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Assign container precheck for queue " + getName() +
            " on node " + node.getNodeName() + " failed");
      }
      return assigned;
    }

    // Sort the queues while holding a read lock on this parent only.
    // The individual entries are not locked and can change which means that
    // the collection of childQueues can not be sorted by calling Sort().
    // Locking each childqueue to prevent changes would have a large
    // performance impact.
    // We do not have to handle the queue removal case as a queue must be
    // empty before removal. Assigning an application to a queue and removal of
    // that queue both need the scheduler lock.
    // @PerformanceCritical: TreeSet creation and sorting (O(c log c)) occurs on every assignment
    TreeSet<FSQueue> sortedChildQueues = new TreeSet<>(policy.getComparator());
    readLock.lock();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Node " + node.getNodeName() + " offered to parent queue: " +
            getName() + " visiting " + childQueues.size() + " children");
      }
      sortedChildQueues.addAll(childQueues);
      for (FSQueue child : sortedChildQueues) {
        assigned = child.assignContainer(node);
        if (!Resources.equals(assigned, Resources.none())) {
          break;
        }
      }
    } finally {
      readLock.unlock();
    }
    return assigned;
  }

  /**
   * Returns an immutable copy of the child queues list.
   * The immutable copy prevents external modification while allowing
   * safe iteration without holding locks.
   *
   * @complexity Time: O(c) for ImmutableList copy where c=number of child queues.
   *             Space: O(c) for returned immutable copy of child queue references.
   *             Source: FSParentQueue.java:234-241
   *
   * @return an immutable list of child queues
   */
  @Override
  public List<FSQueue> getChildQueues() {
    readLock.lock();
    try {
      return ImmutableList.copyOf(childQueues);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Increments the count of runnable applications in this parent queue.
   * Called when a new application becomes runnable in a descendant leaf queue.
   *
   * @complexity Time: O(1) for counter increment operation.
   *             Space: O(1) no additional allocations.
   *             Source: FSParentQueue.java:243-250
   */
  void incrementRunnableApps() {
    writeLock.lock();
    try {
      runnableApps++;
    } finally {
      writeLock.unlock();
    }
  }
  
  /**
   * Decrements the count of runnable applications in this parent queue.
   * Called when an application is no longer runnable in a descendant leaf queue.
   *
   * @complexity Time: O(1) for counter decrement operation.
   *             Space: O(1) no additional allocations.
   *             Source: FSParentQueue.java:252-259
   */
  void decrementRunnableApps() {
    writeLock.lock();
    try {
      runnableApps--;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns the cached count of runnable applications in this parent queue
   * and all descendant queues.
   *
   * @complexity Time: O(1) for cached count access.
   *             Space: O(1) returns primitive int value.
   *             Source: FSParentQueue.java:262-269
   *
   * @return the number of runnable applications
   */
  @Override
  public int getNumRunnableApps() {
    readLock.lock();
    try {
      return runnableApps;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    readLock.lock();
    try {
      for (FSQueue queue: childQueues) {
        if (!queue.isEmpty()) {
          return false;
        }
      }
    } finally {
      readLock.unlock();
    }
    return true;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
      for (FSQueue childQueue : childQueues) {
        childQueue.collectSchedulerApplications(apps);
      }
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public ActiveUsersManager getAbstractUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dumpStateInternal(StringBuilder sb) {
    sb.append("{Name: " + getName() +
        ", Weight: " + weights +
        ", Policy: " + policy.getName() +
        ", FairShare: " + getFairShare() +
        ", SteadyFairShare: " + getSteadyFairShare() +
        ", MaxShare: " + getMaxShare() +
        ", MinShare: " + minShare +
        ", ResourceUsage: " + getResourceUsage() +
        ", Demand: " + getDemand() +
        ", MaxAMShare: " + maxAMShare +
        ", Runnable: " + getNumRunnableApps() +
        "}");

    for(FSQueue child : getChildQueues()) {
      sb.append(", ");
      child.dumpStateInternal(sb);
    }
  }
}
