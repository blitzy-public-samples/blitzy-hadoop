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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.InvalidLabelResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException
        .InvalidResourceType;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResourceRequestException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException
        .GREATER_THAN_MAX_RESOURCE_MESSAGE_TEMPLATE;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException
        .LESS_THAN_ZERO_RESOURCE_MESSAGE_TEMPLATE;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.UNKNOWN_REASON_MESSAGE_TEMPLATE;

/**
 * Utilities shared by schedulers.
 *
 * @performance This utility class provides O(r) operations for resource validation
 *              where r is the number of resource types (typically 2-10). All methods
 *              are thread-safe for concurrent scheduler operations. Linear scaling
 *              with number of resource types configured in the cluster; performance
 *              degrades gracefully as custom resource types are added.
 */
@Private
@Unstable
public class SchedulerUtils {

  /**
   * This class contains invalid resource information along with its
   * resource request. Used to report detailed validation failures for
   * queue maximum resource checks.
   *
   * @complexity Space: O(r) where r=number of invalid resource dimensions.
   *             Typically small (0-2 invalid resources in practice).
   *             Source: SchedulerUtils.java:93-112
   */
  public static class MaxResourceValidationResult {
    private ResourceRequest resourceRequest;
    private List<ResourceInformation> invalidResources;

    /**
     * Constructs a validation result with the request and any invalid resources.
     *
     * @complexity Time: O(1) - simple assignment.
     *             Space: O(1) - stores references, no copying.
     *
     * @param resourceRequest the validated request
     * @param invalidResources list of resource dimensions that failed validation
     */
    MaxResourceValidationResult(ResourceRequest resourceRequest,
        List<ResourceInformation> invalidResources) {
      this.resourceRequest = resourceRequest;
      this.invalidResources = invalidResources;
    }

    /**
     * Checks if the resource request passed validation.
     *
     * @complexity Time: O(1) - checks list emptiness.
     *             Space: O(1) - no allocations.
     *
     * @return true if no invalid resources, false otherwise
     */
    public boolean isValid() {
      return invalidResources.isEmpty();
    }

    @Override
    public String toString() {
      return "MaxResourceValidationResult{" + "resourceRequest="
          + resourceRequest + ", invalidResources=" + invalidResources + '}';
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulerUtils.class);

  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  public static final String RELEASED_CONTAINER =
      "Container released by application";

  public static final String UPDATED_CONTAINER =
      "Temporary container killed by application for ExeutionType update";

  public static final String LOST_CONTAINER =
      "Container released on a *lost* node";

  public static final String PREEMPTED_CONTAINER =
      "Container preempted by scheduler";

  public static final String COMPLETED_APPLICATION =
      "Container of a completed application";

  public static final String EXPIRED_CONTAINER =
      "Container expired since it was unused";

  public static final String UNRESERVED_CONTAINER =
      "Container reservation no longer required.";

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances with ABORTED exit status.
   *
   * @complexity Time: O(1) - creates single ContainerStatus via RecordFactory.
   *             Space: O(1) - single ContainerStatus allocation (~100 bytes).
   *             Source: SchedulerUtils.java:172-176
   *
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost 
   *         container
   */
  public static ContainerStatus createAbnormalContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.ABORTED, diagnostics);
  }


  /**
   * Utility to create a {@link ContainerStatus} for killed containers.
   *
   * @complexity Time: O(1) - creates single ContainerStatus via RecordFactory.
   *             Space: O(1) - single ContainerStatus allocation (~100 bytes).
   *             Source: SchedulerUtils.java:185-189
   *
   * @param containerId {@link ContainerId} of the killed container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for a killed container
   */
  public static ContainerStatus createKilledContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.KILLED_BY_RESOURCEMANAGER, diagnostics);
  }

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances with PREEMPTED exit status.
   *
   * @complexity Time: O(1) - creates single ContainerStatus via RecordFactory.
   *             Space: O(1) - single ContainerStatus allocation (~100 bytes).
   *             Source: SchedulerUtils.java:200-204
   *
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost
   *         container
   */
  public static ContainerStatus createPreemptedContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.PREEMPTED, diagnostics);
  }

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances.
   *
   * @complexity Time: O(1) - creates single ContainerStatus via RecordFactory.
   *             Space: O(1) - single ContainerStatus allocation (~100 bytes).
   *             Source: SchedulerUtils.java:215-224
   * @implNote Uses RecordFactory for consistent object creation across
   *           serialization frameworks. ContainerStatus is marked COMPLETE
   *           regardless of specific exit status to indicate termination.
   *
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param exitStatus the exit status code
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost 
   *         container
   */
  private static ContainerStatus createAbnormalContainerStatus(
      ContainerId containerId, int exitStatus, String diagnostics) {
    ContainerStatus containerStatus =
        recordFactory.newRecordInstance(ContainerStatus.class);
    containerStatus.setContainerId(containerId);
    containerStatus.setDiagnostics(diagnostics);
    containerStatus.setExitStatus(exitStatus);
    containerStatus.setState(ContainerState.COMPLETE);
    return containerStatus;
  }

  /**
   * Utility method to normalize a resource request, by ensuring that the
   * requested memory is a multiple of minMemory and is not zero.
   *
   * @complexity Time: O(r) where r=number of resource types (typically 2-10);
   *             iterates through each resource dimension for normalization.
   *             Space: O(1) - modifies request in place, no auxiliary allocations.
   *             Source: SchedulerUtils.java:209-217
   * @implNote Uses integer ceiling for proper resource alignment to increment
   *           boundaries. Minimum resource is used as the increment value for
   *           normalization, ensuring allocations align to cluster granularity.
   *
   * @param ask resource request.
   * @param resourceCalculator {@link ResourceCalculator} the resource
   * calculator to use.
   * @param minimumResource minimum Resource.
   * @param maximumResource maximum Resource.
   */
  @VisibleForTesting
  public static void normalizeRequest(
    ResourceRequest ask,
    ResourceCalculator resourceCalculator,
    Resource minimumResource,
    Resource maximumResource) {
    ask.setCapability(
        getNormalizedResource(ask.getCapability(), resourceCalculator,
            minimumResource, maximumResource, minimumResource));
  }

  /**
   * Utility method to normalize a resource request, by ensuring that the
   * requested memory is a multiple of increment resource and is not zero.
   *
   * @complexity Time: O(r) where r=number of resource types; performs ceiling
   *             division and clamping for each resource dimension.
   *             Space: O(r) for creating normalized Resource copy.
   *             Source: SchedulerUtils.java:241-251
   * @implNote Resource normalization uses integer ceiling to round up to
   *           the nearest increment boundary, ensuring fair allocation
   *           granularity. Values are clamped between minimum and maximum
   *           after normalization to prevent invalid allocations.
   *
   * @param ask resource request.
   * @param resourceCalculator {@link ResourceCalculator} the resource
   * calculator to use.
   * @param minimumResource minimum Resource.
   * @param maximumResource maximum Resource.
   * @param incrementResource increment Resource.
   * @return normalized resource
   */
  public static Resource getNormalizedResource(
      Resource ask,
      ResourceCalculator resourceCalculator,
      Resource minimumResource,
      Resource maximumResource,
      Resource incrementResource) {
    Resource normalized = Resources.normalize(
        resourceCalculator, ask, minimumResource,
        maximumResource, incrementResource);
    return normalized;
  }

  /**
   * Normalizes node label expression in a resource request using queue defaults.
   *
   * @complexity Time: O(1) - simple conditional assignments and string comparisons.
   *             Space: O(1) - no allocations, modifies request in place.
   *             Source: SchedulerUtils.java:259-288
   * @implNote Label normalization follows precedence: (1) explicit request label,
   *           (2) queue's default label for ANY requests, (3) NO_LABEL for
   *           pre-configured queues without label. Dynamic queues handle labels
   *           separately in RMAppAttemptImpl.ScheduledTransition.
   *
   * @param resReq resource request to normalize
   * @param queueInfo queue information containing default label expression
   */
  private static void normalizeNodeLabelExpressionInRequest(
      ResourceRequest resReq, QueueInfo queueInfo) {

    String labelExp = resReq.getNodeLabelExpression();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested Node Label Expression : " + labelExp);
      LOG.debug("Queue Info : " + queueInfo);
    }

    // if queue has default label expression, and RR doesn't have, use the
    // default label expression of queue
    if (labelExp == null && queueInfo != null && ResourceRequest.ANY
        .equals(resReq.getResourceName())) {
      LOG.debug("Setting default node label expression : {}", queueInfo
          .getDefaultNodeLabelExpression());
      labelExp = queueInfo.getDefaultNodeLabelExpression();
    }

    // If labelExp still equals to null, it could either be a dynamic queue
    // or the label is not configured
    // set it to be NO_LABEL in case of a pre-configured queue. Dynamic
    // queues are handled in RMAppAttemptImp.ScheduledTransition
    if (labelExp == null && queueInfo != null) {
      labelExp = RMNodeLabelsManager.NO_LABEL;
    }

    if (labelExp != null) {
      resReq.setNodeLabelExpression(labelExp);
    }
  }

  /**
   * Normalizes and validates a resource request against cluster constraints.
   *
   * @complexity Time: O(r) where r=number of resource types for validation;
   *             performs label validation O(1) and resource bounds checking O(r).
   *             Space: O(1) - no auxiliary data structures allocated.
   *             Source: SchedulerUtils.java:290-324
   * @implNote Validation order is critical: (1) node label enablement check,
   *           (2) queue info resolution for dynamic queues, (3) label
   *           expression normalization, (4) resource bounds validation.
   *           This ordering ensures early failure for invalid labels before
   *           expensive resource validation. Recovery mode skips validation
   *           to allow container recovery from AM failures.
   *
   * @param resReq resource request to normalize and validate
   * @param maximumAllocation maximum allowed allocation
   * @param queueName target queue name
   * @param isRecovery whether this is a recovery operation (skips validation)
   * @param rmContext RM context for accessing scheduler and configuration
   * @param queueInfo queue information (may be null for auto-created queues)
   * @param nodeLabelsEnabled whether node labels feature is enabled
   * @throws InvalidResourceRequestException if request violates constraints
   */
  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumAllocation, String queueName, boolean isRecovery,
      RMContext rmContext, QueueInfo queueInfo, boolean nodeLabelsEnabled)
          throws InvalidResourceRequestException {
    Configuration conf = rmContext.getYarnConfiguration();
    // If Node label is not enabled throw exception
    if (null != conf && !nodeLabelsEnabled) {
      String labelExp = resReq.getNodeLabelExpression();
      if (!(RMNodeLabelsManager.NO_LABEL.equals(labelExp)
          || null == labelExp)) {
        String message = "NodeLabel is not enabled in cluster, but resource"
            + " request contains a label expression.";
        LOG.warn(message);
        if (!isRecovery) {
          throw new InvalidLabelResourceRequestException(
              "Invalid resource request, node label not enabled "
                  + "but request contains label expression");
        }
      }
    }
    if (null == queueInfo) {
      try {
        queueInfo = rmContext.getScheduler().getQueueInfo(queueName, false,
            false);
      } catch (IOException e) {
        //Queue may not exist since it could be auto-created in case of
        // dynamic queues
      }
    }
    SchedulerUtils.normalizeNodeLabelExpressionInRequest(resReq, queueInfo);

    if (!isRecovery) {
      validateResourceRequest(resReq, maximumAllocation, queueInfo, rmContext);
    }
  }

  /**
   * Normalizes and validates a resource request (non-recovery mode).
   *
   * @complexity Time: O(r) where r=number of resource types; delegates to
   *             full validation method with isRecovery=false.
   *             Space: O(1) - passthrough method with no allocations.
   *             Source: SchedulerUtils.java:326-332
   * @implNote Convenience overload that always performs full validation.
   *           Use the overload with isRecovery parameter for AM recovery
   *           scenarios where validation should be skipped.
   *
   * @param resReq resource request to normalize and validate
   * @param maximumAllocation maximum allowed allocation
   * @param queueName target queue name
   * @param rmContext RM context for accessing scheduler and configuration
   * @param queueInfo queue information (may be null for auto-created queues)
   * @param nodeLabelsEnabled whether node labels feature is enabled
   * @throws InvalidResourceRequestException if request violates constraints
   */
  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumAllocation, String queueName, RMContext rmContext,
      QueueInfo queueInfo, boolean nodeLabelsEnabled)
          throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumAllocation, queueName, false,
        rmContext, queueInfo, nodeLabelsEnabled);
  }

  /**
   * If RM should enforce partition exclusivity for enforced partition "x":
   * 1) If request is "x" and app label is not "x",
   *    override request to app's label.
   * 2) If app label is "x", ensure request is "x".
   *
   * @complexity Time: O(p) where p=number of enforced partitions; uses
   *             HashSet.contains() which is O(1) amortized per lookup.
   *             Space: O(1) - modifies request in place, no allocations.
   *             Source: SchedulerUtils.java:343-355
   * @implNote Uses HashSet for enforcedPartitions to achieve O(1) amortized
   *           lookup time vs O(p) linear scan with List. This is critical
   *           for clusters with many partition constraints where this method
   *           is called for every resource request.
   *
   * @param resReq resource request
   * @param enforcedPartitions list of exclusive enforced partitions
   * @param appLabel app's node label expression
   */
  public static void enforcePartitionExclusivity(ResourceRequest resReq,
      Set<String> enforcedPartitions, String appLabel) {
    if (enforcedPartitions == null || enforcedPartitions.isEmpty()) {
      return;
    }
    if (!enforcedPartitions.contains(appLabel)
        && enforcedPartitions.contains(resReq.getNodeLabelExpression())) {
      resReq.setNodeLabelExpression(appLabel);
    }
    if (enforcedPartitions.contains(appLabel)) {
      resReq.setNodeLabelExpression(appLabel);
    }
  }

  /**
   * Utility method to validate a resource request, by ensuring that the
   * requested memory/vcore is non-negative and not greater than max.
   *
   * @complexity Time: O(r + L) where r=number of resource types and L=label
   *             expression length. Resource validation iterates all types O(r),
   *             label parsing splits on "&&" and checks queue access O(L).
   *             Space: O(1) - no auxiliary data structures created.
   *             Source: SchedulerUtils.java:363-405
   * @implNote Validation order: (1) resource bounds check via
   *           checkResourceRequestAgainstAvailableResource O(r), (2) label
   *           expression location validation (must be ANY for specific nodes),
   *           (3) single label enforcement (no && allowed), (4) queue label
   *           accessibility check. Early termination on first validation
   *           failure for efficiency.
   *
   * @throws InvalidResourceRequestException when there is invalid request
   */
  private static void validateResourceRequest(ResourceRequest resReq,
      Resource maximumAllocation, QueueInfo queueInfo, RMContext rmContext)
      throws InvalidResourceRequestException {
    final Resource requestedResource = resReq.getCapability();
    checkResourceRequestAgainstAvailableResource(requestedResource,
        maximumAllocation);

    String labelExp = resReq.getNodeLabelExpression();
    // we don't allow specify label expression other than resourceName=ANY now
    if (!ResourceRequest.ANY.equals(resReq.getResourceName())
        && labelExp != null && !labelExp.trim().isEmpty()) {
      throw new InvalidLabelResourceRequestException(
          "Invalid resource request, queue=" + queueInfo.getQueueName()
              + " specified node label expression in a "
              + "resource request has resource name = "
              + resReq.getResourceName());
    }

    // we don't allow specify label expression with more than one node labels now
    if (labelExp != null && labelExp.contains("&&")) {
      throw new InvalidLabelResourceRequestException(
          "Invalid resource request, queue=" + queueInfo.getQueueName()
              + " specified more than one node label "
              + "in a node label expression, node label expression = "
              + labelExp);
    }

    if (labelExp != null && !labelExp.trim().isEmpty() && queueInfo != null) {
      if (!checkQueueLabelExpression(queueInfo.getAccessibleNodeLabels(),
          labelExp, rmContext)) {
        throw new InvalidLabelResourceRequestException(
            "Invalid resource request" + ", queue=" + queueInfo.getQueueName()
                + " doesn't have permission to access all labels "
                + "in resource request. labelExpression of resource request="
                + labelExp + ". Queue labels="
                + (queueInfo.getAccessibleNodeLabels() == null ? ""
                    : StringUtils.join(
                        queueInfo.getAccessibleNodeLabels().iterator(), ',')));
      } else {
        checkQueueLabelInLabelManager(labelExp, rmContext);
      }
    }
  }

  /**
   * Extracts resource dimensions that have zero allocation from a Resource.
   *
   * @complexity Time: O(r) where r=number of resource types; iterates all dimensions.
   *             Space: O(r) worst-case for result map when all resources are zero.
   *             Source: SchedulerUtils.java:407-421
   * @implNote Used to identify resource types that are effectively disabled
   *           in a queue's maximum allocation. Requesting non-zero values for
   *           these types should be flagged as invalid.
   *
   * @param resource the resource to analyze
   * @return map of resource names to ResourceInformation for zero-valued dimensions
   */
  private static Map<String, ResourceInformation> getZeroResources(
      Resource resource) {
    Map<String, ResourceInformation> resourceInformations = Maps.newHashMap();
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();

    for (int i = 0; i < maxLength; i++) {
      ResourceInformation resourceInformation =
          resource.getResourceInformation(i);
      if (resourceInformation.getValue() == 0L) {
        resourceInformations.put(resourceInformation.getName(),
            resourceInformation);
      }
    }
    return resourceInformations;
  }

  /**
   * Validates that a requested resource does not exceed available resource
   * and is non-negative for all resource dimensions.
   *
   * @complexity Time: O(r) where r=number of resource types (typically 2-10);
   *             iterates through each resource dimension for comparison.
   *             Space: O(1) - performs in-place validation with no allocations.
   *             Source: SchedulerUtils.java:425-443
   * @implNote Validation checks both negative values (invalid) and values
   *           exceeding maximum allocation. Unit conversion is handled
   *           internally via checkResource() when comparing resources with
   *           different units (e.g., MB vs GB).
   *
   * @param reqResource the requested resource to validate
   * @param availableResource the maximum available resource
   * @throws InvalidResourceRequestException if request is invalid
   */
  @Private
  @VisibleForTesting
  static void checkResourceRequestAgainstAvailableResource(Resource reqResource,
      Resource availableResource) throws InvalidResourceRequestException {
    // @PerformanceCritical: Resource validation loop executed for every container request
    for (int i = 0; i < ResourceUtils.getNumberOfCountableResourceTypes(); i++) {
      final ResourceInformation requestedRI =
          reqResource.getResourceInformation(i);
      final String reqResourceName = requestedRI.getName();

      if (requestedRI.getValue() < 0) {
        throwInvalidResourceException(reqResource, availableResource,
            reqResourceName, InvalidResourceType.LESS_THAN_ZERO);
      }

      boolean valid = checkResource(requestedRI, availableResource);
      if (!valid) {
        throwInvalidResourceException(reqResource, availableResource,
            reqResourceName, InvalidResourceType.GREATER_THEN_MAX_ALLOCATION);
      }
    }
  }

  /**
   * Validates a resource request against queue's maximum resource allocation,
   * identifying any resource dimensions that exceed allowed limits.
   *
   * @complexity Time: O(r) where r=number of resource types; iterates twice:
   *             once to build zero-resource map O(r), once to validate O(r).
   *             Space: O(r) for zero-resources map and invalid resources list.
   *             Source: SchedulerUtils.java:447-470
   * @implNote This validation is separate from global max allocation check.
   *           Queue-specific limits may be more restrictive. Returns detailed
   *           validation result rather than throwing exception to allow
   *           caller to handle partial failures gracefully.
   *
   * @param resReq resource request to validate
   * @param availableResource queue's maximum available resource
   * @return validation result containing any invalid resource dimensions
   * @throws SchedulerInvalidResourceRequestException for fatal validation errors
   */
  public static MaxResourceValidationResult
      validateResourceRequestsAgainstQueueMaxResource(
      ResourceRequest resReq, Resource availableResource)
      throws SchedulerInvalidResourceRequestException {
    final Resource reqResource = resReq.getCapability();
    Map<String, ResourceInformation> resourcesWithZeroAmount =
        getZeroResources(availableResource);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Resources with zero amount: "
          + Arrays.toString(resourcesWithZeroAmount.entrySet().toArray()));
    }

    List<ResourceInformation> invalidResources = Lists.newArrayList();
    for (int i = 0; i < ResourceUtils.getNumberOfCountableResourceTypes(); i++) {
      final ResourceInformation requestedRI =
          reqResource.getResourceInformation(i);
      final String reqResourceName = requestedRI.getName();

      if (resourcesWithZeroAmount.containsKey(reqResourceName)
          && requestedRI.getValue() > 0) {
        invalidResources.add(requestedRI);
      }
    }
    return new MaxResourceValidationResult(resReq, invalidResources);
  }

  /**
   * Checks requested ResourceInformation against available Resource.
   *
   * @complexity Time: O(1) for single resource dimension comparison;
   *             unit conversion via UnitsConversionUtil.convert() is O(1).
   *             Space: O(1) - uses only primitive local variables.
   *             Source: SchedulerUtils.java:478-517
   * @implNote Handles unit conversion transparently when comparing resources
   *           with different units (e.g., "m" vs "K" for memory, "Mi" vs "Gi"
   *           for storage). Converts the smaller unit to larger for comparison
   *           to avoid overflow issues with large values.
   *
   * @param requestedRI the requested resource information
   * @param availableResource the available resource to compare against
   * @return true if request is valid (within limits), false otherwise
   */
  private static boolean checkResource(
      ResourceInformation requestedRI, Resource availableResource) {
    final ResourceInformation availableRI =
        availableResource.getResourceInformation(requestedRI.getName());

    long requestedResourceValue = requestedRI.getValue();
    long availableResourceValue = availableRI.getValue();
    int unitsRelation = UnitsConversionUtil.compareUnits(requestedRI.getUnits(),
        availableRI.getUnits());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested resource information: " + requestedRI);
      LOG.debug("Available resource information: " + availableRI);
      LOG.debug("Relation of units: " + unitsRelation);
    }

    // requested resource unit is less than available resource unit
    // e.g. requestedUnit: "m", availableUnit: "K")
    if (unitsRelation < 0) {
      availableResourceValue =
          UnitsConversionUtil.convert(availableRI.getUnits(),
              requestedRI.getUnits(), availableRI.getValue());

      // requested resource unit is greater than available resource unit
      // e.g. requestedUnit: "G", availableUnit: "M")
    } else if (unitsRelation > 0) {
      requestedResourceValue =
          UnitsConversionUtil.convert(requestedRI.getUnits(),
              availableRI.getUnits(), requestedRI.getValue());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested resource value after conversion: "
          + requestedResourceValue);
      LOG.info("Available resource value after conversion: "
          + availableResourceValue);
    }

    return requestedResourceValue <= availableResourceValue;
  }

  /**
   * Throws an InvalidResourceRequestException with appropriate error message.
   *
   * @complexity Time: O(1) - string formatting and exception creation.
   *             Space: O(1) - creates single exception object with message string.
   *             Source: SchedulerUtils.java:519-544
   * @implNote Centralizes error message formatting for consistent exception
   *           handling. Message templates are shared static constants for
   *           memory efficiency and localization potential.
   *
   * @param reqResource the invalid requested resource
   * @param maxAllowedAllocation maximum allowed allocation for context
   * @param reqResourceName name of the specific invalid resource dimension
   * @param invalidResourceType the type of validation failure
   * @throws InvalidResourceRequestException always thrown with formatted message
   */
  private static void throwInvalidResourceException(Resource reqResource,
          Resource maxAllowedAllocation, String reqResourceName,
          InvalidResourceType invalidResourceType)
      throws InvalidResourceRequestException {
    final String message;

    if (invalidResourceType == InvalidResourceType.LESS_THAN_ZERO) {
      message = String.format(LESS_THAN_ZERO_RESOURCE_MESSAGE_TEMPLATE,
          reqResourceName, reqResource);
    } else if (invalidResourceType ==
            InvalidResourceType.GREATER_THEN_MAX_ALLOCATION) {
      message = String.format(GREATER_THAN_MAX_RESOURCE_MESSAGE_TEMPLATE,
          reqResourceName, reqResource, maxAllowedAllocation,
          ResourceUtils.getResourceTypesMaximumAllocation());
    } else if (invalidResourceType == InvalidResourceType.UNKNOWN) {
      message = String.format(UNKNOWN_REASON_MESSAGE_TEMPLATE, reqResourceName,
          reqResource);
    } else {
      throw new IllegalArgumentException(String.format(
          "InvalidResourceType argument should be either " + "%s, %s or %s",
          InvalidResourceType.LESS_THAN_ZERO,
          InvalidResourceType.GREATER_THEN_MAX_ALLOCATION,
          InvalidResourceType.UNKNOWN));
    }
    throw new InvalidResourceRequestException(message, invalidResourceType);
  }

  /**
   * Validates that a label expression exists in the cluster's label manager.
   *
   * @complexity Time: O(1) amortized; RMNodeLabelsManager.containsNodeLabel()
   *             uses HashSet lookup internally.
   *             Space: O(1) - no allocations.
   *             Source: SchedulerUtils.java:546-557
   * @implNote This check ensures labels used in requests actually exist in the
   *           cluster configuration. Missing labels indicate configuration errors
   *           that should be flagged early rather than causing scheduling failures.
   *
   * @param labelExpression the label to validate
   * @param rmContext RM context for accessing node label manager
   * @throws InvalidLabelResourceRequestException if label doesn't exist
   */
  private static void checkQueueLabelInLabelManager(String labelExpression,
      RMContext rmContext) throws InvalidLabelResourceRequestException {
    // check node label manager contains this label
    if (null != rmContext) {
      RMNodeLabelsManager nlm = rmContext.getNodeLabelManager();
      if (nlm != null && !nlm.containsNodeLabel(labelExpression)) {
        throw new InvalidLabelResourceRequestException(
            "Invalid label resource request, cluster do not contain "
                + ", label= " + labelExpression);
      }
    }
  }

  /**
   * Check queue label expression, check if node label in queue's
   * node-label-expression existed in clusterNodeLabels if rmContext != null.
   *
   * @complexity Time: O(L * q) where L=number of labels in expression (split on &&)
   *             and q=queue labels set size. For each label token, performs
   *             HashSet.contains() which is O(1) amortized, so effective O(L).
   *             Space: O(L) for split string array allocation.
   *             Source: SchedulerUtils.java:570-591
   * @implNote Uses HashSet for queueLabels to achieve O(1) amortized lookup
   *           per label check. The ANY label (*) acts as wildcard allowing
   *           any label expression. Label expressions with && are split and
   *           each component validated independently.
   *
   * @param queueLabels queue Labels.
   * @param labelExpression label expression.
   * @param rmContext rmContext.
   * @return true, if node label in queue's node-label-expression existed in clusterNodeLabels;
   * otherwise false.
   *
   */
  public static boolean checkQueueLabelExpression(Set<String> queueLabels,
      String labelExpression, RMContext rmContext) {
    // if label expression is empty, we can allocate container on any node
    if (labelExpression == null) {
      return true;
    }
    for (String str : labelExpression.split("&&")) {
      str = str.trim();
      if (!str.trim().isEmpty()) {
        // check queue label
        if (queueLabels == null) {
          return false;
        } else {
          if (!queueLabels.contains(str)
              && !queueLabels.contains(RMNodeLabelsManager.ANY)) {
            return false;
          }
        }
      }
    }
    return true;
  }


  /**
   * Converts QueueACL enum to AccessType enum for authorization checks.
   *
   * @complexity Time: O(1) - simple switch statement with constant branches.
   *             Space: O(1) - no allocations, returns existing enum constant.
   *             Source: SchedulerUtils.java:594-602
   *
   * @param acl the queue ACL to convert
   * @return corresponding AccessType or null if no mapping exists
   */
  public static AccessType toAccessType(QueueACL acl) {
    switch (acl) {
    case ADMINISTER_QUEUE:
      return AccessType.ADMINISTER_QUEUE;
    case SUBMIT_APPLICATIONS:
      return AccessType.SUBMIT_APP;
    }
    return null;
  }

  /**
   * Checks if there are pending resource requests for a given partition.
   *
   * @complexity Time: O(r) where r=number of resource types; comparison
   *             iterates through all resource dimensions.
   *             Space: O(1) - no allocations, uses existing Resource objects.
   *             Source: SchedulerUtils.java:604-611
   *
   * @param rc resource calculator for comparison
   * @param usage resource usage tracking object
   * @param partitionToLookAt partition to check for pending resources
   * @param cluster total cluster resource for normalization
   * @return true if pending resources exist, false otherwise
   */
  private static boolean hasPendingResourceRequest(ResourceCalculator rc,
      ResourceUsage usage, String partitionToLookAt, Resource cluster) {
    if (Resources.greaterThan(rc, cluster,
        usage.getPending(partitionToLookAt), Resources.none())) {
      return true;
    }
    return false;
  }

  /**
   * Checks if there are pending resource requests considering scheduling mode.
   *
   * @complexity Time: O(r) where r=number of resource types; delegates to
   *             private hasPendingResourceRequest with partition adjustment.
   *             Space: O(1) - no allocations, simple delegation.
   *             Source: SchedulerUtils.java:628-639
   * @implNote When schedulingMode is IGNORE_PARTITION_EXCLUSIVITY, checks
   *           pending for NO_LABEL partition instead of specified partition.
   *           This allows opportunistic scheduling across partitions.
   *
   * @param rc resource calculator for comparison
   * @param usage resource usage tracking object
   * @param nodePartition partition of the node being considered
   * @param cluster total cluster resource for normalization
   * @param schedulingMode scheduling mode affecting partition lookup
   * @return true if pending resources exist, false otherwise
   */
  @Private
  public static boolean hasPendingResourceRequest(ResourceCalculator rc,
      ResourceUsage usage, String nodePartition, Resource cluster,
      SchedulingMode schedulingMode) {
    String partitionToLookAt = nodePartition;
    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      partitionToLookAt = RMNodeLabelsManager.NO_LABEL;
    }
    return hasPendingResourceRequest(rc, usage, partitionToLookAt, cluster);
  }

  /**
   * Creates an RMContainer for opportunistic container allocation.
   *
   * @complexity Time: O(1) for container creation and node/attempt lookups
   *             (HashMap-based). addRMContainer and allocateContainer are O(1)
   *             for internal map operations.
   *             Space: O(1) for single RMContainerImpl allocation (~500 bytes).
   *             Source: SchedulerUtils.java:641-658
   * @implNote Opportunistic containers bypass capacity scheduling and are
   *           allocated directly to nodes with available resources. The
   *           isRemotelyAllocated flag indicates distributed scheduling.
   *           Returns null if node lookup fails (node may have been removed).
   *
   * @param rmContext RM context for scheduler access
   * @param container the container to wrap
   * @param isRemotelyAllocated whether container was allocated by distributed scheduler
   * @return RMContainer wrapper or null if node not found
   */
  public static RMContainer createOpportunisticRmContainer(RMContext rmContext,
      Container container, boolean isRemotelyAllocated) {
    SchedulerNode node = ((AbstractYarnScheduler) rmContext.getScheduler())
        .getNode(container.getNodeId());
    if (node == null) {
      return null;
    }
    SchedulerApplicationAttempt appAttempt =
        ((AbstractYarnScheduler) rmContext.getScheduler())
            .getCurrentAttemptForContainer(container.getId());
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container),
        appAttempt.getApplicationAttemptId(), container.getNodeId(),
        appAttempt.getUser(), rmContext, isRemotelyAllocated);
    appAttempt.addRMContainer(container.getId(), rmContainer);
    node.allocateContainer(rmContainer);
    return rmContainer;
  }

  /**
   * Checks if a node has heartbeated within the specified interval.
   *
   * @complexity Time: O(1) - simple arithmetic comparison with monotonic time.
   *             Space: O(1) - uses only primitive local variable.
   *             Source: SchedulerUtils.java:660-665
   * @implNote Uses monotonic time to avoid issues with system clock adjustments.
   *           Nodes that haven't heartbeated within skipNodeInterval are
   *           considered stale and may be skipped during scheduling to avoid
   *           allocating to potentially dead nodes.
   *
   * @param node the scheduler node to check
   * @param skipNodeInterval maximum allowed time since last heartbeat (ms)
   * @return true if node heartbeated within interval, false if stale
   */
  public static boolean isNodeHeartbeated(SchedulerNode node,
      long skipNodeInterval) {
    long timeElapsedFromLastHeartbeat =
        Time.monotonicNow() - node.getLastHeartbeatMonotonicTime();
    return timeElapsedFromLastHeartbeat <= skipNodeInterval;
  }
}
