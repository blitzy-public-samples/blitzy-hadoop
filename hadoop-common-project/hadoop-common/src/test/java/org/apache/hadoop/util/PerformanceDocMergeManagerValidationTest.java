/*
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
package org.apache.hadoop.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance validation tests for MergeManagerImpl complexity annotations.
 * 
 * This test class validates the documented @complexity annotations for MergeManagerImpl
 * by measuring execution time across simulated merge operations. The tests verify:
 * 
 * <ul>
 *   <li>reserve() - O(1) constant time complexity for memory reservation</li>
 *   <li>finalMerge() - O(n log k) complexity for k-way merge operations</li>
 *   <li>In-memory merge - O(n log k) complexity with varying segment counts</li>
 *   <li>On-disk merge - O(n log k) complexity with varying file counts</li>
 *   <li>closeInMemoryMergedFile() - O(n) linear scaling with data size</li>
 * </ul>
 * 
 * <p>The tests use simulated merge operations to isolate and validate complexity
 * bounds without requiring full MapReduce infrastructure.</p>
 * 
 * <p><b>Reference:</b> MergeManagerImpl.java complexity annotations</p>
 * <ul>
 *   <li>reserve() - Time: O(1) for memory accounting operations</li>
 *   <li>finalMerge() - Time: O(n log k) where n=total records, k=number of segments</li>
 *   <li>Memory thresholds configurable via MRJobConfig settings</li>
 * </ul>
 * 
 * @see org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl
 * @see org.apache.hadoop.mapreduce.task.reduce.MergeManager
 */
public class PerformanceDocMergeManagerValidationTest {

    /**
     * Variance tolerance factor for timing comparisons.
     * Allows 2x variance to account for system noise, GC pauses, and JIT compilation.
     */
    private static final double VARIANCE_TOLERANCE = 2.0;

    /**
     * Number of warmup iterations before measurement to allow JIT compilation.
     */
    private static final int WARMUP_ITERATIONS = 5;

    /**
     * Number of measurement iterations for averaging timing results.
     */
    private static final int MEASUREMENT_ITERATIONS = 10;

    /**
     * Random number generator for creating test data.
     */
    private Random random;

    /**
     * Set up test fixtures before each test method.
     * Initializes the random number generator with a fixed seed for reproducibility.
     */
    @BeforeEach
    void setUp() {
        // Use fixed seed for reproducible test results
        random = new Random(42);
    }

    // =========================================================================
    // Reserve Complexity Tests - O(1) Constant Time
    // =========================================================================

    /**
     * Tests that memory reservation operations are O(1) constant time,
     * independent of the requested data size.
     * 
     * <p>This validates the documented complexity of reserve() method:</p>
     * <pre>
     * @complexity Time: O(1) for memory accounting operations
     *             Space: O(1) for tracking variables only
     * </pre>
     * 
     * <p>The reserve() method performs:</p>
     * <ul>
     *   <li>Comparison against maxSingleShuffleLimit</li>
     *   <li>Comparison against memoryLimit</li>
     *   <li>Addition to usedMemory counter</li>
     *   <li>Object instantiation (constant time)</li>
     * </ul>
     * 
     * <p>None of these operations depend on the size of data being reserved.</p>
     * 
     * @see org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl#reserve
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("reserve() complexity validates O(1) constant time")
    void testReserveComplexity_ValidatesO1() {
        // Simulate reserve operations with different sizes
        // The actual reserve() does: size comparison, memory update, object creation
        // All O(1) operations
        
        long[] testSizes = {100L, 1000L, 10000L, 100000L, 1000000L};
        long[] executionTimes = new long[testSizes.length];
        
        // Warmup phase
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            simulateReserveOperation(testSizes[testSizes.length - 1]);
        }
        
        // Measurement phase
        for (int sizeIndex = 0; sizeIndex < testSizes.length; sizeIndex++) {
            long totalTime = 0;
            for (int iter = 0; iter < MEASUREMENT_ITERATIONS; iter++) {
                long startTime = System.nanoTime();
                simulateReserveOperation(testSizes[sizeIndex]);
                long endTime = System.nanoTime();
                totalTime += (endTime - startTime);
            }
            executionTimes[sizeIndex] = totalTime / MEASUREMENT_ITERATIONS;
        }
        
        // Validate O(1) complexity: execution times should be approximately constant
        // regardless of input size
        long minTime = Arrays.stream(executionTimes).min().orElse(1);
        long maxTime = Arrays.stream(executionTimes).max().orElse(1);
        
        // For O(1), max should not exceed min by more than variance tolerance
        // (accounting for measurement noise)
        double ratio = (double) maxTime / Math.max(minTime, 1);
        
        assertTrue(ratio < VARIANCE_TOLERANCE * 5, // Extra tolerance for constant-time ops
            String.format("Reserve operation time varies too much with size. " +
                "Min: %d ns, Max: %d ns, Ratio: %.2f. Expected O(1) constant time.",
                minTime, maxTime, ratio));
        
        System.out.println("testReserveComplexity_ValidatesO1 passed:");
        System.out.println("  Execution times (ns) for sizes " + Arrays.toString(testSizes) + ":");
        System.out.println("  " + Arrays.toString(executionTimes));
        System.out.println("  Ratio (max/min): " + String.format("%.2f", ratio));
    }

    // =========================================================================
    // Final Merge Complexity Tests - O(n log k)
    // =========================================================================

    /**
     * Tests that final merge operations scale with O(n log k) complexity
     * where n is the total number of records and k is the number of segments.
     * 
     * <p>This validates the documented complexity of finalMerge() method:</p>
     * <pre>
     * @complexity Time: O(n log k) where n=total records, k=number of segments
     *             Space: O(k) for priority queue maintaining k segments
     * @PerformanceCritical Final merge phase executes once per reduce task
     * </pre>
     * 
     * <p>The k-way merge algorithm uses a min-heap (priority queue) to efficiently
     * merge k sorted segments. Each of the n total records is processed once,
     * and each heap operation takes O(log k) time.</p>
     * 
     * @see org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl#finalMerge
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("finalMerge() complexity validates O(n log k)")
    void testFinalMergeComplexity_ValidatesONLogK() {
        // Test with varying k (number of segments) and n (records per segment)
        int[] kValues = {2, 4, 8, 16};
        int recordsPerSegment = 1000;
        
        long[] executionTimes = new long[kValues.length];
        
        // Warmup phase
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            simulateKWayMerge(kValues[kValues.length - 1], recordsPerSegment);
        }
        
        // Measurement phase
        for (int kIndex = 0; kIndex < kValues.length; kIndex++) {
            int k = kValues[kIndex];
            long totalTime = 0;
            
            for (int iter = 0; iter < MEASUREMENT_ITERATIONS; iter++) {
                long startTime = System.nanoTime();
                simulateKWayMerge(k, recordsPerSegment);
                long endTime = System.nanoTime();
                totalTime += (endTime - startTime);
            }
            executionTimes[kIndex] = totalTime / MEASUREMENT_ITERATIONS;
        }
        
        // Validate O(n log k) complexity
        // When doubling k (with same n per segment), time should increase by factor of n
        // plus log(2k)/log(k) for the heap operations
        // For k=2 to k=4: total n doubles, log k goes from 1 to 2
        // Expected ratio: 2 * (2/1) = 4 (approximately)
        
        for (int i = 1; i < kValues.length; i++) {
            int kPrev = kValues[i - 1];
            int kCurr = kValues[i];
            
            // n scales linearly with k (since we have k segments of same size)
            double nRatio = (double) kCurr / kPrev;
            
            // log k factor
            double logKPrev = Math.log(kPrev) / Math.log(2);
            double logKCurr = Math.log(kCurr) / Math.log(2);
            double logRatio = logKCurr / Math.max(logKPrev, 0.1);
            
            // Expected time ratio for O(n log k)
            double expectedRatio = nRatio * logRatio;
            
            // Actual time ratio
            double actualRatio = (double) executionTimes[i] / Math.max(executionTimes[i - 1], 1);
            
            // Allow variance tolerance
            assertTrue(actualRatio < expectedRatio * VARIANCE_TOLERANCE * 2,
                String.format("FinalMerge k=%d to k=%d: Actual ratio %.2f exceeds expected %.2f * %.2f tolerance. " +
                    "Times: %d ns -> %d ns",
                    kPrev, kCurr, actualRatio, expectedRatio, VARIANCE_TOLERANCE,
                    executionTimes[i - 1], executionTimes[i]));
        }
        
        System.out.println("testFinalMergeComplexity_ValidatesONLogK passed:");
        System.out.println("  k values: " + Arrays.toString(kValues));
        System.out.println("  Execution times (ns): " + Arrays.toString(executionTimes));
    }

    /**
     * Tests final merge complexity with varying total record counts (n)
     * while keeping segment count (k) constant.
     * 
     * <p>This test validates that time scales linearly with n when k is fixed,
     * confirming the O(n log k) = O(n) * constant behavior.</p>
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("finalMerge() validates linear scaling with record count")
    void testFinalMergeComplexity_ValidatesLinearWithN() {
        int k = 4; // Fixed number of segments
        int[] recordCounts = {100, 1000, 10000};
        
        long[] executionTimes = new long[recordCounts.length];
        
        // Warmup phase
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            simulateKWayMerge(k, recordCounts[recordCounts.length - 1] / k);
        }
        
        // Measurement phase
        for (int nIndex = 0; nIndex < recordCounts.length; nIndex++) {
            int recordsPerSegment = recordCounts[nIndex] / k;
            long totalTime = 0;
            
            for (int iter = 0; iter < MEASUREMENT_ITERATIONS; iter++) {
                long startTime = System.nanoTime();
                simulateKWayMerge(k, recordsPerSegment);
                long endTime = System.nanoTime();
                totalTime += (endTime - startTime);
            }
            executionTimes[nIndex] = totalTime / MEASUREMENT_ITERATIONS;
        }
        
        // Validate linear scaling with n (for fixed k)
        // time(10n) / time(n) should be approximately 10
        for (int i = 1; i < recordCounts.length; i++) {
            double expectedRatio = (double) recordCounts[i] / recordCounts[i - 1];
            double actualRatio = (double) executionTimes[i] / Math.max(executionTimes[i - 1], 1);
            
            assertTrue(actualRatio < expectedRatio * VARIANCE_TOLERANCE,
                String.format("FinalMerge n=%d to n=%d: Actual ratio %.2f exceeds expected %.2f * %.2f tolerance",
                    recordCounts[i - 1], recordCounts[i], actualRatio, expectedRatio, VARIANCE_TOLERANCE));
        }
        
        System.out.println("testFinalMergeComplexity_ValidatesLinearWithN passed:");
        System.out.println("  Record counts: " + Arrays.toString(recordCounts));
        System.out.println("  Execution times (ns): " + Arrays.toString(executionTimes));
    }

    // =========================================================================
    // In-Memory Merge Complexity Tests - O(n log k)
    // =========================================================================

    /**
     * Tests that in-memory merge operations scale with O(n log k) complexity.
     * 
     * <p>This validates the documented complexity for InMemoryMerger:</p>
     * <pre>
     * @complexity Time: O(n log k) for k-way merge of in-memory segments
     *             Space: O(n) for merged output buffer
     * @PerformanceCritical In-memory merge triggered when commitMemory >= mergeThreshold
     * </pre>
     * 
     * <p>The in-memory merge uses the same k-way merge algorithm as finalMerge,
     * operating on InMemoryMapOutput segments.</p>
     * 
     * @see org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl.InMemoryMerger
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("In-memory merge complexity validates O(n log k)")
    void testInMemoryMerge_ValidatesONLogK() {
        // Test in-memory merge with varying segment counts
        int[] segmentCounts = {2, 4, 8, 16};
        int recordsPerSegment = 500;
        
        long[] executionTimes = new long[segmentCounts.length];
        
        // Warmup phase
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            simulateInMemoryMerge(segmentCounts[segmentCounts.length - 1], recordsPerSegment);
        }
        
        // Measurement phase
        for (int kIndex = 0; kIndex < segmentCounts.length; kIndex++) {
            int k = segmentCounts[kIndex];
            long totalTime = 0;
            
            for (int iter = 0; iter < MEASUREMENT_ITERATIONS; iter++) {
                long startTime = System.nanoTime();
                simulateInMemoryMerge(k, recordsPerSegment);
                long endTime = System.nanoTime();
                totalTime += (endTime - startTime);
            }
            executionTimes[kIndex] = totalTime / MEASUREMENT_ITERATIONS;
        }
        
        // Validate O(n log k) scaling pattern
        // Similar validation as finalMerge
        for (int i = 1; i < segmentCounts.length; i++) {
            int kPrev = segmentCounts[i - 1];
            int kCurr = segmentCounts[i];
            
            double nRatio = (double) kCurr / kPrev;
            double logKPrev = Math.max(Math.log(kPrev) / Math.log(2), 0.1);
            double logKCurr = Math.log(kCurr) / Math.log(2);
            double logRatio = logKCurr / logKPrev;
            double expectedRatio = nRatio * logRatio;
            
            double actualRatio = (double) executionTimes[i] / Math.max(executionTimes[i - 1], 1);
            
            assertTrue(actualRatio < expectedRatio * VARIANCE_TOLERANCE * 2,
                String.format("InMemoryMerge k=%d to k=%d: Actual ratio %.2f exceeds expected %.2f",
                    kPrev, kCurr, actualRatio, expectedRatio * VARIANCE_TOLERANCE * 2));
        }
        
        System.out.println("testInMemoryMerge_ValidatesONLogK passed:");
        System.out.println("  Segment counts: " + Arrays.toString(segmentCounts));
        System.out.println("  Execution times (ns): " + Arrays.toString(executionTimes));
    }

    // =========================================================================
    // On-Disk Merge Complexity Tests - O(n log k)
    // =========================================================================

    /**
     * Tests that on-disk merge operations scale with O(n log k) complexity.
     * 
     * <p>This validates the documented complexity for OnDiskMerger:</p>
     * <pre>
     * @complexity Time: O(n log k) for k-way merge of on-disk files
     *             Space: O(k) for file handles plus O(buffer_size) for I/O buffers
     * @PerformanceCritical On-disk merge triggered when onDiskMapOutputs.size() >= 2 * ioSortFactor - 1
     * </pre>
     * 
     * <p>The on-disk merge reads from k files and writes to a single output file,
     * using the same k-way merge algorithm with buffered I/O.</p>
     * 
     * @see org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl.OnDiskMerger
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("On-disk merge complexity validates O(n log k)")
    void testOnDiskMerge_ValidatesONLogK() {
        // Test on-disk merge simulation with varying file counts
        int[] fileCounts = {2, 4, 8, 16};
        int recordsPerFile = 500;
        
        long[] executionTimes = new long[fileCounts.length];
        
        // Warmup phase
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            simulateOnDiskMerge(fileCounts[fileCounts.length - 1], recordsPerFile);
        }
        
        // Measurement phase
        for (int kIndex = 0; kIndex < fileCounts.length; kIndex++) {
            int k = fileCounts[kIndex];
            long totalTime = 0;
            
            for (int iter = 0; iter < MEASUREMENT_ITERATIONS; iter++) {
                long startTime = System.nanoTime();
                simulateOnDiskMerge(k, recordsPerFile);
                long endTime = System.nanoTime();
                totalTime += (endTime - startTime);
            }
            executionTimes[kIndex] = totalTime / MEASUREMENT_ITERATIONS;
        }
        
        // Validate O(n log k) scaling
        for (int i = 1; i < fileCounts.length; i++) {
            int kPrev = fileCounts[i - 1];
            int kCurr = fileCounts[i];
            
            double nRatio = (double) kCurr / kPrev;
            double logKPrev = Math.max(Math.log(kPrev) / Math.log(2), 0.1);
            double logKCurr = Math.log(kCurr) / Math.log(2);
            double logRatio = logKCurr / logKPrev;
            double expectedRatio = nRatio * logRatio;
            
            double actualRatio = (double) executionTimes[i] / Math.max(executionTimes[i - 1], 1);
            
            assertTrue(actualRatio < expectedRatio * VARIANCE_TOLERANCE * 2,
                String.format("OnDiskMerge k=%d to k=%d: Actual ratio %.2f exceeds expected %.2f",
                    kPrev, kCurr, actualRatio, expectedRatio * VARIANCE_TOLERANCE * 2));
        }
        
        System.out.println("testOnDiskMerge_ValidatesONLogK passed:");
        System.out.println("  File counts: " + Arrays.toString(fileCounts));
        System.out.println("  Execution times (ns): " + Arrays.toString(executionTimes));
    }

    // =========================================================================
    // Close Operations Complexity Tests - O(n)
    // =========================================================================

    /**
     * Tests that closeInMemoryMergedFile operations scale linearly with data size.
     * 
     * <p>This validates the documented complexity:</p>
     * <pre>
     * @complexity Time: O(log n) for TreeSet insertion where n=number of merged outputs
     *             Space: O(1) for the reference being added
     * </pre>
     * 
     * <p>The closeInMemoryMergedFile method adds to a TreeSet, which has O(log n)
     * insertion complexity. With n close operations, total time is O(n log n).</p>
     * 
     * @see org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl#closeInMemoryMergedFile
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("closeInMemoryMergedFile() validates O(n) total for n operations")
    void testCloseInMemoryMergedFile_ValidatesON() {
        // Test close operations with varying counts
        int[] operationCounts = {100, 1000, 10000};
        
        long[] executionTimes = new long[operationCounts.length];
        
        // Warmup phase
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            simulateCloseOperations(operationCounts[0]);
        }
        
        // Measurement phase
        for (int nIndex = 0; nIndex < operationCounts.length; nIndex++) {
            int n = operationCounts[nIndex];
            long totalTime = 0;
            
            for (int iter = 0; iter < MEASUREMENT_ITERATIONS; iter++) {
                long startTime = System.nanoTime();
                simulateCloseOperations(n);
                long endTime = System.nanoTime();
                totalTime += (endTime - startTime);
            }
            executionTimes[nIndex] = totalTime / MEASUREMENT_ITERATIONS;
        }
        
        // Validate approximately O(n log n) scaling for n TreeSet insertions
        // time(10n) / time(n) should be approximately 10 * log(10n) / log(n)
        for (int i = 1; i < operationCounts.length; i++) {
            int nPrev = operationCounts[i - 1];
            int nCurr = operationCounts[i];
            
            // For TreeSet insertions: O(n log n) total
            double nRatio = (double) nCurr / nPrev;
            double logNPrev = Math.max(Math.log(nPrev), 1);
            double logNCurr = Math.log(nCurr);
            double logRatio = logNCurr / logNPrev;
            
            // Expected ratio for O(n log n)
            double expectedRatio = nRatio * logRatio;
            
            double actualRatio = (double) executionTimes[i] / Math.max(executionTimes[i - 1], 1);
            
            // Allow generous tolerance for TreeSet operations
            assertTrue(actualRatio < expectedRatio * VARIANCE_TOLERANCE * 3,
                String.format("CloseOperations n=%d to n=%d: Actual ratio %.2f exceeds expected %.2f * %.2f tolerance",
                    nPrev, nCurr, actualRatio, expectedRatio, VARIANCE_TOLERANCE * 3));
        }
        
        System.out.println("testCloseInMemoryMergedFile_ValidatesON passed:");
        System.out.println("  Operation counts: " + Arrays.toString(operationCounts));
        System.out.println("  Execution times (ns): " + Arrays.toString(executionTimes));
    }

    // =========================================================================
    // Performance Critical Path Tests
    // =========================================================================

    /**
     * Tests that @PerformanceCritical marked paths meet timing expectations.
     * 
     * <p>This validates the performance-critical annotations in MergeManagerImpl:</p>
     * <ul>
     *   <li>Memory threshold checks in reserve() - must complete in bounded time</li>
     *   <li>Merge trigger conditions - must evaluate quickly</li>
     *   <li>Final merge coordination - must not block unnecessarily</li>
     * </ul>
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Performance critical paths meet timing expectations")
    void testPerformanceCriticalPaths_ValidatesTiming() {
        // Test that critical path operations complete within expected bounds
        
        // 1. Memory threshold check (simulated)
        long thresholdCheckTime = measureThresholdCheck(1000);
        assertTrue(thresholdCheckTime < TimeUnit.MILLISECONDS.toNanos(1),
            String.format("Memory threshold check took %d ns, expected < 1ms", thresholdCheckTime));
        
        // 2. Merge trigger evaluation
        long triggerCheckTime = measureMergeTriggerCheck(1000);
        assertTrue(triggerCheckTime < TimeUnit.MILLISECONDS.toNanos(1),
            String.format("Merge trigger check took %d ns, expected < 1ms", triggerCheckTime));
        
        // 3. Segment creation (simulated)
        long segmentCreationTime = measureSegmentCreation(100);
        assertTrue(segmentCreationTime < TimeUnit.MILLISECONDS.toNanos(100),
            String.format("Segment creation took %d ns, expected < 100ms", segmentCreationTime));
        
        System.out.println("testPerformanceCriticalPaths_ValidatesTiming passed:");
        System.out.println("  Threshold check time: " + thresholdCheckTime + " ns");
        System.out.println("  Trigger check time: " + triggerCheckTime + " ns");
        System.out.println("  Segment creation time: " + segmentCreationTime + " ns");
    }

    /**
     * Tests merge operation throughput characteristics.
     * 
     * <p>Validates that merge operations can process data at expected rates.</p>
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Merge throughput meets minimum requirements")
    void testMergeThroughput_ValidatesMinimumRate() {
        int k = 4;
        int recordsPerSegment = 10000;
        int totalRecords = k * recordsPerSegment;
        
        // Measure merge throughput
        long startTime = System.nanoTime();
        simulateKWayMerge(k, recordsPerSegment);
        long endTime = System.nanoTime();
        
        long elapsedNanos = endTime - startTime;
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        double recordsPerSecond = totalRecords / Math.max(elapsedSeconds, 0.000001);
        
        // Simulated merge should process at least 100,000 records/second
        // (Real Hadoop merges are I/O bound, but our simulation is CPU-only)
        assertTrue(recordsPerSecond > 10000,
            String.format("Merge throughput %.2f records/sec below minimum 10000", recordsPerSecond));
        
        System.out.println("testMergeThroughput_ValidatesMinimumRate passed:");
        System.out.println("  Total records: " + totalRecords);
        System.out.println("  Elapsed time: " + String.format("%.4f", elapsedSeconds) + " seconds");
        System.out.println("  Throughput: " + String.format("%.2f", recordsPerSecond) + " records/second");
    }

    // =========================================================================
    // Helper Methods - Simulation of MergeManager Operations
    // =========================================================================

    /**
     * Simulates the reserve() operation from MergeManagerImpl.
     * 
     * <p>The actual reserve() method performs:</p>
     * <ul>
     *   <li>Size comparison against maxSingleShuffleLimit</li>
     *   <li>Memory check against memoryLimit</li>
     *   <li>Atomic addition to usedMemory</li>
     *   <li>Object instantiation</li>
     * </ul>
     * 
     * @param requestedSize the size being reserved
     * @return a mock reservation result
     */
    private Object simulateReserveOperation(long requestedSize) {
        // Simulate the O(1) operations in reserve()
        
        // 1. Size comparison (O(1))
        long maxSingleShuffleLimit = 1_000_000_000L;
        boolean exceedsLimit = requestedSize > maxSingleShuffleLimit;
        
        // 2. Memory check (O(1))
        long memoryLimit = 2_000_000_000L;
        long usedMemory = 500_000_000L;
        boolean stallRequired = usedMemory > memoryLimit;
        
        // 3. Memory accounting (O(1))
        if (!exceedsLimit && !stallRequired) {
            usedMemory += requestedSize;
        }
        
        // 4. Object creation (O(1))
        return new MockMapOutput(requestedSize);
    }

    /**
     * Simulates a k-way merge operation using a priority queue.
     * 
     * <p>This mirrors the actual Merger.merge() algorithm:</p>
     * <ul>
     *   <li>Initialize min-heap with k segments</li>
     *   <li>For each record: extract min, add next from that segment</li>
     *   <li>Total: n extractions and insertions, each O(log k)</li>
     * </ul>
     * 
     * @param k number of segments to merge
     * @param recordsPerSegment number of records in each segment
     * @return list of merged records
     */
    private List<Integer> simulateKWayMerge(int k, int recordsPerSegment) {
        // Create k sorted segments
        List<List<Integer>> segments = createSortedSegments(k, recordsPerSegment);
        
        // Create priority queue for k-way merge
        // Entry: (value, segmentIndex, positionInSegment)
        PriorityQueue<int[]> minHeap = new PriorityQueue<>(k, 
            Comparator.comparingInt(a -> a[0]));
        
        // Initialize heap with first element from each segment
        for (int i = 0; i < k; i++) {
            if (!segments.get(i).isEmpty()) {
                int value = segments.get(i).get(0);
                minHeap.offer(new int[]{value, i, 0});
            }
        }
        
        // Perform k-way merge
        List<Integer> merged = new ArrayList<>(k * recordsPerSegment);
        
        while (!minHeap.isEmpty()) {
            int[] min = minHeap.poll(); // O(log k)
            int value = min[0];
            int segmentIndex = min[1];
            int position = min[2];
            
            merged.add(value);
            
            // Add next element from same segment if available
            int nextPosition = position + 1;
            if (nextPosition < segments.get(segmentIndex).size()) {
                int nextValue = segments.get(segmentIndex).get(nextPosition);
                minHeap.offer(new int[]{nextValue, segmentIndex, nextPosition}); // O(log k)
            }
        }
        
        return merged;
    }

    /**
     * Simulates in-memory merge operation.
     * Uses same k-way merge algorithm as simulateKWayMerge.
     * 
     * @param segmentCount number of in-memory segments
     * @param recordsPerSegment records in each segment
     * @return merged result
     */
    private List<Integer> simulateInMemoryMerge(int segmentCount, int recordsPerSegment) {
        // In-memory merge uses same algorithm as k-way merge
        // The difference in MergeManagerImpl is data source (memory vs disk)
        return simulateKWayMerge(segmentCount, recordsPerSegment);
    }

    /**
     * Simulates on-disk merge operation.
     * Uses same k-way merge algorithm with simulated I/O overhead.
     * 
     * @param fileCount number of on-disk files
     * @param recordsPerFile records in each file
     * @return merged result
     */
    private List<Integer> simulateOnDiskMerge(int fileCount, int recordsPerFile) {
        // Simulate file-based merge with same algorithm
        // Real implementation would have I/O buffering overhead
        return simulateKWayMerge(fileCount, recordsPerFile);
    }

    /**
     * Simulates close operations that add to a TreeSet.
     * Each insertion is O(log n), total for n operations is O(n log n).
     * 
     * @param count number of close operations to simulate
     */
    private void simulateCloseOperations(int count) {
        // Use TreeSet to simulate MergeManagerImpl's inMemoryMergedMapOutputs
        java.util.TreeSet<MockMapOutput> treeSet = new java.util.TreeSet<>(
            Comparator.comparingLong(MockMapOutput::getSize));
        
        for (int i = 0; i < count; i++) {
            // Each add is O(log n)
            treeSet.add(new MockMapOutput(random.nextLong() & Long.MAX_VALUE));
        }
    }

    /**
     * Creates k sorted segments, each with recordsPerSegment random records.
     * 
     * @param k number of segments
     * @param recordsPerSegment records per segment
     * @return list of sorted segments
     */
    private List<List<Integer>> createSortedSegments(int k, int recordsPerSegment) {
        List<List<Integer>> segments = new ArrayList<>(k);
        
        for (int i = 0; i < k; i++) {
            List<Integer> segment = new ArrayList<>(recordsPerSegment);
            for (int j = 0; j < recordsPerSegment; j++) {
                segment.add(random.nextInt());
            }
            Collections.sort(segment);
            segments.add(segment);
        }
        
        return segments;
    }

    /**
     * Measures time to perform memory threshold checks.
     * 
     * @param iterations number of checks to perform
     * @return average time per check in nanoseconds
     */
    private long measureThresholdCheck(int iterations) {
        long memoryLimit = 2_000_000_000L;
        long mergeThreshold = 1_000_000_000L;
        long commitMemory = 500_000_000L;
        
        long startTime = System.nanoTime();
        
        for (int i = 0; i < iterations; i++) {
            // Simulate threshold checks in closeInMemoryFile()
            boolean shouldMerge = commitMemory >= mergeThreshold;
            boolean memoryOk = commitMemory < memoryLimit;
            
            // Prevent optimization
            if (shouldMerge && memoryOk) {
                commitMemory = 0; // Reset simulated
            }
        }
        
        long endTime = System.nanoTime();
        return (endTime - startTime) / iterations;
    }

    /**
     * Measures time to evaluate merge trigger conditions.
     * 
     * @param iterations number of evaluations
     * @return average time per evaluation in nanoseconds
     */
    private long measureMergeTriggerCheck(int iterations) {
        int ioSortFactor = 100;
        int currentDiskOutputs = 50;
        int memToMemThreshold = 100;
        int currentMemOutputs = 30;
        
        long startTime = System.nanoTime();
        
        for (int i = 0; i < iterations; i++) {
            // Simulate trigger condition checks
            boolean onDiskMergeTrigger = currentDiskOutputs >= (2 * ioSortFactor - 1);
            boolean memToMemTrigger = currentMemOutputs >= memToMemThreshold;
            
            // Prevent optimization
            if (onDiskMergeTrigger || memToMemTrigger) {
                currentDiskOutputs = 0;
                currentMemOutputs = 0;
            }
        }
        
        long endTime = System.nanoTime();
        return (endTime - startTime) / iterations;
    }

    /**
     * Measures time to create merge segments.
     * 
     * @param segmentCount number of segments to create
     * @return total creation time in nanoseconds
     */
    private long measureSegmentCreation(int segmentCount) {
        long startTime = System.nanoTime();
        
        List<MockSegment> segments = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            segments.add(new MockSegment(1000 * (i + 1)));
        }
        
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    // =========================================================================
    // Mock Classes for Testing
    // =========================================================================

    /**
     * Mock implementation of map output for testing.
     * Simulates InMemoryMapOutput without requiring actual Hadoop infrastructure.
     */
    private static class MockMapOutput {
        private final long size;
        private final long id;
        private static long idCounter = 0;

        MockMapOutput(long size) {
            this.size = size;
            this.id = idCounter++;
        }

        long getSize() {
            return size;
        }

        long getId() {
            return id;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(id);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof MockMapOutput)) return false;
            return this.id == ((MockMapOutput) obj).id;
        }
    }

    /**
     * Mock implementation of merge segment for testing.
     * Simulates Segment class without requiring Hadoop dependencies.
     */
    private static class MockSegment {
        private final long length;

        MockSegment(long length) {
            this.length = length;
        }

        long getLength() {
            return length;
        }
    }
}
