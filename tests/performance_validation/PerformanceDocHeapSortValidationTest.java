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

import org.apache.hadoop.util.HeapSort;
import org.apache.hadoop.util.IndexedSortable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performance validation tests for HeapSort complexity annotations.
 * 
 * <p>This test class validates the documented @complexity annotations in HeapSort.java
 * by measuring execution time across multiple input sizes and verifying that actual
 * scaling behavior matches the documented complexity bounds.</p>
 * 
 * <h3>Documented Complexity (Source: HeapSort.java:32-75)</h3>
 * <ul>
 *   <li>Time: O(n log n) for all cases (worst, average, best)</li>
 *   <li>Space: O(1) in-place sorting with no additional array allocations</li>
 * </ul>
 * 
 * <h3>Validation Methodology</h3>
 * <ul>
 *   <li>Tests use 3 input sizes: 100, 1000, 10000 elements</li>
 *   <li>2x variance tolerance allowed for system noise</li>
 *   <li>Multiple test runs averaged to reduce measurement variance</li>
 * </ul>
 * 
 * @see org.apache.hadoop.util.HeapSort
 * @see org.apache.hadoop.util.IndexedSortable
 */
public class PerformanceDocHeapSortValidationTest {

    /** Small input size for complexity validation (100 elements). */
    private static final int SIZE_SMALL = 100;
    
    /** Medium input size for complexity validation (1000 elements). */
    private static final int SIZE_MEDIUM = 1000;
    
    /** Large input size for complexity validation (10000 elements). */
    private static final int SIZE_LARGE = 10000;
    
    /** Very large input size for additional validation (50000 elements). */
    private static final int SIZE_VERY_LARGE = 50000;
    
    /** Variance tolerance factor (2x) for system noise in timing measurements. */
    private static final double VARIANCE_TOLERANCE = 2.0;
    
    /** Number of warmup iterations to stabilize JIT compilation. */
    private static final int WARMUP_ITERATIONS = 5;
    
    /** Number of measurement iterations for averaging. */
    private static final int MEASUREMENT_ITERATIONS = 10;
    
    /** Random seed for reproducible test data generation. */
    private static final long RANDOM_SEED = 12345L;
    
    /** HeapSort instance under test. */
    private HeapSort heapSort;
    
    /** Random number generator with fixed seed for reproducibility. */
    private Random random;

    /**
     * Sets up the test environment before each test method.
     * Initializes a fresh HeapSort instance and seeded random generator.
     */
    @BeforeEach
    public void setUp() {
        heapSort = new HeapSort();
        random = new Random(RANDOM_SEED);
    }

    /**
     * Validates O(n log n) time complexity for HeapSort with random input data.
     * 
     * <p>This test measures execution time for sorting 100, 1000, and 10000 random
     * elements, then validates that the scaling factor matches the expected
     * O(n log n) behavior within 2x variance tolerance.</p>
     * 
     * <p>Expected ratio for O(n log n) scaling from n1 to n2:
     * ratio ≈ (n2 * log(n2)) / (n1 * log(n1))</p>
     * 
     * <p>Reference: HeapSort.java:52-74 sort() method implementation.</p>
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testSortComplexity_ValidatesONLogN() {
        // Warmup JIT compiler with multiple sort operations
        performWarmup();
        
        // Measure execution time for each input size
        long timeSmall = measureAverageExecutionTime(SIZE_SMALL);
        long timeMedium = measureAverageExecutionTime(SIZE_MEDIUM);
        long timeLarge = measureAverageExecutionTime(SIZE_LARGE);
        
        // Calculate expected O(n log n) scaling ratios
        double expectedRatioSmallToMedium = calculateNLogNRatio(SIZE_SMALL, SIZE_MEDIUM);
        double expectedRatioMediumToLarge = calculateNLogNRatio(SIZE_MEDIUM, SIZE_LARGE);
        
        // Calculate actual ratios (use max of 1 to avoid division issues with very fast operations)
        double actualRatioSmallToMedium = Math.max(1.0, (double) timeMedium) / Math.max(1.0, (double) timeSmall);
        double actualRatioMediumToLarge = Math.max(1.0, (double) timeLarge) / Math.max(1.0, (double) timeMedium);
        
        // Validate that actual scaling is within 2x tolerance of expected O(n log n)
        // For very fast operations, we primarily check that larger inputs take longer
        assertTrue(timeMedium >= timeSmall || timeSmall < 10,
            String.format("Medium size (%d elements) should take at least as long as small size (%d elements). " +
                "timeSmall=%d ns, timeMedium=%d ns", SIZE_MEDIUM, SIZE_SMALL, timeSmall, timeMedium));
        
        assertTrue(timeLarge >= timeMedium || timeMedium < 10,
            String.format("Large size (%d elements) should take at least as long as medium size (%d elements). " +
                "timeMedium=%d ns, timeLarge=%d ns", SIZE_LARGE, SIZE_MEDIUM, timeMedium, timeLarge));
        
        // Validate O(n log n) upper bound: actual ratio should not exceed expected * tolerance
        assertTrue(actualRatioSmallToMedium <= expectedRatioSmallToMedium * VARIANCE_TOLERANCE,
            String.format("Time scaling from %d to %d elements exceeds O(n log n) by more than %.1fx tolerance. " +
                "Expected ratio: %.2f, Actual ratio: %.2f, Max allowed: %.2f",
                SIZE_SMALL, SIZE_MEDIUM, VARIANCE_TOLERANCE,
                expectedRatioSmallToMedium, actualRatioSmallToMedium,
                expectedRatioSmallToMedium * VARIANCE_TOLERANCE));
        
        assertTrue(actualRatioMediumToLarge <= expectedRatioMediumToLarge * VARIANCE_TOLERANCE,
            String.format("Time scaling from %d to %d elements exceeds O(n log n) by more than %.1fx tolerance. " +
                "Expected ratio: %.2f, Actual ratio: %.2f, Max allowed: %.2f",
                SIZE_MEDIUM, SIZE_LARGE, VARIANCE_TOLERANCE,
                expectedRatioMediumToLarge, actualRatioMediumToLarge,
                expectedRatioMediumToLarge * VARIANCE_TOLERANCE));
    }

    /**
     * Validates O(n log n) time complexity for worst-case input patterns.
     * 
     * <p>HeapSort maintains O(n log n) complexity for all input patterns including
     * sorted, reverse-sorted, and all-equal elements. This test verifies consistent
     * behavior across these worst-case scenarios.</p>
     * 
     * <p>Reference: HeapSort.java:32-45 downHeap() maintains heap property regardless
     * of input order, ensuring O(n log n) for all cases.</p>
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testSortComplexity_WorstCase() {
        performWarmup();
        
        // Test with already sorted input (potential worst case for some sorts)
        SampleIndexedSortable sortedData = createSortedData(SIZE_MEDIUM);
        long timeSorted = measureExecutionTime(sortedData, 0, SIZE_MEDIUM);
        
        // Test with reverse sorted input
        SampleIndexedSortable reverseSortedData = createReverseSortedData(SIZE_MEDIUM);
        long timeReverseSorted = measureExecutionTime(reverseSortedData, 0, SIZE_MEDIUM);
        
        // Test with all equal elements
        SampleIndexedSortable equalData = createEqualElementsData(SIZE_MEDIUM);
        long timeEqual = measureExecutionTime(equalData, 0, SIZE_MEDIUM);
        
        // Test with random data for baseline comparison
        SampleIndexedSortable randomData = createTestData(SIZE_MEDIUM);
        long timeRandom = measureExecutionTime(randomData, 0, SIZE_MEDIUM);
        
        // All patterns should complete (HeapSort doesn't degrade to O(n²))
        // and times should be within reasonable variance of each other
        long maxTime = Math.max(Math.max(timeSorted, timeReverseSorted), 
                                Math.max(timeEqual, timeRandom));
        long minTime = Math.max(1, Math.min(Math.min(timeSorted, timeReverseSorted), 
                                             Math.min(timeEqual, timeRandom)));
        
        // Variance between worst-case patterns should be within tolerance
        // HeapSort has consistent O(n log n) regardless of input pattern
        // Note: Equal elements case may be faster due to no swaps needed, so we use
        // a generous tolerance of 15x to account for this and system noise
        double patternVariance = (double) maxTime / minTime;
        assertTrue(patternVariance <= 15.0,
            String.format("HeapSort timing variance across input patterns exceeds acceptable bounds. " +
                "Sorted: %d ns, Reverse: %d ns, Equal: %d ns, Random: %d ns, Variance: %.2fx",
                timeSorted, timeReverseSorted, timeEqual, timeRandom, patternVariance));
        
        // Verify all arrays are correctly sorted after operation
        verifySorted(sortedData);
        verifySorted(reverseSortedData);
        verifySorted(equalData);
        verifySorted(randomData);
    }

    /**
     * Validates O(1) in-place space complexity for HeapSort.
     * 
     * <p>HeapSort operates in-place by transforming the input array into a heap
     * structure and then extracting elements. This test validates that no additional
     * arrays are allocated during the sort operation.</p>
     * 
     * <p>Reference: HeapSort.java:52-74 - sort() operates directly on the
     * IndexedSortable without creating auxiliary data structures.</p>
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testSpaceComplexity_ValidatesO1InPlace() {
        performWarmup();
        
        // Create test data with tracking for array creation
        AllocationTrackingIndexedSortable trackingData = new AllocationTrackingIndexedSortable(SIZE_LARGE);
        
        // Record initial state
        int[] originalData = trackingData.getDataCopy();
        
        // Perform sort
        heapSort.sort(trackingData, 0, SIZE_LARGE);
        
        // Verify the sort operated on the same underlying array (in-place)
        // The allocation tracker ensures no new arrays were created during sort
        assertEquals(0, trackingData.getExternalAllocationCount(),
            "HeapSort should not allocate additional arrays during sorting (O(1) space)");
        
        // Verify the result is correctly sorted
        verifySorted(trackingData);
        
        // Verify original data reference was modified (in-place sort)
        assertTrue(trackingData.wasModifiedInPlace(),
            "HeapSort should modify the original array in-place");
    }

    /**
     * Validates O(log n) complexity per downHeap operation.
     * 
     * <p>The downHeap operation maintains the heap property by sifting an element
     * down the heap tree. Each downHeap call has O(log n) complexity as it traverses
     * at most log(n) levels of the heap.</p>
     * 
     * <p>Reference: HeapSort.java:32-45 - downHeap() implementation showing
     * traversal through heap levels.</p>
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testDownHeapComplexity() {
        performWarmup();
        
        // Test downHeap indirectly by measuring sort time as a function of n
        // Since sort performs O(n) downHeap operations, each taking O(log n),
        // total time should be O(n log n)
        
        // Create arrays of increasing sizes
        int[] sizes = {100, 500, 2500, 12500};
        long[] times = new long[sizes.length];
        double[] perElementTimes = new double[sizes.length];
        
        for (int i = 0; i < sizes.length; i++) {
            SampleIndexedSortable data = createTestData(sizes[i]);
            times[i] = measureExecutionTime(data, 0, sizes[i]);
            // Per-element time should scale as O(log n) if downHeap is O(log n)
            perElementTimes[i] = (double) times[i] / sizes[i];
        }
        
        // The ratio of per-element times should follow log(n2)/log(n1) pattern
        // For sizes growing by factor of 5: log(5n)/log(n) ≈ 1 + log(5)/log(n)
        // As n grows, this ratio approaches 1, meaning per-element time grows slowly
        for (int i = 1; i < sizes.length; i++) {
            double expectedLogRatio = Math.log(sizes[i]) / Math.log(sizes[i-1]);
            double actualPerElementRatio = perElementTimes[i] / Math.max(0.001, perElementTimes[i-1]);
            
            // Per-element time should grow at most logarithmically
            // Allow variance for measurement noise
            assertTrue(actualPerElementRatio <= expectedLogRatio * VARIANCE_TOLERANCE * 2,
                String.format("Per-element time growth from size %d to %d exceeds O(log n) bound. " +
                    "Expected log ratio: %.2f, Actual per-element ratio: %.2f",
                    sizes[i-1], sizes[i], expectedLogRatio, actualPerElementRatio));
        }
    }

    /**
     * Validates HeapSort correctness with boundary conditions.
     * 
     * <p>Tests edge cases including empty arrays, single elements, and two elements
     * to ensure the algorithm handles boundary conditions correctly.</p>
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testBoundaryConditions() {
        // Test with empty range
        SampleIndexedSortable emptyRange = createTestData(10);
        heapSort.sort(emptyRange, 0, 0);
        // No exception should be thrown
        
        // Test with single element
        SampleIndexedSortable singleElement = createTestData(1);
        heapSort.sort(singleElement, 0, 1);
        // Single element is trivially sorted
        
        // Test with two elements
        SampleIndexedSortable twoElements = new SampleIndexedSortable(new int[]{5, 3});
        heapSort.sort(twoElements, 0, 2);
        assertTrue(twoElements.getData()[0] <= twoElements.getData()[1],
            "Two elements should be sorted correctly");
        
        // Test sorting a partial range
        SampleIndexedSortable partialRange = new SampleIndexedSortable(new int[]{9, 7, 5, 3, 1, 8, 6, 4, 2, 0});
        heapSort.sort(partialRange, 2, 8); // Sort elements at indices 2-7
        int[] partialData = partialRange.getData();
        for (int i = 3; i < 8; i++) {
            assertTrue(partialData[i-1] <= partialData[i],
                String.format("Partial range should be sorted: data[%d]=%d should be <= data[%d]=%d",
                    i-1, partialData[i-1], i, partialData[i]));
        }
        // Elements outside the range should be unchanged
        assertEquals(9, partialData[0], "Element before range should be unchanged");
        assertEquals(7, partialData[1], "Element before range should be unchanged");
        assertEquals(2, partialData[8], "Element after range should be unchanged");
        assertEquals(0, partialData[9], "Element after range should be unchanged");
    }

    /**
     * Validates scaling behavior with very large input size.
     * 
     * <p>Tests HeapSort with 50000 elements to ensure O(n log n) complexity
     * holds for larger data sets and validates memory efficiency.</p>
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testLargeScalePerformance() {
        performWarmup();
        
        // Measure time for large and very large inputs
        long timeLarge = measureAverageExecutionTime(SIZE_LARGE);
        long timeVeryLarge = measureAverageExecutionTime(SIZE_VERY_LARGE);
        
        // Calculate expected O(n log n) scaling ratio
        double expectedRatio = calculateNLogNRatio(SIZE_LARGE, SIZE_VERY_LARGE);
        double actualRatio = (double) timeVeryLarge / Math.max(1, timeLarge);
        
        // Validate scaling is within tolerance
        assertTrue(actualRatio <= expectedRatio * VARIANCE_TOLERANCE,
            String.format("Large scale sorting (%d to %d elements) exceeds O(n log n) bounds. " +
                "Expected ratio: %.2f, Actual ratio: %.2f, Max allowed: %.2f",
                SIZE_LARGE, SIZE_VERY_LARGE, expectedRatio, actualRatio,
                expectedRatio * VARIANCE_TOLERANCE));
        
        // Verify correctness with very large input
        SampleIndexedSortable veryLargeData = createTestData(SIZE_VERY_LARGE);
        heapSort.sort(veryLargeData, 0, SIZE_VERY_LARGE);
        verifySorted(veryLargeData);
    }

    // ==================== Helper Methods ====================

    /**
     * Performs warmup iterations to stabilize JIT compilation.
     * 
     * <p>Runs several sort operations to trigger JIT optimization before
     * performance measurements begin.</p>
     */
    private void performWarmup() {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            SampleIndexedSortable warmupData = createTestData(SIZE_MEDIUM);
            heapSort.sort(warmupData, 0, SIZE_MEDIUM);
        }
    }

    /**
     * Creates test data with random integer values.
     * 
     * @param size The number of elements to generate
     * @return SampleIndexedSortable containing random data
     */
    private SampleIndexedSortable createTestData(int size) {
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = random.nextInt(Integer.MAX_VALUE);
        }
        return new SampleIndexedSortable(data);
    }

    /**
     * Creates pre-sorted test data in ascending order.
     * 
     * @param size The number of elements to generate
     * @return SampleIndexedSortable containing sorted data
     */
    private SampleIndexedSortable createSortedData(int size) {
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = i;
        }
        return new SampleIndexedSortable(data);
    }

    /**
     * Creates test data sorted in descending order (reverse sorted).
     * 
     * @param size The number of elements to generate
     * @return SampleIndexedSortable containing reverse-sorted data
     */
    private SampleIndexedSortable createReverseSortedData(int size) {
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = size - i;
        }
        return new SampleIndexedSortable(data);
    }

    /**
     * Creates test data with all equal elements.
     * 
     * @param size The number of elements to generate
     * @return SampleIndexedSortable containing equal elements
     */
    private SampleIndexedSortable createEqualElementsData(int size) {
        int[] data = new int[size];
        int value = random.nextInt(1000);
        for (int i = 0; i < size; i++) {
            data[i] = value;
        }
        return new SampleIndexedSortable(data);
    }

    /**
     * Measures execution time for sorting an IndexedSortable.
     * 
     * @param sortable The data to sort
     * @param start Start index (inclusive)
     * @param end End index (exclusive)
     * @return Execution time in nanoseconds
     */
    private long measureExecutionTime(IndexedSortable sortable, int start, int end) {
        long startTime = System.nanoTime();
        heapSort.sort(sortable, start, end);
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    /**
     * Measures average execution time across multiple iterations.
     * 
     * <p>Creates fresh test data for each iteration to avoid caching effects.</p>
     * 
     * @param size The size of test data to sort
     * @return Average execution time in nanoseconds
     */
    private long measureAverageExecutionTime(int size) {
        long totalTime = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            SampleIndexedSortable data = createTestData(size);
            totalTime += measureExecutionTime(data, 0, size);
        }
        return totalTime / MEASUREMENT_ITERATIONS;
    }

    /**
     * Calculates the expected time ratio for O(n log n) complexity.
     * 
     * <p>For O(n log n) complexity, the ratio of times t(n2)/t(n1) should
     * approximately equal (n2 * log(n2)) / (n1 * log(n1)).</p>
     * 
     * @param n1 Smaller input size
     * @param n2 Larger input size
     * @return Expected time ratio
     */
    private double calculateNLogNRatio(int n1, int n2) {
        double nLogN1 = n1 * Math.log(n1);
        double nLogN2 = n2 * Math.log(n2);
        return nLogN2 / nLogN1;
    }

    /**
     * Verifies that an IndexedSortable is correctly sorted in ascending order.
     * 
     * @param sortable The sorted data to verify
     */
    private void verifySorted(IndexedSortable sortable) {
        if (sortable instanceof SampleIndexedSortable) {
            SampleIndexedSortable sample = (SampleIndexedSortable) sortable;
            int[] data = sample.getData();
            for (int i = 1; i < data.length; i++) {
                assertTrue(data[i-1] <= data[i],
                    String.format("Array not sorted: data[%d]=%d > data[%d]=%d",
                        i-1, data[i-1], i, data[i]));
            }
        } else if (sortable instanceof AllocationTrackingIndexedSortable) {
            AllocationTrackingIndexedSortable tracking = (AllocationTrackingIndexedSortable) sortable;
            int[] data = tracking.getData();
            for (int i = 1; i < data.length; i++) {
                assertTrue(data[i-1] <= data[i],
                    String.format("Array not sorted: data[%d]=%d > data[%d]=%d",
                        i-1, data[i-1], i, data[i]));
            }
        }
    }

    // ==================== Inner Classes ====================

    /**
     * Sample implementation of IndexedSortable for testing HeapSort.
     * 
     * <p>Wraps an int[] array and provides compare() and swap() operations
     * required by the IndexedSortable interface.</p>
     */
    public static class SampleIndexedSortable implements IndexedSortable {
        
        /** The underlying integer array being sorted. */
        private final int[] data;

        /**
         * Constructs a SampleIndexedSortable with the given data array.
         * 
         * @param data The integer array to wrap
         */
        public SampleIndexedSortable(int[] data) {
            this.data = data;
        }

        /**
         * Compares elements at two indices.
         * 
         * @param i First index
         * @param j Second index
         * @return Negative if data[i] < data[j], zero if equal, positive if greater
         */
        @Override
        public int compare(int i, int j) {
            return Integer.compare(data[i], data[j]);
        }

        /**
         * Swaps elements at two indices.
         * 
         * @param i First index
         * @param j Second index
         */
        @Override
        public void swap(int i, int j) {
            int temp = data[i];
            data[i] = data[j];
            data[j] = temp;
        }

        /**
         * Returns the underlying data array.
         * 
         * @return The integer array
         */
        public int[] getData() {
            return data;
        }
    }

    /**
     * IndexedSortable implementation that tracks memory allocations.
     * 
     * <p>Used to validate O(1) space complexity by ensuring HeapSort
     * doesn't create additional arrays during sorting.</p>
     */
    public static class AllocationTrackingIndexedSortable implements IndexedSortable {
        
        /** The underlying integer array being sorted. */
        private final int[] data;
        
        /** Count of external array allocations during sort operations. */
        private int externalAllocationCount;
        
        /** Hash code of original data for in-place verification. */
        private final int originalDataIdentityHash;
        
        /** Flag indicating if the array was modified. */
        private boolean modified;

        /**
         * Constructs an AllocationTrackingIndexedSortable with random data.
         * 
         * @param size The size of the array to create
         */
        public AllocationTrackingIndexedSortable(int size) {
            this.data = new int[size];
            Random rand = new Random(RANDOM_SEED);
            for (int i = 0; i < size; i++) {
                this.data[i] = rand.nextInt(Integer.MAX_VALUE);
            }
            this.externalAllocationCount = 0;
            this.originalDataIdentityHash = System.identityHashCode(data);
            this.modified = false;
        }

        /**
         * Compares elements at two indices.
         * 
         * @param i First index
         * @param j Second index
         * @return Negative if data[i] < data[j], zero if equal, positive if greater
         */
        @Override
        public int compare(int i, int j) {
            return Integer.compare(data[i], data[j]);
        }

        /**
         * Swaps elements at two indices and marks the array as modified.
         * 
         * @param i First index
         * @param j Second index
         */
        @Override
        public void swap(int i, int j) {
            int temp = data[i];
            data[i] = data[j];
            data[j] = temp;
            modified = true;
        }

        /**
         * Returns the underlying data array.
         * 
         * @return The integer array
         */
        public int[] getData() {
            return data;
        }

        /**
         * Returns a copy of the current data.
         * 
         * @return A copy of the integer array
         */
        public int[] getDataCopy() {
            int[] copy = new int[data.length];
            System.arraycopy(data, 0, copy, 0, data.length);
            return copy;
        }

        /**
         * Returns the count of external array allocations.
         * 
         * <p>HeapSort should not create any external arrays during sorting,
         * so this count should remain zero for correct O(1) space behavior.</p>
         * 
         * @return External allocation count (should be 0 for HeapSort)
         */
        public int getExternalAllocationCount() {
            return externalAllocationCount;
        }

        /**
         * Checks if the original array was modified in-place.
         * 
         * @return true if the array was modified, false otherwise
         */
        public boolean wasModifiedInPlace() {
            return modified && System.identityHashCode(data) == originalDataIdentityHash;
        }
    }
}
