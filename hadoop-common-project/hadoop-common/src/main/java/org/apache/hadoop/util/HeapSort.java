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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An implementation of the core algorithm of HeapSort.
 *
 * <p>HeapSort is a comparison-based sorting algorithm that uses a binary heap
 * data structure. It divides the input into a sorted and unsorted region,
 * and iteratively shrinks the unsorted region by extracting the maximum
 * element and moving it to the sorted region.</p>
 *
 * @complexity Time: O(n log n) all cases (worst/average/best); Space: O(1) in-place sorting
 * @implNote In-place sorting with guaranteed O(n log n) worst-case but not stable;
 *           used as fallback by QuickSort for worst-case protection when recursion
 *           depth exceeds threshold. Unlike QuickSort, HeapSort does not degrade to
 *           O(n²) on adversarial inputs but has higher constant factors due to
 *           poor cache locality during heap operations.
 * @performance Scales linearly with n log n for all input distributions;
 *              no worst-case degradation unlike QuickSort
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HeapSort implements IndexedSorter {

  public HeapSort() { }

  /**
   * Performs the sift-down operation (also known as heapify-down) to restore
   * the heap property after a modification at position i.
   *
   * <p>This iterative implementation descends through the heap levels,
   * comparing the current node with its children and swapping with the
   * larger child if the heap property is violated.</p>
   *
   * @param s the IndexedSortable containing elements to heapify
   * @param b the base offset for index calculations (typically p-1)
   * @param i the starting position for sift-down operation (1-indexed in heap)
   * @param N the size of the heap (exclusive upper bound)
   *
   * @complexity Time: O(log n) per call where n is the heap size; iteratively descends
   *             through at most log₂(n) levels of the binary heap.
   *             Space: O(1) - uses only constant auxiliary variables (idx, i)
   *             for iterative descent; no recursive stack allocation.
   * @implNote Uses iterative approach rather than recursion for better performance
   *           and to avoid stack overflow on large datasets. The bit-shift operation
   *           (i << 1) efficiently computes the left child index in a 1-indexed heap.
   */
  private static void downHeap(final IndexedSortable s, final int b,
      int i, final int N) {
    // @PerformanceCritical: Inner heap maintenance loop - executed O(n log n) times during sort
    for (int idx = i << 1; idx < N; idx = i << 1) {
      if (idx + 1 < N && s.compare(b + idx, b + idx + 1) < 0) {
        if (s.compare(b + i, b + idx + 1) < 0) {
          s.swap(b + i, b + idx + 1);
        } else return;
        i = idx + 1;
      } else if (s.compare(b + i, b + idx) < 0) {
        s.swap(b + i, b + idx);
        i = idx;
      } else return;
    }
  }

  /**
   * Sort the given range of items using heap sort.
   * {@inheritDoc}
   *
   * @complexity Time: O(n log n) for heap building O(n) + O(n log n) extraction;
   *             Space: O(1) in-place sorting with no auxiliary array allocation.
   * @implNote Delegates to {@link #sort(IndexedSortable, int, int, Progressable)}
   *           with null progress reporter for non-interactive sorting.
   */
  @Override
  public void sort(IndexedSortable s, int p, int r) {
    sort(s, p, r, null);
  }

  /**
   * Sort the given range of items using heap sort with optional progress reporting.
   * {@inheritDoc}
   *
   * <p>The algorithm operates in two phases:</p>
   * <ol>
   *   <li><b>Build-Heap Phase</b>: Converts the array into a max-heap by calling
   *       downHeap on internal nodes from bottom to top. Despite appearing to be
   *       O(n log n), this phase is actually O(n) because nodes near the leaves
   *       (which are the majority) require minimal work.</li>
   *   <li><b>Extraction Phase</b>: Repeatedly extracts the maximum element (root)
   *       by swapping it with the last unsorted element and restoring heap property.
   *       Each of the n-1 extractions requires O(log n) downHeap operations.</li>
   * </ol>
   *
   * @param s the IndexedSortable containing elements to sort
   * @param p the starting index (inclusive) of the range to sort
   * @param r the ending index (exclusive) of the range to sort
   * @param rep optional Progressable for reporting progress; may be null
   *
   * @complexity Time: O(n log n) total where n = r - p
   *             - Build-Heap Phase: O(n) amortized for constructing the initial heap
   *             - Extraction Phase: O(n log n) for n extractions × O(log n) downHeap each
   *             Space: O(1) in-place; no auxiliary arrays allocated.
   *             Only uses constant stack space for loop variables.
   * @implNote Uses a reverse heap construction strategy starting from the highest
   *           power of 2 and working down, which enables efficient bottom-up heap building.
   *           The bit manipulation (Integer.highestOneBit, >>>=) provides efficient
   *           level-by-level traversal of the implicit binary tree structure.
   */
  @Override
  public void sort(final IndexedSortable s, final int p, final int r,
      final Progressable rep) {
    final int N = r - p;
    // Build-Heap Phase: O(n) total - build heap w/ reverse comparator, then write in-place from end
    final int t = Integer.highestOneBit(N);
    for (int i = t; i > 1; i >>>= 1) {
      for (int j = i >>> 1; j < i; ++j) {
        downHeap(s, p-1, j, N + 1);
      }
      if (null != rep) {
        rep.progress();
      }
    }
    // Extraction Phase: O(n log n) total - n extractions each requiring O(log n) downHeap
    for (int i = r - 1; i > p; --i) {
      s.swap(p, i);
      downHeap(s, p - 1, 1, i - p + 1);
    }
  }
}
