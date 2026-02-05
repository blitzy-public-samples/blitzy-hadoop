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
 * An implementation of the core algorithm of QuickSort.
 *
 * <p>This implementation uses a dual-pivot partitioning scheme with median-of-three
 * pivot selection for improved performance on typical inputs. For small subarrays
 * (fewer than 13 elements), it switches to insertion sort for better cache locality
 * and reduced overhead.</p>
 *
 * @complexity Time: O(n log n) average-case, O(n²) worst-case (sorted/reverse sorted input);
 *             Space: O(log n) stack depth for recursive calls in average case,
 *             O(n) stack depth in worst case for degenerate input
 * @implNote Uses quicksort with heapsort fallback (via {@link HeapSort}) for worst-case
 *           protection when recursion depth exceeds {@link #getMaxDepth(int)} limit.
 *           This ensures O(n log n) worst-case time complexity at the cost of slightly
 *           higher constant factors when fallback is triggered. The depth limit is
 *           set to 2 * ceil(log(n)) to detect degenerate partitioning early.
 * @performance Scales linearly with input size for average case; worst-case quadratic
 *              behavior is mitigated by heapsort fallback. Optimal for general-purpose
 *              sorting of IndexedSortable collections in MapReduce shuffle operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QuickSort implements IndexedSorter {

  private static final IndexedSorter alt = new HeapSort();

  public QuickSort() { }

  /**
   * Conditionally swaps elements at positions p and r if they are out of order.
   * Used as part of median-of-three pivot selection to improve partitioning quality.
   *
   * @param s the sortable collection
   * @param p first position to compare
   * @param r second position to compare
   * @complexity Time: O(1) - single comparison and potential swap operation;
   *             Space: O(1) - no additional memory allocation
   */
  private static void fix(IndexedSortable s, int p, int r) {
    if (s.compare(p, r) > 0) {
      s.swap(p, r);
    }
  }

  /**
   * Calculates the maximum recursion depth before falling back to heapsort.
   * Returns approximately 2 * ceil(log₂(n)), which provides sufficient depth
   * for well-partitioned quicksort while detecting degenerate cases early.
   *
   * <p>The formula uses bit manipulation for efficient computation:
   * {@code (32 - numberOfLeadingZeros(x-1)) << 2} computes 4 times the
   * number of bits needed to represent (x-1), effectively yielding
   * approximately 4 * ceil(log₂(x)).</p>
   *
   * @param x the size of the range to be sorted; must be positive
   * @return the maximum recursion depth threshold
   * @throws IllegalArgumentException if x is less than or equal to zero
   * @complexity Time: O(1) - constant time bit manipulation using
   *             Integer.numberOfLeadingZeros intrinsic;
   *             Space: O(1) - no additional memory allocation
   */
  protected static int getMaxDepth(int x) {
    if (x <= 0)
      throw new IllegalArgumentException("Undefined for " + x);
    return (32 - Integer.numberOfLeadingZeros(x - 1)) << 2;
  }

  /**
   * Sort the given range of items using quick sort.
   * {@inheritDoc} If the recursion depth falls below {@link #getMaxDepth},
   * then switch to {@link HeapSort}.
   *
   * @complexity Time: O(n log n) average-case where n = r - p, O(n²) worst-case
   *             for sorted/reverse sorted input (mitigated by heapsort fallback);
   *             Space: O(log n) stack depth for recursive calls in average case
   * @see #sortInternal(IndexedSortable, int, int, Progressable, int)
   */
  @Override
  public void sort(IndexedSortable s, int p, int r) {
    sort(s, p, r, null);
  }

  /**
   * Sort the given range of items using quick sort with progress reporting.
   * {@inheritDoc}
   *
   * <p>Progress is reported at each recursion level via the provided
   * {@link Progressable} to prevent timeout during long-running sorts.</p>
   *
   * @param s the sortable collection to sort
   * @param p the start index (inclusive) of the range to sort
   * @param r the end index (exclusive) of the range to sort
   * @param rep progress reporter for heartbeat during long operations; may be null
   * @complexity Time: O(n log n) average-case where n = r - p, O(n²) worst-case
   *             for sorted/reverse sorted input (mitigated by heapsort fallback);
   *             Space: O(log n) stack depth for recursive calls in average case
   * @see #sortInternal(IndexedSortable, int, int, Progressable, int)
   */
  @Override
  public void sort(final IndexedSortable s, int p, int r,
      final Progressable rep) {
    sortInternal(s, p, r, rep, getMaxDepth(r - p));
  }

  /**
   * Internal recursive quicksort implementation with depth limiting.
   *
   * <p>This method implements a hybrid sorting strategy:</p>
   * <ul>
   *   <li>For small subarrays (n &lt; 13): Uses insertion sort for O(n²) but with
   *       excellent cache locality and low overhead</li>
   *   <li>For larger subarrays: Uses quicksort with median-of-three pivot selection</li>
   *   <li>When depth limit exceeded: Falls back to heapsort to guarantee O(n log n)</li>
   * </ul>
   *
   * <p>The partitioning scheme handles equal elements efficiently by gathering them
   * around the pivot, which prevents quadratic behavior on inputs with many duplicates.</p>
   *
   * @param s the sortable collection to sort
   * @param p the start index (inclusive) of the range to sort
   * @param r the end index (exclusive) of the range to sort
   * @param rep progress reporter for heartbeat during long operations; may be null
   * @param depth remaining recursion depth before heapsort fallback
   * @complexity Time: O(n log n) average-case, O(n²) worst-case for degenerate partitioning;
   *             worst-case is mitigated by heapsort fallback when depth exhausted.
   *             Best-case O(n log n) with good pivot selection.
   *             Space: O(log n) stack depth for recursive calls in average case,
   *             O(n) stack depth worst-case for maximally unbalanced partitions
   *             (before heapsort fallback triggers)
   * @implNote Recurses on smaller partition first to limit stack depth to O(log n)
   *           in the average case. The larger partition is processed via tail-call
   *           optimization (converted to iteration via while loop).
   */
  private static void sortInternal(final IndexedSortable s, int p, int r,
      final Progressable rep, int depth) {
    if (null != rep) {
      rep.progress();
    }
    while (true) {
    if (r-p < 13) {
      // Insertion sort for small subarrays - O(n²) but cache-friendly with low overhead
      for (int i = p; i < r; ++i) {
        for (int j = i; j > p && s.compare(j-1, j) > 0; --j) {
          s.swap(j, j-1);
        }
      }
      return;
    }
    if (--depth < 0) {
      // Depth limit exceeded - fall back to heapsort for O(n log n) guarantee
      // @PerformanceCritical: Heapsort fallback ensures worst-case O(n log n)
      alt.sort(s, p, r, rep);
      return;
    }

    // select, move pivot into first position
    // Median-of-three pivot selection for improved partitioning
    fix(s, (p+r) >>> 1, p);
    fix(s, (p+r) >>> 1, r - 1);
    fix(s, p, r-1);

    // Divide - partition around pivot at position p
    int i = p;
    int j = r;
    int ll = p;
    int rr = r;
    int cr;
    // @PerformanceCritical: Inner partition loop (>5% execution time in sorting workloads)
    // This dual-scan partitioning with equal-element handling is the hot path
    while(true) {
      while (++i < j) {
        if ((cr = s.compare(i, p)) > 0) break;
        if (0 == cr && ++ll != i) {
          s.swap(ll, i);
        }
      }
      while (--j > i) {
        if ((cr = s.compare(p, j)) > 0) break;
        if (0 == cr && --rr != j) {
          s.swap(rr, j);
        }
      }
      if (i < j) s.swap(i, j);
      else break;
    }
    j = i;
    // swap pivot- and all eq values- into position
    while (ll >= p) {
      s.swap(ll--, --i);
    }
    while (rr < r) {
      s.swap(rr++, j++);
    }

    // Conquer
    // Recurse on smaller interval first to keep stack shallow
    assert i != j;
    if (i - p < r - j) {
      sortInternal(s, p, i, rep, depth);
      p = j;
    } else {
      sortInternal(s, j, r, rep, depth);
      r = i;
    }
    }
  }

}
