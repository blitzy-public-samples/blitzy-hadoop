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
 * Interface for sort algorithms accepting {@link IndexedSortable} items.
 *
 * A sort algorithm implementing this interface may only
 * {@link IndexedSortable#compare} and {@link IndexedSortable#swap} items
 * for a range of indices to effect a sort across that range.
 *
 * @performance Implementations typically provide O(n log n) time complexity for general sorting.
 *              Space complexity varies by implementation: O(1) for HeapSort (in-place),
 *              O(log n) for QuickSort (stack depth), O(n) for MergeSort (auxiliary buffer).
 *              Stability guarantees vary: MergeSort is stable, QuickSort and HeapSort are not stable.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public interface IndexedSorter {

  /**
   * Sort the items accessed through the given IndexedSortable over the given
   * range of logical indices. From the perspective of the sort algorithm,
   * each index between l (inclusive) and r (exclusive) is an addressable
   * entry.
   * @see IndexedSortable#compare
   * @see IndexedSortable#swap
   *
   * @complexity Time: O(n log n) expected where n = r - l, though worst-case varies by
   *             implementation (e.g., O(n²) for basic QuickSort with adversarial input).
   *             Space: Varies by implementation - O(1) for HeapSort, O(log n) for QuickSort
   *             (recursive stack depth), O(n) for MergeSort (auxiliary merge buffer).
   *             Implementations should document their specific complexity characteristics.
   *
   * @param s The IndexedSortable containing the items to sort.
   * @param l The left (inclusive) index of the sort range.
   * @param r The right (exclusive) index of the sort range.
   */
  void sort(IndexedSortable s, int l, int r);

  /**
   * Same as {@link #sort(IndexedSortable,int,int)}, but indicate progress
   * periodically.
   * @see #sort(IndexedSortable,int,int)
   *
   * @complexity Time: O(n log n) expected where n = r - l, with same variance as
   *             {@link #sort(IndexedSortable,int,int)}. The {@link Progressable#progress()}
   *             calls add O(1) constant overhead per invocation; implementations typically
   *             call progress() O(n) times total, adding O(n) cumulative overhead which
   *             does not change the dominant O(n log n) asymptotic complexity.
   *             Space: Same as {@link #sort(IndexedSortable,int,int)} - varies by implementation.
   *
   * @param s The IndexedSortable containing the items to sort.
   * @param l The left (inclusive) index of the sort range.
   * @param r The right (exclusive) index of the sort range.
   * @param rep The Progressable to report progress during sorting.
   */
  void sort(IndexedSortable s, int l, int r, Progressable rep);

}
