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

import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IntWritable;

/**
 * An implementation of the core algorithm of MergeSort.
 *
 * <p>This class provides a stable merge sort implementation optimized for
 * MapReduce workloads, using IntWritable comparators for compatibility with
 * Hadoop's serialization framework.</p>
 *
 * @complexity Time: O(n log n) all cases (worst/average/best) where n is the number of elements;
 *             Space: O(n) auxiliary array for merge buffer (src and dest arrays required)
 * @implNote Stable sort; preferred when stability is required despite higher memory usage
 *           compared to in-place algorithms like HeapSort. Uses IntWritable comparator for
 *           MapReduce compatibility, enabling efficient comparison of serialized integer keys.
 *           Includes insertion sort optimization for small arrays (&lt;7 elements) and
 *           fast-path optimization for nearly-sorted input sequences.
 * @performance Linear scaling with input size; memory-bound for very large arrays due to
 *              auxiliary space requirement. Recommended for datasets where stability is
 *              required or when input is expected to be partially sorted.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MergeSort {
  //Reusable IntWritables
  IntWritable I = new IntWritable(0);
  IntWritable J = new IntWritable(0);
  
  //the comparator that the algo should use
  private Comparator<IntWritable> comparator;
  
  public MergeSort(Comparator<IntWritable> comparator) {
    this.comparator = comparator;
  }
  
  /**
   * Sorts the specified range of the destination array using merge sort algorithm.
   *
   * <p>This method recursively divides the array into halves, sorts each half,
   * and merges them back together. The algorithm uses the src array as temporary
   * storage during the merge phase.</p>
   *
   * <p><b>Algorithm Details:</b></p>
   * <ul>
   *   <li>For small arrays (&lt;7 elements): Uses insertion sort with O(n²) worst-case
   *       but excellent cache locality and low overhead for small n</li>
   *   <li>For larger arrays: Recursively splits into halves (O(log n) levels),
   *       with O(n) merge work at each level</li>
   *   <li>Nearly-sorted optimization: If halves are already in order, performs
   *       O(n) array copy instead of O(n) merge comparison</li>
   * </ul>
   *
   * @param src source array used as temporary storage during merge; must be a copy of dest
   * @param dest destination array to be sorted in place
   * @param low the index of the first element (inclusive) to be sorted
   * @param high the index of the last element (exclusive) to be sorted
   *
   * @complexity Time: O(n log n) worst-case, average-case, best-case where n = high - low;
   *             Recurrence: T(n) = 2T(n/2) + O(n) merge work per level;
   *             Space: O(n) auxiliary for src/dest arrays plus O(log n) stack depth for recursion.
   *             Insertion sort fallback for n&lt;7: O(n²) time but negligible for small n.
   *             Nearly-sorted optimization reduces merge phase to O(n) copy when
   *             src[mid-1] &lt;= src[mid], providing best-case speedup for partially ordered data.
   * @implNote The insertion sort threshold of 7 elements is chosen based on empirical
   *           performance studies showing that insertion sort outperforms divide-and-conquer
   *           for very small arrays due to lower constant factors and better cache behavior.
   *           The nearly-sorted optimization (lines 65-70) provides significant speedup for
   *           data that is already partially ordered, common in MapReduce shuffle phases.
   *
   * Source: MergeSort.java:42-83
   */
  public void mergeSort(int src[], int dest[], int low, int high) {
    int length = high - low;

    // Insertion sort on smallest arrays
    if (length < 7) {
      for (int i=low; i<high; i++) {
        for (int j=i;j > low; j--) {
          I.set(dest[j-1]);
          J.set(dest[j]);
          if (comparator.compare(I, J)>0)
            swap(dest, j, j-1);
        }
      }
      return;
    }

    // Recursively sort halves of dest into src
    int mid = (low + high) >>> 1;
    mergeSort(dest, src, low, mid);
    mergeSort(dest, src, mid, high);

    I.set(src[mid-1]);
    J.set(src[mid]);
    // If list is already sorted, just copy from src to dest.  This is an
    // optimization that results in faster sorts for nearly ordered lists.
    if (comparator.compare(I, J) <= 0) {
      System.arraycopy(src, low, dest, low, length);
      return;
    }

    // Merge sorted halves (now in src) into dest
    for (int i = low, p = low, q = mid; i < high; i++) {
      if (q < high && p < mid) {
        I.set(src[p]);
        J.set(src[q]);
      }
      if (q>=high || p<mid && comparator.compare(I, J) <= 0)
        dest[i] = src[p++];
      else
        dest[i] = src[q++];
    }
  }

  /**
   * Swaps two elements in the specified array.
   *
   * <p>This is a standard in-place swap operation used by the insertion sort
   * fallback for small subarrays.</p>
   *
   * @param x the array in which to swap elements
   * @param a the index of the first element to swap
   * @param b the index of the second element to swap
   *
   * @complexity Time: O(1) constant time - single element exchange with 3 assignments;
   *             Space: O(1) constant space - uses single temporary variable (4 bytes)
   *
   * Source: MergeSort.java:85-89
   */
  private void swap(int x[], int a, int b) {
    int t = x[a];
    x[a] = x[b];
    x[b] = t;
  }
}
