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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

/**
 * Shuffle phase implementation for MapReduce reduce tasks.
 * 
 * <p>This class coordinates the shuffling of map output data to reduce tasks,
 * managing parallel fetchers, event polling, and the final merge operation.</p>
 * 
 * @performance Shuffle phase is typically network-bound in distributed deployments.
 *              Total time = max(network_transfer_time, merge_time). Parallelism
 *              controlled by MRJobConfig.SHUFFLE_PARALLEL_COPIES (default 5).
 *              Memory pressure managed by MergeManagerImpl thresholds configured via
 *              mapreduce.reduce.shuffle.input.buffer.percent (default 70% of heap).
 *              Local mode uses single LocalFetcher thread, bypassing network overhead.
 *
 * @param <K> the key type for map output
 * @param <V> the value type for map output
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
@SuppressWarnings({"unchecked", "rawtypes"})
public class Shuffle<K, V> implements ShuffleConsumerPlugin<K, V>,
    ExceptionReporter {
  private static final int PROGRESS_FREQUENCY = 2000;
  private static final int MAX_EVENTS_TO_FETCH = 10000;
  private static final int MIN_EVENTS_TO_FETCH = 100;
  private static final int MAX_RPC_OUTSTANDING_EVENTS = 3000000;
  
  private ShuffleConsumerPlugin.Context context;

  private TaskAttemptID reduceId;
  private JobConf jobConf;
  private Reporter reporter;
  private ShuffleClientMetrics metrics;
  private TaskUmbilicalProtocol umbilical;
  
  private ShuffleSchedulerImpl<K, V> scheduler;
  private MergeManager<K, V> merger;
  private Throwable throwable = null;
  private String throwingThreadName = null;
  private Progress copyPhase;
  private TaskStatus taskStatus;
  private Task reduceTask; //Used for status updates
  private Map<TaskAttemptID, MapOutputFile> localMapFiles;

  @Override
  public void init(ShuffleConsumerPlugin.Context context) {
    this.context = context;

    this.reduceId = context.getReduceId();
    this.jobConf = context.getJobConf();
    this.umbilical = context.getUmbilical();
    this.reporter = context.getReporter();
    this.metrics = ShuffleClientMetrics.create(context.getReduceId(),
        this.jobConf);
    this.copyPhase = context.getCopyPhase();
    this.taskStatus = context.getStatus();
    this.reduceTask = context.getReduceTask();
    this.localMapFiles = context.getLocalMapFiles();
    
    scheduler = new ShuffleSchedulerImpl<K, V>(jobConf, taskStatus, reduceId,
        this, copyPhase, context.getShuffledMapsCounter(),
        context.getReduceShuffleBytes(), context.getFailedShuffleCounter());
    merger = createMergeManager(context);
  }

  protected MergeManager<K, V> createMergeManager(
      ShuffleConsumerPlugin.Context context) {
    return new MergeManagerImpl<K, V>(reduceId, jobConf, context.getLocalFS(),
        context.getLocalDirAllocator(), reporter, context.getCodec(),
        context.getCombinerClass(), context.getCombineCollector(), 
        context.getSpilledRecordsCounter(),
        context.getReduceCombineInputCounter(),
        context.getMergedMapOutputsCounter(), this, context.getMergePhase(),
        context.getMapOutputFile());
  }

  /**
   * Executes the shuffle phase, coordinating map output fetching and merging.
   * 
   * <p>This method spawns EventFetcher and Fetcher threads to retrieve map outputs
   * from completed map tasks, waits for all outputs to be fetched, and performs
   * the final merge to produce a sorted iterator for the reduce phase.</p>
   * 
   * @complexity Time: O(m * n/m) = O(n) where n=total records across all mappers,
   *             m=number of mappers; each mapper contributes n/m records on average.
   *             EventFetcher polls completion events from TaskUmbilicalProtocol,
   *             Fetcher threads retrieve map outputs in parallel (controlled by
   *             SHUFFLE_PARALLEL_COPIES, default 5). Final merge adds O(n log k)
   *             where k=number of map outputs being merged.
   *             Space: O(buffer_size) configurable via mapreduce.reduce.shuffle.
   *             input.buffer.percent (default 70% of heap); memory is managed by
   *             MergeManagerImpl with configurable spill thresholds.
   * 
   * @performance Scales linearly with input data; network-bound for large datasets.
   *              Local mode (localMapFiles != null) uses single LocalFetcher thread,
   *              bypassing network transfer. Remote mode spawns numFetchers parallel
   *              fetch threads. Progress reported every PROGRESS_FREQUENCY (2000ms).
   *              Final merge complexity determined by MergeManagerImpl based on
   *              memory availability and spill configuration.
   * 
   * @implNote Thread architecture: 1 EventFetcher thread polls TaskUmbilicalProtocol
   *           for map completion events (up to maxEventsToFetch per RPC, scaled by
   *           number of reducers to prevent OOM). numFetchers Fetcher threads
   *           (default 5, configurable via MRJobConfig.SHUFFLE_PARALLEL_COPIES)
   *           retrieve map outputs in parallel. Exceptions from any thread are
   *           captured and surfaced via reportException(). Shutdown sequence:
   *           eventFetcher.shutDown() -&gt; fetchers[].shutDown() -&gt; scheduler.close()
   *           -&gt; merger.close() returns final RawKeyValueIterator for reduce phase.
   * 
   * @return RawKeyValueIterator providing sorted, merged key-value pairs for reduce
   * @throws IOException if shuffle or merge operations fail
   * @throws InterruptedException if the shuffle is interrupted
   */
  @Override
  public RawKeyValueIterator run() throws IOException, InterruptedException {
    // Scale the maximum events we fetch per RPC call to mitigate OOM issues
    // on the ApplicationMaster when a thundering herd of reducers fetch events
    // TODO: This should not be necessary after HADOOP-8942
    int eventsPerReducer = Math.max(MIN_EVENTS_TO_FETCH,
        MAX_RPC_OUTSTANDING_EVENTS / jobConf.getNumReduceTasks());
    int maxEventsToFetch = Math.min(MAX_EVENTS_TO_FETCH, eventsPerReducer);

    // Start the map-completion events fetcher thread
    final EventFetcher<K, V> eventFetcher =
        new EventFetcher<K, V>(reduceId, umbilical, scheduler, this,
            maxEventsToFetch);
    eventFetcher.start();
    
    // Start the map-output fetcher threads
    boolean isLocal = localMapFiles != null;
    final int numFetchers = isLocal ? 1 :
        jobConf.getInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 5);
    Fetcher<K, V>[] fetchers = new Fetcher[numFetchers];
    if (isLocal) {
      fetchers[0] = new LocalFetcher<K, V>(jobConf, reduceId, scheduler,
          merger, reporter, metrics, this, reduceTask.getShuffleSecret(),
          localMapFiles);
      fetchers[0].start();
    } else {
      for (int i=0; i < numFetchers; ++i) {
        fetchers[i] = new Fetcher<K, V>(jobConf, reduceId, scheduler, merger,
                                       reporter, metrics, this, 
                                       reduceTask.getShuffleSecret());
        fetchers[i].start();
      }
    }
    
    // @PerformanceCritical: Main shuffle loop - fetches all map outputs (>10% reduce task time).
    // Polls scheduler every PROGRESS_FREQUENCY ms (2000ms) until all mappers complete.
    // Network I/O dominates in distributed mode; local mode bypasses this via LocalFetcher.
    // Wait for shuffle to complete successfully
    while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
      // Complexity: O(1) per poll; total iterations = O(shuffle_time / PROGRESS_FREQUENCY)
      // Merge operations triggered asynchronously by MergeManagerImpl based on memory thresholds
      reporter.progress();
      
      synchronized (this) {
        if (throwable != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                 throwable);
        }
      }
    }

    // Stop the event-fetcher thread
    eventFetcher.shutDown();
    
    // Stop the map-output fetcher threads
    for (Fetcher<K, V> fetcher : fetchers) {
      fetcher.shutDown();
    }
    
    // stop the scheduler
    scheduler.close();

    copyPhase.complete(); // copy is already complete
    taskStatus.setPhase(TaskStatus.Phase.SORT);
    reduceTask.statusUpdate(umbilical);

    // Finish the on-going merges...
    RawKeyValueIterator kvIter = null;
    try {
      kvIter = merger.close();
    } catch (Throwable e) {
      throw new ShuffleError("Error while doing final merge ", e);
    }

    // Sanity check
    synchronized (this) {
      if (throwable != null) {
        throw new ShuffleError("error in shuffle in " + throwingThreadName,
                               throwable);
      }
    }
    
    return kvIter;
  }

  @Override
  public void close(){
  }

  public synchronized void reportException(Throwable t) {
    if (throwable == null) {
      throwable = t;
      throwingThreadName = Thread.currentThread().getName();
      // Notify the scheduler so that the reporting thread finds the 
      // exception immediately.
      synchronized (scheduler) {
        scheduler.notifyAll();
      }
    }
  }
  
  public static class ShuffleError extends IOException {
    private static final long serialVersionUID = 5753909320586607881L;

    ShuffleError(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
