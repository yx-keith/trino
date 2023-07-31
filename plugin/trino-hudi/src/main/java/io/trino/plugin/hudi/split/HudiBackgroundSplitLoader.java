/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.split;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.MoreFutures;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfoLoader;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hudi.HudiSessionProperties.getPartitionLoaderParallelism;
import static java.util.Objects.requireNonNull;

public class HudiBackgroundSplitLoader
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final ExecutorService executor;
    private final Consumer<Throwable> errorListener;
    private final HudiSplitFactory hudiSplitFactory;
    private final Deque<List<String>> partitionNamesQueue;
    private final Deque<HudiPartitionInfo> partitionInfoQueue;
    private final ScheduledExecutorService partitionLoaderExecutor;
    private final Deque<Boolean> partitionLoadStatusQueue;
    private final int partitionLoaderNumThreads;


    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            HudiDirectoryLister hudiDirectoryLister,
            AsyncQueue<ConnectorSplit> asyncQueue,
            ExecutorService executor,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            Deque<List<String>> partitionNamesQueue,
            Deque<HudiPartitionInfo> partitionInfoQueue,
            ScheduledExecutorService partitionLoaderExecutor,
            Consumer<Throwable> errorListener)
    {
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.errorListener = requireNonNull(errorListener, "errorListener is null");
        this.hudiSplitFactory = new HudiSplitFactory(tableHandle, hudiSplitWeightProvider);
        this.partitionNamesQueue = requireNonNull(partitionNamesQueue, "partitionNamesQueue is null");
        this.partitionInfoQueue = requireNonNull(partitionInfoQueue, "partitionInfoDeque is null");
        this.partitionLoaderExecutor = requireNonNull(partitionLoaderExecutor, "partitionLoaderExecutor is null");
        this.partitionLoadStatusQueue = new ConcurrentLinkedDeque<>();
        this.partitionLoaderNumThreads = getPartitionLoaderParallelism(session);
    }


    public void start()
    {
        List<ListenableFuture<Void>> splitFutures = new ArrayList<>();
        for (int i = 0; i < partitionLoaderNumThreads; i++) {
            partitionLoadStatusQueue.offer(true);
            HudiPartitionInfoLoader partitionInfoLoader = new HudiPartitionInfoLoader(partitionNamesQueue, hudiDirectoryLister, partitionInfoQueue, partitionLoadStatusQueue);
            Futures.submit(partitionInfoLoader, partitionLoaderExecutor);
        }

        while (!partitionLoadStatusQueue.isEmpty() || !partitionInfoQueue.isEmpty()) {
            if (partitionInfoQueue.isEmpty()) {
                continue;
            }
            HudiPartitionInfo partition = partitionInfoQueue.poll();
            ListenableFuture<Void> splitsFuture = Futures.submit(() -> loadSplits(partition), executor);
            splitFutures.add(splitsFuture);
        }

        Futures.whenAllComplete(splitFutures).run(asyncQueue::finish, directExecutor());
    }


    private void loadSplits(HudiPartitionInfo partition)
    {
        List<HivePartitionKey> partitionKeys = partition.getHivePartitionKeys();
        List<FileStatus> partitionFiles = hudiDirectoryLister.listStatus(partition);
        partitionFiles.stream()
                .flatMap(fileStatus -> hudiSplitFactory.createSplits(partitionKeys, fileStatus))
                .map(asyncQueue::offer)
                .forEachOrdered(MoreFutures::getFutureValue);
    }

    private <T> void hookErrorListener(ListenableFuture<T> future)
    {
        Futures.addCallback(future, new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T result) {}

            @Override
            public void onFailure(Throwable t)
            {
                errorListener.accept(t);
            }
        }, directExecutor());
    }
}
