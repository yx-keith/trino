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
package io.trino.plugin.hudi.partition;

import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hudi.exception.HoodieIOException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiSessionProperties.getMaxPartitionBatchSize;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinPartitionBatchSize;

public class HudiPartitionInfoLoader
        implements Runnable
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final Deque<HudiPartitionInfo> partitionQueue;

    public HudiPartitionInfoLoader(
            ConnectorSession session,
            HudiDirectoryLister hudiDirectoryLister)
    {
        this.hudiDirectoryLister = hudiDirectoryLister;
        this.partitionQueue = new ConcurrentLinkedDeque<>();
    }

    @Override
    public void run()
    {
        List<HudiPartitionInfo> hudiPartitionInfoList = hudiDirectoryLister.getPartitionsToScan().stream()
                .sorted(Comparator.comparing(HudiPartitionInfo::getComparingKey))
                .collect(Collectors.toList());

        // empty partitioned table
        if (hudiPartitionInfoList.isEmpty()) {
            return;
        }

        // non-partitioned table
        if (hudiPartitionInfoList.size() == 1 && hudiPartitionInfoList.get(0).getHivePartitionName().isEmpty()) {
            partitionQueue.addAll(hudiPartitionInfoList);
            return;
        }

        partitionQueue.addAll(hudiPartitionInfoList);
    }

    public Deque<HudiPartitionInfo> getPartitionQueue()
    {
        return partitionQueue;
    }
}
