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
package io.trino.plugin.iceberg.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/11/5 16:52
 */
public class CombinedIcebergSplit
        implements ConnectorSplit
{
    List<IcebergSplit> splits;
    private final SplitWeight splitWeight;

    @JsonCreator
    public CombinedIcebergSplit(@JsonProperty("splits") List<IcebergSplit> splits,
                                @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    @JsonProperty
    public List<IcebergSplit> getSplits()
    {
        return splits;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return splits.stream()
                .map(IcebergSplit::getAddresses)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    @Override
    public Object getInfo()
    {
        return splits.stream()
                .map(IcebergSplit::getInfo)
                .collect(toImmutableList());
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return splitWeight.getRetainedSizeInBytes() + splits.stream()
                .mapToLong(IcebergSplit::getRetainedSizeInBytes)
                .sum();
    }

    @Override
    public String toString()
    {
        MoreObjects.ToStringHelper toStringHelper = toStringHelper(this);
        splits.forEach(toStringHelper::addValue);
        return toStringHelper.toString();
    }
}
