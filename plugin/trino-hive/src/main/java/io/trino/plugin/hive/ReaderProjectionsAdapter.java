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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static java.util.Objects.requireNonNull;

public class ReaderProjectionsAdapter
{
    private final List<ChannelMapping> outputToInputMapping;
    private final List<Type> outputTypes;
    private final List<Type> inputTypes;

    public ReaderProjectionsAdapter(
            List<? extends ColumnHandle> expectedColumns,
            ReaderColumns readColumns,
            ColumnTypeGetter typeGetter,
            ProjectionGetter projectionGetter)
    {
        requireNonNull(expectedColumns, "expectedColumns is null");
        requireNonNull(readColumns, "readColumns is null");

        ImmutableList.Builder<ChannelMapping> mappingBuilder = ImmutableList.builder();

        for (int i = 0; i < expectedColumns.size(); i++) {
            ColumnHandle projectedColumnHandle = readColumns.getForColumnAt(i);
            int inputChannel = readColumns.getPositionForColumnAt(i);
            List<Integer> dereferences = projectionGetter.get(expectedColumns.get(i), projectedColumnHandle);

            mappingBuilder.add(new ChannelMapping(inputChannel, dereferences));
        }

        outputToInputMapping = mappingBuilder.build();

        outputTypes = expectedColumns.stream()
                .map(typeGetter::get)
                .collect(toImmutableList());

        inputTypes = readColumns.get().stream()
                .map(typeGetter::get)
                .collect(toImmutableList());
    }

    @Nullable
    public Page adaptPage(@Nullable Page input)
    {
        if (input == null) {
            return null;
        }

        Block[] blocks = new Block[outputToInputMapping.size()];

        // Prepare adaptations to extract dereferences
        for (int i = 0; i < outputToInputMapping.size(); i++) {
            ChannelMapping mapping = outputToInputMapping.get(i);

            Block inputBlock = input.getBlock(mapping.getInputChannelIndex());
            blocks[i] = createAdaptedLazyBlock(inputBlock, mapping.getDereferenceSequence(), outputTypes.get(i));
        }

        return new Page(input.getPositionCount(), blocks);
    }

    private static Block createAdaptedLazyBlock(Block inputBlock, List<Integer> dereferenceSequence, Type type)
    {
        if (dereferenceSequence.size() == 0) {
            return inputBlock;
        }

        if (inputBlock == null) {
            return null;
        }

        return new LazyBlock(inputBlock.getPositionCount(), new DereferenceBlockLoader(inputBlock, dereferenceSequence, type));
    }

    private static class DereferenceBlockLoader
            implements LazyBlockLoader
    {
        private final List<Integer> dereferenceSequence;
        private final Type type;
        private boolean loaded;
        private Block inputBlock;

        DereferenceBlockLoader(Block inputBlock, List<Integer> dereferenceSequence, Type type)
        {
            this.inputBlock = requireNonNull(inputBlock, "inputBlock is null");
            this.dereferenceSequence = requireNonNull(dereferenceSequence, "dereferenceSequence is null");
            this.type = type;
        }

        @Override
        public Block load()
        {
            checkState(!loaded, "Already loaded");
            Block loadedBlock = loadInternalBlock(dereferenceSequence, inputBlock);
            inputBlock = null;
            loaded = true;
            return loadedBlock;
        }

        /**
         * Applies dereference operations on the input block to extract the required internal block. If the input block is lazy
         * in a nested manner, this implementation avoids loading the entire input block.
         */
        private Block loadInternalBlock(List<Integer> dereferences, Block parentBlock)
        {
            if (dereferences.size() == 0) {
                return parentBlock.getLoadedBlock();
            }

            List<Block> fields = getRowFieldsFromBlock(parentBlock);

            int dereferenceIndex = dereferences.get(0);
            List<Integer> remainingDereferences = dereferences.subList(1, dereferences.size());

            Block fieldBlock = fields.get(dereferenceIndex);
            return loadInternalBlock(remainingDereferences, fieldBlock);
        }
    }

    List<ChannelMapping> getOutputToInputMapping()
    {
        return outputToInputMapping;
    }

    List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    List<Type> getInputTypes()
    {
        return inputTypes;
    }

    @VisibleForTesting
    static class ChannelMapping
    {
        private final int inputChannelIndex;
        private final List<Integer> dereferenceSequence;

        public ChannelMapping(int inputBlockIndex, List<Integer> dereferenceSequence)
        {
            checkArgument(inputBlockIndex >= 0, "inputBlockIndex cannot be negative");
            this.inputChannelIndex = inputBlockIndex;
            this.dereferenceSequence = ImmutableList.copyOf(requireNonNull(dereferenceSequence, "dereferenceSequence is null"));
        }

        public int getInputChannelIndex()
        {
            return inputChannelIndex;
        }

        public List<Integer> getDereferenceSequence()
        {
            return dereferenceSequence;
        }
    }

    public interface ColumnTypeGetter
    {
        Type get(ColumnHandle column);
    }

    public interface ProjectionGetter
    {
        List<Integer> get(ColumnHandle required, ColumnHandle read);
    }
}
