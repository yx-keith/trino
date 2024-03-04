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
package io.trino.plugin.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.split.CombinedIcebergSplit;
import io.trino.plugin.iceberg.util.DataFileWithDeleteFiles;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.*;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.TableScanUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.math.LongMath.saturatedAdd;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnHandle;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getSplitSize;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.*;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch NO_MORE_SPLITS_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final ConnectorSession session;
    private final IcebergTableHandle tableHandle;
    private final TableScan tableScan;
    private final Optional<Long> maxScannedFileSizeInBytes;
    private final Map<Integer, Type.PrimitiveType> fieldIdToType;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final Constraint constraint;
    private final TypeManager typeManager;
    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final Set<Integer> projectedBaseColumns;
    private final TupleDomain<IcebergColumnHandle> dataColumnPredicate;
    private final Domain pathDomain;
    private final Domain fileModifiedTimeDomain;
    private final OptionalLong limit;

    private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;
    private CloseableIterable<FileScanTask> fileScanIterable;
    private long targetSplitSize;
    private CloseableIterator<FileScanTask> fileScanIterator;
    private CloseableIterable<CombinedScanTask> combinedScanIterable;
    private CloseableIterator<CombinedScanTask> combinedScanIterator;
    private Iterator<FileScanTask> fileTasksIterator = emptyIterator();
    private boolean fileHasAnyDeletions;

    private final boolean recordScannedFiles;
    private final ImmutableSet.Builder<DataFileWithDeleteFiles> scannedFiles = ImmutableSet.builder();
    private long outputRowsLowerBound;
    private final boolean icebergSplitsCombineEnabled;

    public IcebergSplitSource(
            TrinoFileSystemFactory fileSystemFactory,
            ConnectorSession session,
            IcebergTableHandle tableHandle,
            TableScan tableScan,
            Optional<DataSize> maxScannedFileSize,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            TypeManager typeManager,
            boolean recordScannedFiles,
            double minimumAssignedSplitWeight,
            boolean icebergSplitsCombineEnabled)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.maxScannedFileSizeInBytes = maxScannedFileSize.map(DataSize::toBytes);
        this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.recordScannedFiles = recordScannedFiles;
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        this.projectedBaseColumns = tableHandle.getProjectedColumns().stream()
                .map(column -> column.getBaseColumnIdentity().getId())
                .collect(toImmutableSet());
        this.dataColumnPredicate = tableHandle.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        this.pathDomain = getPathDomain(tableHandle.getEnforcedPredicate());
        checkArgument(
                tableHandle.getUnenforcedPredicate().isAll() || tableHandle.getLimit().isEmpty(),
                "Cannot enforce LIMIT %s with unenforced predicate %s present",
                tableHandle.getLimit(),
                tableHandle.getUnenforcedPredicate());
        this.limit = tableHandle.getLimit();
        this.fileModifiedTimeDomain = getFileModifiedTimePathDomain(tableHandle.getEnforcedPredicate());
        this.icebergSplitsCombineEnabled = icebergSplitsCombineEnabled;
    }


    public CompletableFuture<ConnectorSplitBatch> getNextBatchBak(int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(ignored -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        if (fileScanIterable == null) {
            // Used to avoid duplicating work if the Dynamic Filter was already pushed down to the Iceberg API
            boolean dynamicFilterIsComplete = dynamicFilter.isComplete();
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate().transformKeys(IcebergColumnHandle.class::cast);
            TupleDomain<IcebergColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                    .intersect(pushedDownDynamicFilterPredicate);
            // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
            TupleDomain<IcebergColumnHandle> simplifiedPredicate = fullPredicate.simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
            boolean usedSimplifiedPredicate = !simplifiedPredicate.equals(fullPredicate);
            if (usedSimplifiedPredicate) {
                // Pushed down predicate was simplified, always evaluate it against individual splits
                this.pushedDownDynamicFilterPredicate = TupleDomain.all();
            }

            TupleDomain<IcebergColumnHandle> effectivePredicate = dataColumnPredicate
                    .intersect(simplifiedPredicate);

            if (effectivePredicate.isNone()) {
                finish();
                return completedFuture(NO_MORE_SPLITS_BATCH);
            }

            Expression filterExpression = toIcebergExpression(effectivePredicate);
            // If the Dynamic Filter will be evaluated against each file, stats are required. Otherwise, skip them.
            boolean requiresColumnStats = usedSimplifiedPredicate || !dynamicFilterIsComplete;
            TableScan scan = tableScan.filter(filterExpression);
            if (requiresColumnStats) {
                scan = scan.includeColumnStats();
            }
            this.fileScanIterable = closer.register(scan.planFiles());
            this.targetSplitSize = getSplitSize(session)
                    .map(DataSize::toBytes)
                    .orElseGet(tableScan::targetSplitSize);
            this.fileScanIterator = closer.register(fileScanIterable.iterator());
            this.fileTasksIterator = emptyIterator();
        }

        TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(IcebergColumnHandle.class::cast);
        if (dynamicFilterPredicate.isNone()) {
            finish();
            return completedFuture(NO_MORE_SPLITS_BATCH);
        }

        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize && (fileTasksIterator.hasNext() || fileScanIterator.hasNext())) {
            if (!fileTasksIterator.hasNext()) {
                FileScanTask wholeFileTask = fileScanIterator.next();
                if (wholeFileTask.deletes().isEmpty() && noDataColumnsProjected(wholeFileTask)) {
                    fileTasksIterator = List.of(wholeFileTask).iterator();
                }
                else {
                    fileTasksIterator = wholeFileTask.split(targetSplitSize).iterator();
                }
                fileHasAnyDeletions = false;
                // In theory, .split() could produce empty iterator, so let's evaluate the outer loop condition again.
                continue;
            }
            FileScanTask scanTask = fileTasksIterator.next();
            fileHasAnyDeletions = fileHasAnyDeletions || !scanTask.deletes().isEmpty();
            if (scanTask.deletes().isEmpty() &&
                    maxScannedFileSizeInBytes.isPresent() &&
                    scanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
                continue;
            }

            if (!pathDomain.includesNullableValue(utf8Slice(scanTask.file().path().toString()))) {
                continue;
            }
            if (!fileModifiedTimeDomain.isAll()) {
                long fileModifiedTime = getModificationTime(scanTask.file().path().toString());
                if (!fileModifiedTimeDomain.includesNullableValue(packDateTimeWithZone(fileModifiedTime, UTC_KEY))) {
                    continue;
                }
            }
            IcebergSplit icebergSplit = toIcebergSplit(scanTask);

            Schema fileSchema = scanTask.spec().schema();
            Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(scanTask);

            Set<IcebergColumnHandle> identityPartitionColumns = partitionKeys.keySet().stream()
                    .map(fieldId -> getColumnHandle(fileSchema.findField(fieldId), typeManager))
                    .collect(toImmutableSet());

            Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> getPartitionValues(identityPartitionColumns, partitionKeys));

            if (!dynamicFilterPredicate.isAll() && !dynamicFilterPredicate.equals(pushedDownDynamicFilterPredicate)) {
                if (!partitionMatchesPredicate(
                        identityPartitionColumns,
                        partitionValues,
                        dynamicFilterPredicate)) {
                    continue;
                }
                if (!fileMatchesPredicate(
                        fieldIdToType,
                        dynamicFilterPredicate,
                        scanTask.file().lowerBounds(),
                        scanTask.file().upperBounds(),
                        scanTask.file().nullValueCounts())) {
                    continue;
                }
            }
            if (!partitionMatchesConstraint(identityPartitionColumns, partitionValues, constraint)) {
                continue;
            }
            if (recordScannedFiles) {
                // Positional and Equality deletes can only be cleaned up if the whole table has been optimized.
                // Equality deletes may apply to many files, and position deletes may be grouped together. This makes it difficult to know if they are obsolete.
                List<org.apache.iceberg.DeleteFile> fullyAppliedDeletes = tableHandle.getEnforcedPredicate().isAll() ? scanTask.deletes() : ImmutableList.of();
                scannedFiles.add(new DataFileWithDeleteFiles(scanTask.file(), fullyAppliedDeletes));
            }
            if (!fileTasksIterator.hasNext()) {
                // This is the last task for this file
                if (!fileHasAnyDeletions) {
                    // There were no deletions, so we produced splits covering the whole file
                    outputRowsLowerBound = saturatedAdd(outputRowsLowerBound, scanTask.file().recordCount());
                    if (limit.isPresent() && limit.getAsLong() <= outputRowsLowerBound) {
                        finish();
                    }
                }
            }
            splits.add(icebergSplit);
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(ignored -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        boolean generateFileTask = !icebergSplitsCombineEnabled ? fileScanIterable == null : combinedScanIterable == null;
        if (generateFileTask) {
            // Used to avoid duplicating work if the Dynamic Filter was already pushed down to the Iceberg API
            boolean dynamicFilterIsComplete = dynamicFilter.isComplete();
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate().transformKeys(IcebergColumnHandle.class::cast);
            TupleDomain<IcebergColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                    .intersect(pushedDownDynamicFilterPredicate);
            // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
            TupleDomain<IcebergColumnHandle> simplifiedPredicate = fullPredicate.simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
            boolean usedSimplifiedPredicate = !simplifiedPredicate.equals(fullPredicate);
            if (usedSimplifiedPredicate) {
                // Pushed down predicate was simplified, always evaluate it against individual splits
                this.pushedDownDynamicFilterPredicate = TupleDomain.all();
            }

            TupleDomain<IcebergColumnHandle> effectivePredicate = dataColumnPredicate.intersect(simplifiedPredicate);
            if (effectivePredicate.isNone()) {
                finish();
                return completedFuture(NO_MORE_SPLITS_BATCH);
            }

            Expression filterExpression = toIcebergExpression(effectivePredicate);
            // If the Dynamic Filter will be evaluated against each file, stats are required. Otherwise, skip them.
            boolean requiresColumnStats = usedSimplifiedPredicate || !dynamicFilterIsComplete;
            TableScan scan = tableScan.filter(filterExpression);
            if (requiresColumnStats) {
                scan = scan.includeColumnStats();
            }
            initSplitIterator(scan);
            isFinished();
        }

        TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(IcebergColumnHandle.class::cast);
        if (dynamicFilterPredicate.isNone()) {
            finish();
            return completedFuture(NO_MORE_SPLITS_BATCH);
        }
        return completedFuture(new ConnectorSplitBatch(loadSplits(maxSize), isFinished()));
    }

    private void initSplitIterator(TableScan scan)
    {
        if (icebergSplitsCombineEnabled) {
            initCombinedScanTaskIterator(scan);
        } else {
            initFileScanTaskIterator(scan);
        }
    }

    private List<ConnectorSplit> loadSplits(int maxSize)
    {
        if (icebergSplitsCombineEnabled) {
            return loadCombineSplits(maxSize);
        }
        return loadIcebergSplits(maxSize);
    }

    private void initFileScanTaskIterator(TableScan scan)
    {
        this.fileScanIterable = TableScanUtil.splitFiles(scan.planFiles(), scan.targetSplitSize());
        closer.register(fileScanIterable);
        this.fileScanIterator = fileScanIterable.iterator();
        closer.register(fileScanIterator);
        this.targetSplitSize = getSplitSize(session)
                .map(DataSize::toBytes)
                .orElseGet(tableScan::targetSplitSize);
        this.fileTasksIterator = emptyIterator();
    }

    private void initCombinedScanTaskIterator(TableScan scan)
    {
        this.combinedScanIterable = scan.planTasks();
        closer.register(combinedScanIterable);
        this.combinedScanIterator = combinedScanIterable.iterator();
        closer.register(combinedScanIterator);
    }

    private List<ConnectorSplit> loadIcebergSplits(int maxSize)
    {
        //v2
/*        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize && (fileTasksIterator.hasNext() || fileScanIterator.hasNext())) {
            if (!fileTasksIterator.hasNext()) {
                FileScanTask wholeFileTask = fileScanIterator.next();
                if (wholeFileTask.deletes().isEmpty() && noDataColumnsProjected(wholeFileTask)) {
                    fileTasksIterator = List.of(wholeFileTask).iterator();
                }
                else {
                    fileTasksIterator = wholeFileTask.split(targetSplitSize).iterator();
                }
                fileHasAnyDeletions = false;
                // In theory, .split() could produce empty iterator, so let's evaluate the outer loop condition again.
                continue;
            }
            FileScanTask scanTask = fileTasksIterator.next();
            fileHasAnyDeletions = fileHasAnyDeletions || !scanTask.deletes().isEmpty();
            IcebergSplit icebergSplit = createSplit(scanTask).orElse(null);

            if (!fileTasksIterator.hasNext()) {
                // This is the last task for this file
                if (!fileHasAnyDeletions) {
                    // There were no deletions, so we produced splits covering the whole file
                    outputRowsLowerBound = saturatedAdd(outputRowsLowerBound, scanTask.file().recordCount());
                    if (limit.isPresent() && limit.getAsLong() <= outputRowsLowerBound) {
                        finish();
                    }
                }
            }
            splits.add(icebergSplit);
        }*/

        //v3
        Iterator<FileScanTask> fileScanTasks = Iterators.limit(fileScanIterator, maxSize);
        List<ConnectorSplit> splits = new ArrayList<>();
        while (fileScanTasks.hasNext()) {
            FileScanTask scanTask = fileScanTasks.next();
            IcebergSplit icebergSplit = createSplit(scanTask).orElse(null);
            splits.add(icebergSplit);
        }
        return splits;
    }

    private List<ConnectorSplit> loadCombineSplits(int maxSize)
    {
        Iterator<CombinedScanTask> combinedScanTasks = Iterators.limit(combinedScanIterator, maxSize);
        List<ConnectorSplit> combineSplits = new ArrayList<>();
        while(combinedScanTasks.hasNext()) {
            CombinedScanTask combinedScanTask = combinedScanTasks.next();
            List<IcebergSplit> splits = combinedScanTask.files().stream()
                    .map(fileScanTask -> createSplit(fileScanTask))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());
            SplitWeight splitWeight = SplitWeight.fromProportion((double) splits.stream().mapToLong(IcebergSplit::getLength).sum() / tableScan.targetSplitSize());
            combineSplits.add(new CombinedIcebergSplit(splits, splitWeight));
        }
        return combineSplits;
    }

    public Optional<IcebergSplit> createSplit(FileScanTask scanTask)
    {
        if (scanTask.deletes().isEmpty() && maxScannedFileSizeInBytes.isPresent() && scanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
            return Optional.empty();
        }

        if (!pathDomain.includesNullableValue(utf8Slice(scanTask.file().path().toString()))) {
            return Optional.empty();
        }
        if (!fileModifiedTimeDomain.isAll()) {
            long fileModifiedTime = getModificationTime(scanTask.file().path().toString());
            if (!fileModifiedTimeDomain.includesNullableValue(packDateTimeWithZone(fileModifiedTime, UTC_KEY))) {
                return Optional.empty();
            }
        }

        Schema fileSchema = scanTask.spec().schema();
        Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(scanTask);

        Set<IcebergColumnHandle> identityPartitionColumns = partitionKeys.keySet().stream()
                .map(fieldId -> getColumnHandle(fileSchema.findField(fieldId), typeManager))
                .collect(toImmutableSet());

        Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> getPartitionValues(identityPartitionColumns, partitionKeys));

        TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(IcebergColumnHandle.class::cast);
        if (!dynamicFilterPredicate.isAll() && !dynamicFilterPredicate.equals(pushedDownDynamicFilterPredicate)) {
            if (!partitionMatchesPredicate(
                    identityPartitionColumns,
                    partitionValues,
                    dynamicFilterPredicate)) {
                return Optional.empty();
            }
            if (!fileMatchesPredicate(
                    fieldIdToType,
                    dynamicFilterPredicate,
                    scanTask.file().lowerBounds(),
                    scanTask.file().upperBounds(),
                    scanTask.file().nullValueCounts())) {
                return Optional.empty();
            }
        }
        if (!partitionMatchesConstraint(identityPartitionColumns, partitionValues, constraint)) {
            return Optional.empty();
        }
        // Positional and Equality deletes can only be cleaned up if the whole table has been optimized.
        // Equality deletes may apply to many files, and position deletes may be grouped together. This makes it difficult to know if they are obsolete.
        if (recordScannedFiles) {
            List<org.apache.iceberg.DeleteFile> fullyAppliedDeletes = tableHandle.getEnforcedPredicate().isAll() ? scanTask.deletes() : ImmutableList.of();
            scannedFiles.add(new DataFileWithDeleteFiles(scanTask.file(), fullyAppliedDeletes));
        }
        return Optional.of(toIcebergSplit(scanTask));
    }

    private boolean noDataColumnsProjected(FileScanTask fileScanTask)
    {
        return fileScanTask.spec().fields().stream()
                .filter(partitionField -> partitionField.transform().isIdentity())
                .map(PartitionField::sourceId)
                .collect(toImmutableSet())
                .containsAll(projectedBaseColumns);
    }

    private long getModificationTime(String path)
    {
        try {
            TrinoInputFile inputFile = fileSystemFactory.create(session).newInputFile(Location.of(path));
            return inputFile.lastModified().toEpochMilli();
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to get file modification time: " + path, e);
        }
    }

    private void finish()
    {
        close();
        if(icebergSplitsCombineEnabled) {
            this.combinedScanIterable = CloseableIterable.empty();
            this.combinedScanIterator = CloseableIterator.empty();
        } else {
            this.fileScanIterable = CloseableIterable.empty();
            this.fileScanIterator = CloseableIterator.empty();
            this.fileTasksIterator = emptyIterator();
        }
    }

    @Override
    public boolean isFinished()
    {
        if(icebergSplitsCombineEnabled) {
            return combinedScanIterable != null && !combinedScanIterator.hasNext();
        }
        return fileScanIterator != null && !fileScanIterator.hasNext() && !fileTasksIterator.hasNext();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.copyOf(scannedFiles.build()));
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    static boolean fileMatchesPredicate(
            Map<Integer, Type.PrimitiveType> primitiveTypeForFieldId,
            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate,
            @Nullable Map<Integer, ByteBuffer> lowerBounds,
            @Nullable Map<Integer, ByteBuffer> upperBounds,
            @Nullable Map<Integer, Long> nullValueCounts)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow();

        for (Map.Entry<IcebergColumnHandle, Domain> domainEntry : domains.entrySet()) {
            IcebergColumnHandle column = domainEntry.getKey();
            Domain domain = domainEntry.getValue();

            int fieldId = column.getId();
            boolean mayContainNulls;
            if (nullValueCounts == null) {
                mayContainNulls = true;
            }
            else {
                Long nullValueCount = nullValueCounts.get(fieldId);
                mayContainNulls = nullValueCount == null || nullValueCount > 0;
            }
            Type type = primitiveTypeForFieldId.get(fieldId);
            Domain statisticsDomain = domainForStatistics(
                    column,
                    lowerBounds == null ? null : fromByteBuffer(type, lowerBounds.get(fieldId)),
                    upperBounds == null ? null : fromByteBuffer(type, upperBounds.get(fieldId)),
                    mayContainNulls);
            if (!domain.overlaps(statisticsDomain)) {
                return false;
            }
        }
        return true;
    }

    private static Domain domainForStatistics(
            IcebergColumnHandle columnHandle,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            boolean mayContainNulls)
    {
        io.trino.spi.type.Type type = columnHandle.getType();
        Type icebergType = toIcebergType(type, columnHandle.getColumnIdentity());
        if (lowerBound == null && upperBound == null) {
            return Domain.create(ValueSet.all(type), mayContainNulls);
        }

        Range statisticsRange;
        if (lowerBound != null && upperBound != null) {
            statisticsRange = Range.range(
                    type,
                    convertIcebergValueToTrino(icebergType, lowerBound),
                    true,
                    convertIcebergValueToTrino(icebergType, upperBound),
                    true);
        }
        else if (upperBound != null) {
            statisticsRange = Range.lessThanOrEqual(type, convertIcebergValueToTrino(icebergType, upperBound));
        }
        else {
            statisticsRange = Range.greaterThanOrEqual(type, convertIcebergValueToTrino(icebergType, lowerBound));
        }
        return Domain.create(ValueSet.ofRanges(statisticsRange), mayContainNulls);
    }

    static boolean partitionMatchesConstraint(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            Constraint constraint)
    {
        // We use Constraint just to pass functional predicate here from DistributedExecutionPlanner
        verify(constraint.getSummary().isAll());

        if (constraint.predicate().isEmpty() ||
                intersection(constraint.getPredicateColumns().orElseThrow(), identityPartitionColumns).isEmpty()) {
            return true;
        }
        return constraint.predicate().get().test(partitionValues.get());
    }

    @VisibleForTesting
    static boolean partitionMatchesPredicate(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow();

        for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
            Domain allowedDomain = domains.get(partitionColumn);
            if (allowedDomain != null) {
                if (!allowedDomain.includesNullableValue(partitionValues.get().get(partitionColumn).getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    private IcebergSplit toIcebergSplit(FileScanTask task)
    {
        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().recordCount(),
                IcebergFileFormat.fromIceberg(task.file().format()),
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.file().partition()),
                task.deletes().stream()
                        .map(DeleteFile::fromIceberg)
                        .collect(toImmutableList()),
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / tableScan.targetSplitSize(), minimumAssignedSplitWeight), 1.0)));
    }

    private static Domain getPathDomain(TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        IcebergColumnHandle pathColumn = pathColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(pathColumn);
        if (domain == null) {
            return Domain.all(pathColumn.getType());
        }
        return domain;
    }

    private static Domain getFileModifiedTimePathDomain(TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        IcebergColumnHandle fileModifiedTimeColumn = fileModifiedTimeColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(fileModifiedTimeColumn);
        if (domain == null) {
            return Domain.all(fileModifiedTimeColumn.getType());
        }
        return domain;
    }
}
