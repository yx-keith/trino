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
package io.trino.plugin.hudi;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.GenericHiveRecordCursor;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormatName;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiUtil.getFile;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.*;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/9/6 15:04
 */
public class HudiRecordCursor
{
    public static GenericHiveRecordCursor createRealTimeRecordCursor(ConnectorSession session, HdfsEnvironment hdfsEnvironment, HudiSplit split, Properties schema, List<HiveColumnHandle> columns, String basePath)
    {
        HudiFile hudiFile = getFile(split);
        Path logFilePath = new Path(hudiFile.getPath());

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session), new Path(basePath));
        return hdfsEnvironment.doAs(session.getIdentity(), () -> {
            RecordReader<?, ?> recordReader = createRecordReader(configuration,
                    split,
                    schema,
                    columns,
                    basePath);

            return new GenericHiveRecordCursor(configuration,
                    logFilePath,
                    genericRecordReader(recordReader),
                    hudiFile.getLength(),
                    schema,
                    columns);
        });
    }

    public static RecordReader<?, ?> createRecordReader(Configuration configuration, HudiSplit split, Properties schema, List<HiveColumnHandle> columns, String basePath)
    {
        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = columns.stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .collect(toImmutableList());

        // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
        setReadColumns(configuration, readColumns);

        InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema);
        JobConf jobConf = toJobConf(configuration);

        HudiFile logFile = getFile(split);
        Path logFilePath = new Path(logFile.getPath());
        List<HoodieLogFile> logFiles = split.getLogFiles().stream().map(file -> new HoodieLogFile(file.getPath())).collect(Collectors.toList());
        FileSplit fileSplit = new FileSplit(logFilePath, logFile.getStart(), logFile.getLength(), (String[]) null);

        schema.stringPropertyNames().stream()
                .forEach(name -> jobConf.set(name, schema.getProperty(name)));
        configureCompressionCodecs(jobConf);

        FileSplit hudiSplit;
        try {
            hudiSplit = new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, split.getCommitTime(), false, Option.empty());
            @SuppressWarnings("unchecked")
            RecordReader<? extends WritableComparable<?>, ? extends Writable> recordReader = (RecordReader<? extends WritableComparable<?>, ? extends Writable>)
                    inputFormat.getRecordReader(hudiSplit, jobConf, Reporter.NULL);
            return recordReader;
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, format("Error opening Hudi split %s (offset=%s, length=%s) using %s: %s",
                    logFilePath,
                    logFile.getStart(),
                    logFile.getLength(),
                    getInputFormatName(schema),
                    firstNonNull(e.getMessage(), e.getClass().getName())),
                    e);
        }
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, ? extends Writable> genericRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, ? extends Writable>) recordReader;
    }

    public static InputFormat<?, ?> getInputFormat(Configuration configuration, Properties schema)
    {
        String inputFormatName = getInputFormatName(schema);
        JobConf jobConf = toJobConf(configuration);
        try {
            Class<? extends InputFormat<?, ?>> inputFormatClass = getInputFormatClass(jobConf, inputFormatName);
            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        } catch (ClassNotFoundException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Unable to create input format " + inputFormatName, e);
        }
    }

    @SuppressWarnings({"unchecked", "RedundantCast"})
    private static Class<? extends InputFormat<?, ?>> getInputFormatClass(JobConf conf, String inputFormatName)
            throws ClassNotFoundException
    {
        Class<?> clazz = conf.getClassByName(inputFormatName);
        return (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
    }

    public static JobConf toJobConf(Configuration conf)
    {
        if (conf instanceof JobConf) {
            return (JobConf) conf;
        }
        return new JobConf(conf);
    }

    private static void configureCompressionCodecs(JobConf jobConf)
    {
        // add Airlift LZO and LZOP to head of codecs list so as to not override existing entries
        List<String> codecs = newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().split(jobConf.get("io.compression.codecs", "")));
        if (!codecs.contains(LzoCodec.class.getName())) {
            codecs.add(0, LzoCodec.class.getName());
        }
        if (!codecs.contains(LzopCodec.class.getName())) {
            codecs.add(0, LzopCodec.class.getName());
        }
        jobConf.set("io.compression.codecs", codecs.stream().collect(joining(",")));
    }

    public static void setReadColumns(Configuration configuration, List<HiveColumnHandle> readColumns)
    {
        configuration.set(READ_COLUMN_IDS_CONF_STR, join(readColumns, HiveColumnHandle::getBaseHiveColumnIndex));
        configuration.set(READ_COLUMN_NAMES_CONF_STR, join(readColumns, HiveColumnHandle::getName));
        configuration.setBoolean(READ_ALL_COLUMNS, false);
    }

    private static <T, R> String join(List<T> list, Function<T, R> function)
    {
        return Joiner.on(',').join(list.stream().map(function).iterator());
    }
}
