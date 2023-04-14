package com.ververica.cdc.connectors.sf.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import com.ververica.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** 并行读取 splitReader. */
public class ParallelSourceSplitReader<C extends SourceConfig>
        extends IncrementalSourceSplitReader<C> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelSourceSplitReader.class);

    private final Object lock = new Object();

    private StreamSplit streamSplit;

    private int currentReadTableNum = 0;

    public ParallelSourceSplitReader(
            int subtaskId,
            DataSourceDialect<C> dataSourceDialect,
            C sourceConfig,
            RestartStreamTaskSupplier supplier) {
        super(subtaskId, dataSourceDialect, sourceConfig);
        supplier.setSupplier(
                () -> {
                    try {
                        restartBinlogReaderIfNeed();
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException("重启任务失败", e);
                    }
                });
    }

    @Override
    public RecordsWithSplitIds<SourceRecords> fetch() throws IOException {
        synchronized (lock) {
            checkSplitOrStartNext();
            restartBinlogReaderIfNeed();
            if (isStreamFetcher() && currentFetcher.isFinished()) {
                return emptySourceRecord();
            } else {
                Iterator<SourceRecords> dataIt = null;
                try {
                    dataIt = currentFetcher.pollSplitRecords();
                } catch (InterruptedException e) {
                    LOG.warn("fetch data failed.", e);
                    throw new IOException(e);
                }
                return dataIt == null
                        ? finishedSnapshotSplit()
                        : ChangeEventRecords.forRecords(currentSplitId, dataIt);
            }
        }
    }

    protected void checkSplitOrStartNext() throws IOException {
        // the stream fetcher should keep alive
        if (isStreamFetcher()) {
            return;
        }

        if (canAssignNextSplit()) {
            final SourceSplitBase nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining.");
            }
            currentSplitId = nextSplit.splitId();

            if (nextSplit.isSnapshotSplit()) {
                if (currentFetcher == null) {
                    final FetchTask.Context taskContext =
                            dataSourceDialect.createFetchTaskContext(nextSplit, sourceConfig);
                    currentFetcher = new IncrementalSourceScanFetcher(taskContext, subtaskId);
                }
                currentFetcher.submitTask(dataSourceDialect.createFetchTask(nextSplit));
            } else {
                streamSplit = nextSplit.asStreamSplit();
                currentFetcher = new IncrementalSourceStreamFetcher(null, subtaskId);
            }
        }
    }

    public void restartBinlogReaderIfNeed() throws IOException {
        synchronized (lock) {
            if (isStreamFetcher() && currentReadTableNum < streamSplit.getTableSchemas().size()) {

                LOG.info("判断是否需要重启 binlog reader");
                stopBinlogReaderIfNeed();

                LOG.info("刷新的表信息到 config 配置文件中");
                flushTableListToConfig();

                LOG.info("启动 stream 任务，开始读取 stream 变更数据。");
                startBinlogReader();

                LOG.info("等待任务启动");
                sleep(1000 * 10);
            }
        }
    }

    private void stopBinlogReaderIfNeed() {
        if (!currentFetcher.isFinished()) {
            try {
                LOG.info("获取到上次消费到的 offset");
                Offset lastOffset =
                        ((IncrementalSourceStreamFetcher) currentFetcher).getCurrentOffset();
                if (lastOffset != null) {
                    streamSplit.setStartingOffset(lastOffset);
                }

                LOG.info("正在停止老的 stream 任务。。。");
                currentFetcher.close();

                LOG.info("正在清空队列中的数据。。。");
                currentFetcher.pollSplitRecords();
            } catch (InterruptedException e) {
                LOG.error("停止 binlog 任务失败。",e);
            }
        }
    }

    private void startBinlogReader() {
        currentReadTableNum = streamSplit.getTableSchemas().size();
        final FetchTask.Context taskContext =
                dataSourceDialect.createFetchTaskContext(streamSplit, sourceConfig);
        currentFetcher = new IncrementalSourceStreamFetcher(taskContext, subtaskId);
        currentFetcher.submitTask(dataSourceDialect.createFetchTask(streamSplit));
    }

    private void flushTableListToConfig() {
        Set<TableId> tableIds = streamSplit.getTableSchemas().keySet();
        List<String> databaseList =
                new ArrayList<>(
                        tableIds.stream()
                                .map(tableId -> tableId.catalog())
                                .collect(Collectors.toSet()));
        List<String> tableList =
                tableIds.stream()
                        .map(
                                tableId -> {
                                    if (StringUtils.isNoneEmpty(tableId.schema())) {
                                        return tableId.schema() + "." + tableId.table();
                                    } else {
                                        return tableId.catalog() + "." + tableId.table();
                                    }
                                })
                        .collect(Collectors.toList());

        JdbcSourceConfig jdbcSourceConfig = (JdbcSourceConfig) sourceConfig;
        jdbcSourceConfig.setDatabaseList(databaseList);
        jdbcSourceConfig.setTableList(tableList);

        jdbcSourceConfig
                .getDbzProperties()
                .setProperty("database.include.list", String.join(",", databaseList));
        jdbcSourceConfig
                .getDbzProperties()
                .setProperty("table.include.list", String.join(",", tableList));

        jdbcSourceConfig.setDbzConfiguration(
                Configuration.from(jdbcSourceConfig.getDbzProperties()));
    }

    private ChangeEventRecords emptySourceRecord() {
        LOG.info("stream 读取的表为空,请尽快添加表。");
        sleep(1000 * 5);
        List<SourceRecords> sourceRecordsSet = new ArrayList<>();
        return ChangeEventRecords.forRecords(currentSplitId, sourceRecordsSet.iterator());
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isStreamFetcher() {
        return currentFetcher instanceof IncrementalSourceStreamFetcher;
    }
}
