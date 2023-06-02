package com.ververica.cdc.connectors.base.source.enumerator;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.connectors.base.config.BoundednessConfig;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

public abstract class BoundednessEnumerator<SplitT extends SourceSplit>
        implements SplitEnumerator<SplitT, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(BoundednessEnumerator.class);

    protected final boolean bounded;
    private final Path extDataPath;

    // 这个值来源于状态
    private boolean needStop = false;
    private Long checkpointIdToFinish;
    private Map<String, Map<String, String>> extMap;

    public static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    protected BoundednessEnumerator(BoundednessConfig sourceConfig) {
        this.bounded = sourceConfig.isBounded();
        this.extDataPath = sourceConfig.extDataPath();
    }

    public void stop(Map<String, Map<String, String>> extMap) {
        this.extMap = extMap;
        needStop = true;
    }

    public PendingSplitsState snapshotState(long checkpointId) throws IOException {
        final PendingSplitsState checkpointT = doSnapshotState(checkpointId);
        LOG.info(
                getUUID()
                        + " snapshotState bounded:"
                        + bounded
                        + ";  needStop: "
                        + needStop
                        + "; checkpointIdToFinish:"
                        + checkpointIdToFinish
                        + "; checkpointId: "
                        + checkpointId);
        writeOffsetIfNeeded(checkpointId);
        return checkpointT;
    }

    private void writeOffsetIfNeeded(long checkpointId) throws IOException {
        if (bounded && needStop && this.checkpointIdToFinish == null) {
            try {
                FileSystem fileSystem = extDataPath.getFileSystem(new Configuration());
                LOG.info("init cdc dir [" + extDataPath + "]");
                mkCdcDir(fileSystem);

                final Path tmpPath =
                        new Path(extDataPath, String.format("%s.%s", getUUID(), "tmp"));
                final Path formalPath = new Path(extDataPath, getUUID());

                LOG.info("cleaning [" + tmpPath + "," + formalPath + "]");
                cleanHostPortFile(fileSystem, tmpPath, formalPath);
                LOG.info("cleaned [" + tmpPath + "," + formalPath + "]");

                LOG.info("creating [" + tmpPath + "]");
                createAndWriteHostPortTempFile(fileSystem, tmpPath);
                LOG.info("created [" + tmpPath + "]");

                this.checkpointIdToFinish = checkpointId;
            } catch (Exception e) {
                close();
                throw e;
            }
        }
    }

    private void createAndWriteHostPortTempFile(FileSystem fileSystem, Path tmpPath)
            throws JsonProcessingException {
        final String writeValue =
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(extMap);
        FileUtils.createFileInPath(
                fileSystem,
                tmpPath,
                Optional.of(writeValue.getBytes(StandardCharsets.UTF_8)),
                false);
    }

    private void cleanHostPortFile(FileSystem fileSystem, Path tmpPath, Path formalPath)
            throws IOException {
        if (fileSystem.exists(formalPath)) {
            fileSystem.delete(formalPath, false);
        }

        if (fileSystem.exists(tmpPath)) {
            fileSystem.delete(tmpPath, false);
        }
    }

    private void mkCdcDir(FileSystem fileSystem) throws IOException {
        try {
            if (!fileSystem.exists(extDataPath)) {
                fileSystem.mkdirs(extDataPath);
                LOG.info(getUUID() + " create " + extDataPath.getName());
            } else {
                LOG.info(getUUID() + " path " + extDataPath.getName() + " is exists");
            }
        } catch (Exception e) {
            LOG.warn(
                    getUUID()
                            + " create "
                            + extDataPath.getName()
                            + "error! exists "
                            + fileSystem.exists(extDataPath),
                    e);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        doNotifyCheckpointComplete(checkpointId);
        if (bounded && needStop && this.checkpointIdToFinish != null) {
            // rename 文件
            if (checkpointId == this.checkpointIdToFinish) {
                renameHostPortFile();
            } else if (checkpointId > this.checkpointIdToFinish) {
                try {
                    EnumeratorMonitor.stopAllEnumeratorIfNeeded(this);
                } catch (Exception r) {
                    close();
                    throw r;
                }
            }
        }

        LOG.info(
                getUUID()
                        + " notifyCheckpointComplete bounded:"
                        + bounded
                        + ";  needStop: "
                        + needStop
                        + "; checkpointIdToFinish:"
                        + checkpointIdToFinish
                        + "; checkpointId: "
                        + checkpointId);
    }

    public abstract void doNotifyCheckpointComplete(long checkpointId) throws Exception;

    public abstract void doStop() throws IOException;

    public abstract PendingSplitsState doSnapshotState(long checkpointId);

    protected void renameHostPortFile() throws IOException {
        final FileSystem fileSystem = extDataPath.getFileSystem(new Configuration());
        final String tmpPathStr = String.format("%s.%s", getUUID(), "tmp");
        final Path tmpPath = new Path(extDataPath, tmpPathStr);
        final Path destPath = new Path(extDataPath, getUUID());

        if (fileSystem.exists(destPath)) {
            LOG.info(destPath + " is exists.");
            return;
        }

        if (!fileSystem.exists(tmpPath)) {
            throw new IOException(tmpPath + " is not exists.");
        }
        // 重命名
        fileSystem.rename(tmpPath, destPath);
        LOG.info(
                "enumerator: "
                        + getUUID()
                        + " rename "
                        + tmpPath
                        + " to + "
                        + destPath
                        + " success. File exists: "
                        + fileSystem.exists(destPath));
    }

    public abstract String getUUID();
}
