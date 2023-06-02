package com.ververica.cdc.connectors.base.source.enumerator;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.BoundednessConfig;
import com.ververica.cdc.connectors.base.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class EnumeratorMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(EnumeratorMonitor.class);
    private static Map<BoundednessEnumerator, Boolean> enumeratorMap = new ConcurrentHashMap<>();
    private static Map<String, BoundednessEnumerator> enumerators = new ConcurrentHashMap<>();
    private static volatile Path extDataPath;
    private static volatile Path offsetHistoryPath;
    private static volatile Path offsetHistoryPathTmp;
    private static volatile boolean isOk = false;

    // private static Map<BoundednessEnumerator, String> enumeratorExt = new ConcurrentHashMap<>();

    // private static ThreadFactory threadFactory =
    //         new ThreadFactoryBuilder().setNameFormat("EnumeratorMonitor").build();
    // private static ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);

    public static void addEnumerator(
            BoundednessEnumerator enumerator, BoundednessConfig sourceConfig) {
        final BoundednessEnumerator boundednessEnumerator = enumerators.get(enumerator.getUUID());
        if (boundednessEnumerator != null) {
            enumeratorMap.remove(boundednessEnumerator);
        }

        enumeratorMap.put(enumerator, false);
        enumerators.put(enumerator.getUUID(), enumerator);
        if (extDataPath == null) {
            extDataPath = sourceConfig.extDataPath();
            offsetHistoryPath = new Path(extDataPath, "offsetHistory");
            offsetHistoryPathTmp = new Path(extDataPath, "offsetHistory.tmp");
        }
        LOG.info(
                "added enumerator:"
                        + enumerator
                        + " to enumeratorMap, size "
                        + enumeratorMap.size()
                        + "; extDataPath: "
                        + extDataPath);
        // 删除 offsetHistoryPath 文件
        delete();
    }

    public static void stopAllEnumeratorIfNeeded(BoundednessEnumerator enumerator)
            throws IOException {
        enumeratorMap.put(enumerator, true);

        final boolean allEnumeratorShouldStop = enumeratorMap.values().stream().allMatch(x -> x);

        // 当从状态恢复时 有些 库可能已经跑完了， 就不会再 上报 所以检测 目录
        final boolean stop = allEnumeratorShouldStop || isAllReady();

        LOG.info(
                "received enumerator:"
                        + enumerator.getUUID()
                        + " to topAll, allEnumeratorShouldStop: "
                        + allEnumeratorShouldStop
                        + ", stop:"
                        + stop
                        + ", isOk:"
                        + isOk);

        if (stop) {
            if (!isOk) {
                // 文件聚合
                collectOffset();
            }

            enumerator.doStop();
            enumeratorMap.remove(enumerator);
            LOG.info(
                    "stopped enumerator:"
                            + enumerator.getUUID()
                            + " remove from enumeratorMap size "
                            + enumeratorMap.size());
        } else {
            enumeratorMap.forEach(
                    (k, v) -> {
                        LOG.info("enumerator:" + k.getUUID() + " needStop: " + v);
                    });
        }
    }

    // 检测文件个数

    public static boolean isAllReady() {
        final boolean isAllReady =
                enumerators.keySet().stream()
                        .allMatch(
                                x -> {
                                    try {
                                        final boolean exists =
                                                extDataPath
                                                        .getFileSystem(new Configuration())
                                                        .exists(new Path(extDataPath, x));
                                        LOG.info(new Path(extDataPath, x) + "  exists:" + exists);
                                        return exists;
                                    } catch (IOException e) {
                                        LOG.error(new Path(extDataPath, x) + "  check error.", e);
                                        return false;
                                    }
                                });
        LOG.info(extDataPath + "  isAllReady: " + isAllReady);
        return isAllReady;
    }

    public static synchronized void collectOffset() {
        try {
            final FileSystem fileSystem = extDataPath.getFileSystem(new Configuration());
            if (fileSystem.exists(offsetHistoryPath)) {
                LOG.info("collectOffset but " + offsetHistoryPath + " is exists");
                return;
            }
            Map<String, Map> collect = new HashMap<>();
            for (String uuid : enumerators.keySet()) {
                final Path path = new Path(extDataPath, uuid);
                final Optional<byte[]> bytes = FileUtils.readDataFromPath(fileSystem, path, false);
                final String string = new String(bytes.get(), StandardCharsets.UTF_8);
                final Map jsonNode = BoundednessEnumerator.MAPPER.readValue(string, Map.class);
                collect.put(uuid, jsonNode);
            }

            final String writeValue =
                    BoundednessEnumerator.MAPPER
                            .writerWithDefaultPrettyPrinter()
                            .writeValueAsString(collect);
            LOG.info("begin create " + offsetHistoryPathTmp);
            // 写临时文件
            FileUtils.createFileInPath(
                    fileSystem,
                    offsetHistoryPathTmp,
                    Optional.of(writeValue.getBytes(StandardCharsets.UTF_8)),
                    false);
            LOG.info("begin rename " + offsetHistoryPathTmp + " to " + offsetHistoryPath);
            // rename 成正式文件
            fileSystem.rename(offsetHistoryPathTmp, offsetHistoryPath);
            isOk = true;
            LOG.info("rename " + offsetHistoryPathTmp + " to " + offsetHistoryPath + " success");

        } catch (Exception e) {
            delete();
            LOG.error(extDataPath + "  check error.", e);
        }
    }

    public static synchronized void delete() {
        final FileSystem fileSystem;
        try {
            fileSystem = extDataPath.getFileSystem(new Configuration());
            LOG.info("delete " + offsetHistoryPath);
            if (fileSystem.exists(offsetHistoryPath)) {
                fileSystem.delete(offsetHistoryPath, false);
            }

            LOG.info("delete " + offsetHistoryPathTmp);
            if (fileSystem.exists(offsetHistoryPathTmp)) {
                fileSystem.delete(offsetHistoryPathTmp, false);
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException("delete ok file failed", e);
        }
    }
}
