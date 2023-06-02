package com.ververica.cdc.connectors.base.utils;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

public class FileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public static void createFileInPath(
            FileSystem fileSystem,
            org.apache.hadoop.fs.Path fullPath,
            Optional<byte[]> content,
            boolean ignoreIOE) {
        try {
            // If the path does not exist, create it first
            if (!fileSystem.exists(fullPath)) {
                if (fileSystem.createNewFile(fullPath)) {
                    LOG.info("Created a new file in meta path: " + fullPath);
                } else {
                    throw new FlinkRuntimeException("Failed to create file " + fullPath);
                }
            }

            if (content.isPresent()) {
                FSDataOutputStream fsout = fileSystem.create(fullPath, true);
                fsout.write(content.get());
                fsout.close();
            }
        } catch (IOException e) {
            LOG.warn("Failed to create file " + fullPath, e);
            if (!ignoreIOE) {
                throw new FlinkRuntimeException("Failed to create file " + fullPath, e);
            }
        }
    }

    public static Optional<byte[]> readDataFromPath(
            FileSystem fileSystem, org.apache.hadoop.fs.Path detailPath, boolean ignoreIOE) {
        try (FSDataInputStream is = fileSystem.open(detailPath)) {
            return Optional.of(readAsByteArray(is));
        } catch (IOException e) {
            LOG.warn("Could not read details from " + detailPath, e);
            if (!ignoreIOE) {
                throw new FlinkRuntimeException("Could not read details from " + detailPath, e);
            }
            return Optional.empty();
        }
    }

    public static byte[] readAsByteArray(InputStream input) throws IOException {
        return readAsByteArray(input, 128);
    }

    public static byte[] readAsByteArray(InputStream input, int outputSize) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(outputSize);
        copy(input, bos);
        return bos.toByteArray();
    }

    public static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[1024];
        int len;
        while ((len = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, len);
        }
    }
}
