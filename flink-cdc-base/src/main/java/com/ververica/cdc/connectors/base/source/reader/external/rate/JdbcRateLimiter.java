/*
 * Copyright 2022 Ververica Inc.
 *
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

package com.ververica.cdc.connectors.base.source.reader.external.rate;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.RateLimiter;

import io.debezium.util.ObjectSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcRateLimiter {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcRateLimiter.class);

    private final RateLimiter rateLimiter;

    private volatile int bytesPreRecord = -1;

    public JdbcRateLimiter(double permitsPerSecond) {
        rateLimiter = RateLimiter.create(permitsPerSecond);
    }

    public void setBytesPreRecord(Object obj) {
        if (bytesPreRecord <= 0) {
            this.bytesPreRecord = (int) ObjectSizeCalculator.getObjectSize(obj);
        }
    }

    public double acquire(int permits) {
        int recordSize = bytesPreRecord > 0 ? bytesPreRecord : 1024;
        LOG.info("recordSize==>" + recordSize);
        return rateLimiter.acquire(permits * recordSize);
    }
}
