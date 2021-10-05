/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.deltalake;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta reader thread class for {@link DeltaLakeConnectorSource}.
 */
public class DeltaReaderThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(DeltaReaderThread.class);
    private final DeltaLakeConnectorSource source;
    private boolean stopped;
    private ExecutorService parseParquetExecutor;

    public DeltaReaderThread(DeltaLakeConnectorSource source, ExecutorService executor) {
        this.source = source;
        this.stopped = false;
        this.parseParquetExecutor = executor;
    }


    public void run() {
        DeltaReader reader = this.source.reader;
        DeltaCheckpoint checkpoint = source.getMinCheckpoint();
        Long startVersion = checkpoint.getSnapShotVersion();
        while (!stopped) {
            try {
                log.info("begin to read version {} ", startVersion);
                List<DeltaReader.ReadCursor> actionList = reader.getDeltaActionFromSnapShotVersion(
                        startVersion, checkpoint.isFullCopy());
                if (actionList.size() == 0) {
                    log.info("read from version: {} actionSize: {} nextVersion {}",
                            startVersion, actionList.size(), startVersion + 1);
                    Thread.sleep(1000 * 10);
                    continue;
                }
                for (int i = 0; i < actionList.size(); i++) {
                    List<DeltaReader.RowRecordData> rowRecords = reader.readParquetFile(actionList.get(i));
                    log.info("version {} actionIndex: {} rowRecordSize {}", startVersion, i, rowRecords.size());
                    rowRecords.forEach(source::enqueue);
                }
                startVersion++;
            } catch (Exception ex) {
                log.error("read data from delta lake error.", ex);
                close();
            }
        }
    }

    public void close() {
        stopped = true;
    }
}
