/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.trs.pacifica.core;

import com.trs.pacifica.ConfigurationClient;
import com.trs.pacifica.LogStorageFactory;
import com.trs.pacifica.SnapshotStorageFactory;
import com.trs.pacifica.StateMachine;
import com.trs.pacifica.async.thread.ExecutorGroup;
import com.trs.pacifica.async.thread.ReplicaExecutorGroupHolder;
import com.trs.pacifica.fs.FileServiceFactory;
import com.trs.pacifica.fs.FileServiceFactoryHolder;
import com.trs.pacifica.log.codec.LogEntryCodecFactory;
import com.trs.pacifica.log.codec.LogEntryCodecFactoryHolder;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.EndpointManager;
import com.trs.pacifica.rpc.node.EndpointManagerHolder;
import com.trs.pacifica.util.thread.ThreadUtil;
import com.trs.pacifica.util.timer.TimerFactory;
import com.trs.pacifica.util.timer.TimerFactoryHolder;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReplicaOption {

    public static final int DEFAULT_MAX_OPERATION_NUM_PER_BATCH = 16;
    public static final int DEFAULT_GRACE_PERIOD_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(120);
    public static final int MIN_GRACE_PERIOD_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(10);
    public static final int MAX_GRACE_PERIOD_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(600);
    public static final int DEFAULT_SNAPSHOT_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(3);
    public static final int DEFAULT_RECOVER_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(10);
    public static final int DEFAULT_RECOVER_INTERVAL_MS = (int) TimeUnit.SECONDS.toMillis(30);

    public static final int DEFAULT_SNAPSHOT_LOG_INDEX_RESERVED = 10;
    public static final int DEFAULT_SNAPSHOT_LOG_INDEX_MARGIN = 0;
    public static final boolean DEFAULT_ENABLE_LOG_ENTRY_CHECKSUM = true;
    public static final int DEFAULT_DOWNLOAD_SNAPSHOT_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(10);


    /**
     * Grace period. If the time limit of the Secondary detection is exceeded,
     * the Primary is considered to be faulty, and the Primary change request is sent
     */
    private int gracePeriodTimeoutMs = DEFAULT_GRACE_PERIOD_TIMEOUT_MS; // default 120 s

    /**
     * lease period timeout ms= gracePeriodTimeoutMs * leasePeriodTimeoutRatio/100
     */
    private int leasePeriodTimeoutRatio = 80;

    /**
     * factor of heartbeat intervals between replicas
     * heartbeat intervals = lease period timeout * factor / 100
     */
    private int heartBeatFactor = 40;

    /**
     * snapshot timeout ms
     */
    private int snapshotTimeoutMs = DEFAULT_SNAPSHOT_TIMEOUT_MS;


    private int recoverTimeoutMs = DEFAULT_RECOVER_TIMEOUT_MS;
    private int recoverIntervalMs = DEFAULT_RECOVER_INTERVAL_MS;

    private int snapshotLogIndexReserved = DEFAULT_SNAPSHOT_LOG_INDEX_RESERVED;

    // A snapshot saving would be triggered every |snapshot_interval_s| seconds,
    // and at this moment when state machine's commitPoint.index value
    // minus lastSnapshotLogId.index value is greater than snapshotLogIndexMargin value,
    // the snapshot action will be done really.
    // If |snapshotLogIndexMargin| < 0, the distance based snapshot would be disable.
    //
    // Default: 0
    private int snapshotLogIndexMargin = DEFAULT_SNAPSHOT_LOG_INDEX_MARGIN;

    /**
     * path of the log storage
     */
    private String logStoragePath;

    /**
     * path of the snapshot storage
     */
    private String snapshotStoragePath;
    private LogStorageFactory logStorageFactory = LogStorageFactoryHolder.getInstance();
    private SnapshotStorageFactory snapshotStorageFactory = SnapshotStorageFactoryHolder.getInstance();

    private ConfigurationClient configurationClient;

    private ExecutorGroup applyExecutorGroup = ReplicaExecutorGroupHolder.getDefaultInstance();

    private ExecutorGroup logManagerExecutorGroup = ReplicaExecutorGroupHolder.getDefaultInstance();

    private ExecutorGroup fsmCallerExecutorGroup = ReplicaExecutorGroupHolder.getDefaultInstance();

    private ExecutorGroup senderExecutorGroup = ReplicaExecutorGroupHolder.getDefaultInstance();
    private ScheduledExecutorService senderScheduler = ThreadUtil._JPACIFICA_SCHEDULE_EXECUTOR;
    private Executor downloadSnapshotExecutor = ReplicaExecutorGroupHolder.getDefaultInstance();

    /**
     * download snapshot timeout ms
     */
    private int downloadSnapshotTimeoutMs = DEFAULT_DOWNLOAD_SNAPSHOT_TIMEOUT_MS;


    private LogEntryCodecFactory logEntryCodecFactory = LogEntryCodecFactoryHolder.getInstance();

    private TimerFactory timerFactory = TimerFactoryHolder.getInstance();

    private boolean enableLogEntryChecksum = DEFAULT_ENABLE_LOG_ENTRY_CHECKSUM;

    private StateMachine stateMachine;

    private EndpointManager endpointFactory = EndpointManagerHolder.getInstance();

    private RpcClient rpcClient;

    private FileServiceFactory fileServiceFactory = FileServiceFactoryHolder.getInstance();


    /**
     * operation consumer num per batch, The minimum cannot be less than 1
     */
    private int maxOperationNumPerBatch = DEFAULT_MAX_OPERATION_NUM_PER_BATCH;

    public int getGracePeriodTimeoutMs() {
        return gracePeriodTimeoutMs;
    }

    public void setGracePeriodTimeoutMs(int gracePeriodTimeoutMs) {
        this.gracePeriodTimeoutMs = gracePeriodTimeoutMs;
    }

    public int getLeasePeriodTimeoutRatio() {
        return leasePeriodTimeoutRatio;
    }

    public int getLeasePeriodTimeoutMs() {
        return this.gracePeriodTimeoutMs * this.leasePeriodTimeoutRatio / 100;
    }

    public void setLeasePeriodTimeoutRatio(int leasePeriodTimeoutRatio) {
        this.leasePeriodTimeoutRatio = leasePeriodTimeoutRatio;
    }


    public int getSnapshotTimeoutMs() {
        return snapshotTimeoutMs;
    }

    public void setSnapshotTimeoutMs(int snapshotTimeoutMs) {
        this.snapshotTimeoutMs = snapshotTimeoutMs;
    }

    public int getRecoverTimeoutMs() {
        return recoverTimeoutMs;
    }

    public void setRecoverTimeoutMs(int recoverTimeoutMs) {
        this.recoverTimeoutMs = recoverTimeoutMs;
    }

    public ConfigurationClient getConfigurationClient() {
        return configurationClient;
    }

    public void setConfigurationClient(ConfigurationClient configurationClient) {
        this.configurationClient = configurationClient;
    }

    public LogStorageFactory getLogStorageFactory() {
        return logStorageFactory;
    }

    public void setLogStorageFactory(LogStorageFactory logStorageFactory) {
        this.logStorageFactory = logStorageFactory;
    }

    public SnapshotStorageFactory getSnapshotStorageFactory() {
        return snapshotStorageFactory;
    }

    public void setSnapshotStorageFactory(SnapshotStorageFactory snapshotStorageFactory) {
        this.snapshotStorageFactory = snapshotStorageFactory;
    }

    public String getLogStoragePath() {
        return logStoragePath;
    }

    public void setLogStoragePath(String logStoragePath) {
        this.logStoragePath = logStoragePath;
    }


    public String getSnapshotStoragePath() {
        return snapshotStoragePath;
    }

    public void setSnapshotStoragePath(String snapshotStoragePath) {
        this.snapshotStoragePath = snapshotStoragePath;
    }

    public ExecutorGroup getApplyExecutorGroup() {
        return applyExecutorGroup;
    }

    public void setApplyExecutorGroup(ExecutorGroup applyExecutorGroup) {
        this.applyExecutorGroup = applyExecutorGroup;
    }

    public int getMaxOperationNumPerBatch() {
        return maxOperationNumPerBatch;
    }

    public void setMaxOperationNumPerBatch(int maxOperationNumPerBatch) {
        this.maxOperationNumPerBatch = Math.max(1, maxOperationNumPerBatch);
    }

    public boolean isEnableLogEntryChecksum() {
        return enableLogEntryChecksum;
    }

    public void setEnableLogEntryChecksum(boolean enableLogEntryChecksum) {
        this.enableLogEntryChecksum = enableLogEntryChecksum;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public ExecutorGroup getLogManagerExecutorGroup() {
        return logManagerExecutorGroup;
    }

    public void setLogManagerExecutorGroup(ExecutorGroup logManagerExecutorGroup) {
        this.logManagerExecutorGroup = logManagerExecutorGroup;
    }

    public ExecutorGroup getFsmCallerExecutorGroup() {
        return fsmCallerExecutorGroup;
    }

    public void setFsmCallerExecutorGroup(ExecutorGroup fsmCallerExecutorGroup) {
        this.fsmCallerExecutorGroup = fsmCallerExecutorGroup;
    }

    public ExecutorGroup getSenderExecutorGroup() {
        return senderExecutorGroup;
    }

    public void setSenderExecutorGroup(ExecutorGroup senderExecutorGroup) {
        this.senderExecutorGroup = senderExecutorGroup;
    }

    public TimerFactory getTimerFactory() {
        return timerFactory;
    }

    public void setTimerFactory(TimerFactory timerFactory) {
        this.timerFactory = timerFactory;
    }

    public int getSnapshotLogIndexReserved() {
        return snapshotLogIndexReserved;
    }

    public void setSnapshotLogIndexReserved(int snapshotLogIndexReserved) {
        this.snapshotLogIndexReserved = snapshotLogIndexReserved;
    }

    public int getSnapshotLogIndexMargin() {
        return snapshotLogIndexMargin;
    }

    public void setSnapshotLogIndexMargin(int snapshotLogIndexMargin) {
        this.snapshotLogIndexMargin = snapshotLogIndexMargin;
    }

    public EndpointManager getEndpointFactory() {
        return endpointFactory;
    }

    public void setEndpointFactory(EndpointManager endpointFactory) {
        this.endpointFactory = endpointFactory;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public void setRpcClient(RpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    public LogEntryCodecFactory getLogEntryCodecFactory() {
        return logEntryCodecFactory;
    }

    public void setLogEntryCodecFactory(LogEntryCodecFactory logEntryCodecFactory) {
        this.logEntryCodecFactory = logEntryCodecFactory;
    }

    public int getHeartBeatFactor() {
        return heartBeatFactor;
    }

    public void setHeartBeatFactor(int heartBeatFactor) {
        this.heartBeatFactor = heartBeatFactor;
    }

    public FileServiceFactory getFileServiceFactory() {
        return fileServiceFactory;
    }

    public void setFileServiceFactory(FileServiceFactory fileServiceFactory) {
        this.fileServiceFactory = fileServiceFactory;
    }

    public Executor getDownloadSnapshotExecutor() {
        return downloadSnapshotExecutor;
    }

    public void setDownloadSnapshotExecutor(Executor downloadSnapshotExecutor) {
        this.downloadSnapshotExecutor = downloadSnapshotExecutor;
    }

    public int getDownloadSnapshotTimeoutMs() {
        return downloadSnapshotTimeoutMs;
    }

    public void setDownloadSnapshotTimeoutMs(int downloadSnapshotTimeoutMs) {
        this.downloadSnapshotTimeoutMs = downloadSnapshotTimeoutMs;
    }

    public ScheduledExecutorService getSenderScheduler() {
        return senderScheduler;
    }

    public void setSenderScheduler(ScheduledExecutorService senderScheduler) {
        this.senderScheduler = senderScheduler;
    }

    public int getRecoverIntervalMs() {
        return recoverIntervalMs;
    }

    public void setRecoverIntervalMs(int recoverIntervalMs) {
        this.recoverIntervalMs = recoverIntervalMs;
    }
}
