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

import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.LogManager;
import com.trs.pacifica.LogStorage;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;

import java.util.List;

public class LogManagerImpl implements LogManager, LifeCycle<LogManagerImpl.Option> {


    private Option option;

    private LogStorage logStorage;

    public LogManagerImpl() {
    }

    @Override
    public void init(LogManagerImpl.Option option) {
        this.option = option;

    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void appendLogEntries(List<LogEntry> logEntries, Callback callback) {

    }

    @Override
    public LogId getCommitPoint() {
        return null;
    }



    public static final class Option {

        ReplicaOption replicaOption;



        public ReplicaOption getReplicaOption() {
            return replicaOption;
        }

        public void setReplicaOption(ReplicaOption replicaOption) {
            this.replicaOption = replicaOption;
        }
    }
}
