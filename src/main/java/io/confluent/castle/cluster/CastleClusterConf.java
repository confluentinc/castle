/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.castle.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;
import java.nio.file.Paths;

public class CastleClusterConf {
    private final static int DEFAULT_GLOBAL_TIMEOUT = 3600;

    private final String kafkaPath;
    private final String castlePath;
    private final int globalTimeout;

    @JsonCreator
    public CastleClusterConf(@JsonProperty("kafkaPath") String kafkaPath,
                             @JsonProperty("castlePath") String castlePath,
                             @JsonProperty("globalTimeout") int globalTimeout) {
        this.kafkaPath = (kafkaPath == null) ? "" : kafkaPath;
        this.castlePath = (castlePath == null) ? "" : castlePath;
        this.globalTimeout = (globalTimeout <= 0) ? DEFAULT_GLOBAL_TIMEOUT : globalTimeout;
    }

    @JsonProperty
    public String kafkaPath() {
        return kafkaPath;
    }

    public void validateKafkaPath() {
        if (kafkaPath.isEmpty() ||
                !(new File(kafkaPath).isDirectory())) {
            throw new RuntimeException("The current value of kafkaPath (" +
                kafkaPath + ") does not point to a valid directory.");
        }
    }

    @JsonProperty
    public String castlePath() {
        return castlePath;
    }

    @JsonProperty
    public int globalTimeout() {
        return globalTimeout;
    }
}
