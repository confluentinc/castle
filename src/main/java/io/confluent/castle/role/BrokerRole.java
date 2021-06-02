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

package io.confluent.castle.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.castle.action.Action;
import io.confluent.castle.action.BrokerStartAction;
import io.confluent.castle.action.BrokerStatusAction;
import io.confluent.castle.action.BrokerStopAction;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.DynamicVariableProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BrokerRole implements Role {
    // Handle both kafka.Kafka and SupportedKafka
    public static final String KAFKA_CLASS_NAME = "kafka.Kafka";

    private static final String DEFAULT_JVM_PERFORMANCE_OPTS = "-Xmx3g -Xms3g";

    private static final String DEFAULT_EXTERNAL_AUTH = "PLAINTEXT";

    public static final int PORT = 9092;

    private final int initialDelayMs;

    private final Map<String, String> conf;

    private final String jvmOptions;

    private final String externalAuth;

    @JsonCreator
    public BrokerRole(@JsonProperty("initialDelayMs") int initialDelayMs,
                      @JsonProperty("conf") Map<String, String> conf,
                      @JsonProperty("jvmOptions") String jvmOptions,
                      @JsonProperty("externalAuth") String externalAuth) {
        this.initialDelayMs = initialDelayMs;
        this.conf = conf == null ? Collections.emptyMap() :
            Collections.unmodifiableMap(new HashMap<>(conf));
        if ((jvmOptions == null) || jvmOptions.isEmpty()) {
            this.jvmOptions = DEFAULT_JVM_PERFORMANCE_OPTS;
        } else {
            this.jvmOptions = jvmOptions;
        }
        this.externalAuth = externalAuth == null ?
            DEFAULT_EXTERNAL_AUTH : externalAuth;
    }

    @JsonProperty
    public int initialDelayMs() {
        return initialDelayMs;
    }

    @JsonProperty
    public Map<String, String> conf() {
        return conf;
    }

    @JsonProperty
    public String jvmOptions() {
        return jvmOptions;
    }

    @JsonProperty
    public String externalAuth() {
        return externalAuth;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new BrokerStartAction(nodeName, this));
        actions.add(new BrokerStatusAction(nodeName, this));
        actions.add(new BrokerStopAction(nodeName, this));
        return actions;
    }

    @Override
    public Map<String, DynamicVariableProvider> dynamicVariableProviders() {
        return Collections.singletonMap("bootstrapServers", new DynamicVariableProvider(0) {
            @Override
            public String calculate(CastleCluster cluster, CastleNode node) throws Exception {
                StringBuilder bld = new StringBuilder();
                String prefix = "";
                for (String nodeName : cluster.nodesWithRole(BrokerRole.class).values()) {
                    bld.append(prefix);
                    prefix = ",";
                    CastleNode brokerNode = cluster.nodes().get(nodeName);
                    bld.append(String.format("%s:%d", brokerNode.uplink().internalDns(), PORT));
                }
                return bld.toString();
            }
        });
    }
};
