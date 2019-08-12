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
import io.confluent.castle.action.SchemaRegistryStartAction;
import io.confluent.castle.action.SchemaRegistryStatusAction;
import io.confluent.castle.action.SchemaRegistryStopAction;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.DynamicVariableProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryRole implements Role {
    public static final String SCHEMA_REGISTRY_CLASS_NAME =
        "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain";

    private static final String DEFAULT_JVM_PERFORMANCE_OPTS = "-Xmx3g -Xms3g";

    public static final int HTTP_PORT = 8081;

    private final int initialDelayMs;

    private final Map<String, String> conf;

    private final String jvmOptions;

    @JsonCreator
    public SchemaRegistryRole(@JsonProperty("initialDelayMs") int initialDelayMs,
                              @JsonProperty("conf") Map<String, String> conf,
                              @JsonProperty("jvmOptions") String jvmOptions) {
        this.initialDelayMs = initialDelayMs;
        this.conf = conf == null ? Collections.emptyMap() :
            Collections.unmodifiableMap(new HashMap<>(conf));
        if ((jvmOptions == null) || jvmOptions.isEmpty()) {
            this.jvmOptions = DEFAULT_JVM_PERFORMANCE_OPTS;
        } else {
            this.jvmOptions = jvmOptions;
        }
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

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new SchemaRegistryStartAction(nodeName, this));
        actions.add(new SchemaRegistryStatusAction(nodeName, this));
        actions.add(new SchemaRegistryStopAction(nodeName, this));
        return actions;
    }

    @Override
    public Map<String, DynamicVariableProvider> dynamicVariableProviders() {
        return Collections.singletonMap("schema.registry.url", new DynamicVariableProvider(0) {
            @Override
            public String calculate(CastleCluster cluster, CastleNode node) throws Exception {
                StringBuilder bld = new StringBuilder();
                String prefix = "";
                for (String nodeName : cluster.nodesWithRole(SchemaRegistryRole.class).values()) {
                    bld.append(prefix);
                    prefix = ",";
                    CastleNode brokerNode = cluster.nodes().get(nodeName);
                    bld.append(String.format("http://%s:%d", brokerNode.uplink().internalDns(), HTTP_PORT));
                }
                return bld.toString();
            }
        });
    }
};
