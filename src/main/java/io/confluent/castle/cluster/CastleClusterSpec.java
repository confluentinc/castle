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
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.common.JsonMerger;
import io.confluent.castle.common.RangeExpressionExpander;
import io.confluent.castle.common.StringExpander;
import io.confluent.castle.role.Role;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static io.confluent.castle.common.JsonUtil.JSON_SERDE;

public class CastleClusterSpec {
    private final CastleClusterConf conf;
    private final Map<String, CastleNodeSpec> nodes;
    private final Map<String, Role> roles;

    @JsonCreator
    public CastleClusterSpec(@JsonProperty("conf") CastleClusterConf conf,
                             @JsonProperty("nodes") Map<String, CastleNodeSpec> nodes,
                             @JsonProperty("roles") Map<String, Role> roles) throws Exception {
        this.conf = (conf == null) ?
            new CastleClusterConf(null, null, null, 0) : conf;
        if (nodes == null) {
            this.nodes = Collections.emptyMap();
        } else {
            Map<String, CastleNodeSpec> newNodes = new HashMap<>();
            for (Map.Entry<String, CastleNodeSpec> entry: nodes.entrySet()) {
                for (String nodeName : RangeExpressionExpander.expand(entry.getKey())) {
                    CastleNodeSpec nodeCopy = JSON_SERDE.readValue(
                        JSON_SERDE.writeValueAsBytes(entry.getValue()),
                        CastleNodeSpec.class);
                    newNodes.put(nodeName, nodeCopy);
                }
            }
            this.nodes = Collections.unmodifiableMap(newNodes);
        }
        this.roles = Collections.unmodifiableMap(
            (roles == null) ? new HashMap<>() : new HashMap<>(roles));
    }

    @JsonProperty
    public CastleClusterConf conf() {
        return conf;
    }

    @JsonProperty
    public Map<String, CastleNodeSpec> nodes() {
        return nodes;
    }

    @JsonProperty
    public Map<String, Role> roles() {
        return roles;
    }

    /**
     * Return the roles for each node in the cluster.
     *
     * Each node will get a separate copy of each role object.
     */
    public Map<String, Map<Class<? extends Role>, Role>> nodesToRoles() throws Exception {
        Map<String, Map<Class<? extends Role>, Role>> nodesToRoles = new TreeMap<>();
        for (Map.Entry<String, CastleNodeSpec> entry : nodes.entrySet()) {
            String nodeName = entry.getKey();
            CastleNodeSpec node = entry.getValue();
            Map<Class<? extends Role>, Role> roleMap = new HashMap<>();
            nodesToRoles.put(nodeName, roleMap);
            for (String roleName : node.roleNames()) {
                Role role = roles.get(roleName);
                if (role == null) {
                    throw new RuntimeException("For node " + nodeName +
                        ", no role named " + roleName + " found.  Role names are " +
                        CastleUtil.join(node.roleNames(), ", "));
                }
                JsonNode originalRole = JSON_SERDE.valueToTree(role);
                JsonNode deltaRole = node.rolePatches().get(roleName);
                JsonNode nodeRole = JsonMerger.merge(originalRole, deltaRole);
                roleMap.put(role.getClass(),
                    JSON_SERDE.treeToValue(nodeRole, Role.class));
            }
        }
        return nodesToRoles;
    }
}
