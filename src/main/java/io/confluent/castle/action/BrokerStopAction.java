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

package io.confluent.castle.action;

import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.role.BrokerRole;

/**
 * Stop the broker.
 */
public final class BrokerStopAction extends Action {
    public final static String TYPE = "brokerStop";

    public BrokerStopAction(String scope, BrokerRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(JmxDumperStopAction.TYPE, scope),
                new TargetId(SchemaRegistryStopAction.TYPE, scope)
            },
            new String[] {},
            0);
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        if (!node.uplink().canLogin()) {
            node.log().printf("*** Skipping %s, because the node is not running.%n", TYPE);
            return;
        }
        CastleUtil.killJavaProcess(cluster, node, BrokerRole.KAFKA_CLASS_NAME, true);
    }
}
