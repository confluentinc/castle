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

/**
 * Rsyncs the Kafka source directory to the cluster node.
 */
public final class SourceSetupAction extends Action {
    public final static String TYPE = "sourceSetup";

    public SourceSetupAction(String scope) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(LinuxSetupAction.TYPE, scope)
            },
            new String[] {},
            0);
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        if (!node.uplink().canLogin()) {
            node.log().printf("*** Skipping %s, because the node is not accessible.%n", TYPE);
            return;
        }
        cluster.conf().validateKafkaPath();
        cluster.conf().validateSchemaRegistryPath();
        cluster.conf().validateCastlePath();
        node.uplink().command().args(setupDirectoriesCommand()).mustRun();
        node.uplink().command().
            syncTo(cluster.conf().kafkaPath() + "/", ActionPaths.KAFKA_SRC + "/").
            mustRun();
        if (!cluster.conf().schemaRegistryPath().isEmpty()) {
            node.uplink().command().
                    syncTo(cluster.conf().schemaRegistryPath() + "/", ActionPaths.SCHEMA_REGISTRY_SRC + "/").
                    mustRun();
        }
        node.uplink().command().
            syncTo(cluster.conf().castlePath() + "/", ActionPaths.CASTLE_SRC + "/").
            mustRun();
    }

    public static String[] setupDirectoriesCommand() {
        return new String[] {"sudo", "mkdir", "-p",
            ActionPaths.KAFKA_SRC, ActionPaths.SCHEMA_REGISTRY_SRC, ActionPaths.CASTLE_SRC, ActionPaths.LOGS_ROOT,
            "&&", "sudo", "chown", "-R", "`whoami`",
            ActionPaths.KAFKA_SRC, ActionPaths.SCHEMA_REGISTRY_SRC, ActionPaths.CASTLE_SRC, ActionPaths.LOGS_ROOT};
    }
}
