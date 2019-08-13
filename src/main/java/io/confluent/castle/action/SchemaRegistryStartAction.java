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
import io.confluent.castle.common.DynamicVariableExpander;
import io.confluent.castle.role.Schema;
import io.confluent.castle.role.SchemaRegistryRole;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import static io.confluent.castle.action.ActionPaths.SCHEMA_REGISTRY_CONF;
import static io.confluent.castle.action.ActionPaths.SCHEMA_REGISTRY_LOGS;
import static io.confluent.castle.action.ActionPaths.SCHEMA_REGISTRY_ROOT;

public class SchemaRegistryStartAction extends Action  {
    public final static String TYPE = "schemaRegistryStart";

    private final SchemaRegistryRole role;

    public SchemaRegistryStartAction(String scope, SchemaRegistryRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[]{
                new TargetId(BrokerStartAction.TYPE)
            },
            new String[] {},
            role.initialDelayMs());
        this.role = Objects.requireNonNull(role);
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {
        File configFile = null, log4jFile = null;
        try {
            DynamicVariableExpander expander = new DynamicVariableExpander(cluster, node);
            configFile = writeSchemaRegistryConfig(expander, cluster, node);
            log4jFile = writeSchemaRegistryLog4j(cluster, node);
            CastleUtil.killJavaProcess(cluster, node, SchemaRegistryRole.SCHEMA_REGISTRY_CLASS_NAME, false);
            node.uplink().command().args(createSetupPathsCommandLine()).mustRun();
            node.uplink().command().syncTo(configFile.getAbsolutePath(),
                ActionPaths.SCHEMA_REGISTRY_PROPERTIES).mustRun();
            node.uplink().command().syncTo(log4jFile.getAbsolutePath(),
                ActionPaths.SCHEMA_REGISTRY_LOG4J).mustRun();
            writeSchemas(cluster, node);
            node.uplink().command().args(createRunDaemonCommandLine()).mustRun();
        } finally {
            CastleUtil.deleteFileOrLog(node.log(), configFile);
            CastleUtil.deleteFileOrLog(node.log(), log4jFile);
        }
        CastleUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == node.uplink().command().args(
                    CastleUtil.checkJavaProcessStatusArgs(SchemaRegistryRole.SCHEMA_REGISTRY_CLASS_NAME)).run();
            }
        });
        CastleUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == node.uplink().command().args(
                    checkRestServiceArgs()).run();
            }
        });
        node.uplink().command().args(createSchemas()).mustRun();
    }

    public static String[] createSetupPathsCommandLine() {
        return new String[]{"-n", "--",
            "sudo", "rm", "-rf", SCHEMA_REGISTRY_LOGS, SCHEMA_REGISTRY_CONF, "&&",
            "sudo", "mkdir", "-p", SCHEMA_REGISTRY_LOGS, SCHEMA_REGISTRY_CONF, "&&",
            "sudo", "chown", "`whoami`", SCHEMA_REGISTRY_ROOT, SCHEMA_REGISTRY_LOGS, SCHEMA_REGISTRY_CONF
        };
    }

    public static String[] createRunDaemonCommandLine() {
        return new String[] {"nohup", "env",
            "JMX_PORT=9999",
            "LOG_DIR=\"" + SCHEMA_REGISTRY_LOGS + "\"",
            "KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:" + ActionPaths.SCHEMA_REGISTRY_LOG4J + "\"",
            ActionPaths.SCHEMA_REGISTRY_START_SCRIPT, ActionPaths.SCHEMA_REGISTRY_PROPERTIES,
            ">" + SCHEMA_REGISTRY_LOGS + "/stdout-stderr.txt", "2>&1", "</dev/null", "&"};
    }

    private Map<String, String> getDefaultConf() {
        Map<String, String> defaultConf;
        defaultConf = new HashMap<>();
        defaultConf.put("listeners", "http://0.0.0.0:8081");
        defaultConf.put("kafkastore.topic", "_schemas");
        defaultConf.put("debug", "true");
        return defaultConf;
    }

    private File writeSchemaRegistryConfig(DynamicVariableExpander expander,
                                           CastleCluster cluster,
                                           CastleNode node) throws Exception {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        Map<String, String> effectiveConf =
                expander.expand(CastleUtil.mergeConfig(role.conf(), getDefaultConf()));
        try {
            file = new File(cluster.env().workingDirectory(),
                String.format("schema-registry-%d.properties", node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("kafkastore.connection.url=%s%n", cluster.getZooKeeperConnectString()));
            for (Map.Entry<String, String> entry : effectiveConf.entrySet()) {
                osw.write(String.format("%s=%s%n", entry.getKey(), entry.getValue()));
            }
            success = true;
            return file;
        } finally {
            CastleUtil.closeQuietly(cluster.clusterLog(),
                osw, "temporary SchemaRegistry config file OutputStreamWriter");
            CastleUtil.closeQuietly(cluster.clusterLog(),
                fos, "temporary SchemaRegistry config file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    static File writeSchemaRegistryLog4j(CastleCluster cluster, CastleNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().workingDirectory(),
                String.format("schema-registry-log4j-%d.properties", node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("log4j.rootLogger=INFO, kafkaAppender%n"));
            osw.write(String.format("log4j.appender.kafkaAppender=org.apache.log4j.DailyRollingFileAppender%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.DatePattern='.'yyyy-MM-dd-HH%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.File=%s/server.log%n", SCHEMA_REGISTRY_LOGS));
            osw.write(String.format("log4j.appender.kafkaAppender.layout=org.apache.log4j.PatternLayout%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.layout.ConversionPattern=%s%n%n",
                "[%d] %p %m (%c)%n"));
            osw.write(String.format("log4j.logger.kafka=ERROR%n"));
            osw.write(String.format("log4j.logger.org.apache.kafka=ERROR%n"));
            osw.write(String.format("log4j.logger.org.apache.zookeeper=ERROR%n"));
            osw.write(String.format("log4j.logger.org.I0Itec.zkclient.ZkClient=INFO%n"));
            osw.write(String.format("log4j.additivity.kafka.server=false%n"));
            osw.write(String.format("log4j.additivity.kafka.consumer.ZookeeperConsumerConnector=false%n"));
            success = true;
            return file;
        } finally {
            CastleUtil.closeQuietly(cluster.clusterLog(),
                osw, "temporary Schema Registry file OutputStreamWriter");
            CastleUtil.closeQuietly(cluster.clusterLog(),
                fos, "temporary Schema Registry file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    private void writeSchemas(final CastleCluster cluster, final CastleNode node) throws Throwable {
        for (int i = 0; i < role.schemas().size(); i++) {
            File schemaFile = writeSchema(cluster, node, i);
            node.uplink().command().syncTo(schemaFile.getAbsolutePath(),
                String.format(ActionPaths.SCHEMA_REGISTRY_SCHEMA, i)).mustRun();
        }
    }

    private File writeSchema(CastleCluster cluster, CastleNode node, int index) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            Schema schema = role.schemas().get(index);
            file = new File(cluster.env().workingDirectory(),
                    String.format("schema-%d.asc", index));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write("{ \"schema\": \"");
            osw.write(schema.schema().replace("\"", "\\\""));
            osw.write("\" }");
            success = true;
            return file;
        } finally {
            CastleUtil.closeQuietly(cluster.clusterLog(),
                osw, "temporary Schema Registry file OutputStreamWriter");
            CastleUtil.closeQuietly(cluster.clusterLog(),
                fos, "temporary Schema Registry file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    private static String[] checkRestServiceArgs() {
        return new String[] {"-n", "--", "curl", "http://localhost:8081/config"};
    }

    private String[] createSchemas() {
        String separator = "";
        List<String> args = new ArrayList<>();
        args.add("-n");
        args.add("--");
        for (int i = 0; i < role.schemas().size(); i++) {
            Schema schema = role.schemas().get(i);
            if (!separator.isEmpty()) {
                args.add(separator);
            } else {
                separator = "&&";
            }
            args.add("curl");
            args.add("-X");
            args.add("POST");
            args.add("-H");
            args.add("Content-Type:application/vnd.schemaregistry.v1+json");
            args.add("--data");
            args.add("@" + String.format(ActionPaths.SCHEMA_REGISTRY_SCHEMA, i));
            args.add("http://localhost:8081/subjects/" + schema.subject() + "/versions");
        }
        return args.toArray(new String[0]);
    }
};
