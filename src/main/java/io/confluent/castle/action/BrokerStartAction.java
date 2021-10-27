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
import io.confluent.castle.role.BrokerRole;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import static io.confluent.castle.action.ActionPaths.KAFKA_CONF;
import static io.confluent.castle.action.ActionPaths.KAFKA_OPLOGS;
import static io.confluent.castle.action.ActionPaths.KAFKA_ROOT;
import static io.confluent.castle.role.BrokerRole.KAFKA_CLASS_NAME;
import static io.confluent.castle.action.ActionPaths.KAFKA_LOGS;

/**
 * Starts the Kafka broker.
 */
public final class BrokerStartAction extends Action {
    public final static String TYPE = "brokerStart";

    private final BrokerRole role;

    public BrokerStartAction(String scope, BrokerRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[]{
                new TargetId(ZooKeeperStartAction.TYPE)
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
            configFile = writeBrokerConfig(expander, cluster, node);
            log4jFile = writeBrokerLog4j(cluster, node);
            CastleUtil.killJavaProcess(cluster, node, KAFKA_CLASS_NAME, true);
            node.uplink().command().args(createSetupPathsCommandLine()).mustRun();
            node.uplink().command().syncTo(configFile.getAbsolutePath(),
                ActionPaths.KAFKA_BROKER_PROPERTIES).mustRun();
            node.uplink().command().syncTo(log4jFile.getAbsolutePath(),
                ActionPaths.KAFKA_BROKER_LOG4J).mustRun();
            node.uplink().command().args(createRunDaemonCommandLine()).mustRun();
        } finally {
            CastleUtil.deleteFileOrLog(node.log(), configFile);
            CastleUtil.deleteFileOrLog(node.log(), log4jFile);
        }
        CastleUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == node.uplink().command().args(
                    CastleUtil.checkJavaProcessStatusArgs(KAFKA_CLASS_NAME)).run();
            }
        });
    }

    public static String[] createSetupPathsCommandLine() {
        return new String[] {"-n", "--",
            "sudo", "rm", "-rf", KAFKA_OPLOGS, KAFKA_LOGS, KAFKA_CONF, "&&",
            "sudo", "mkdir", "-p", KAFKA_OPLOGS, KAFKA_LOGS, KAFKA_CONF, "&&",
            "sudo", "chown", "`whoami`", KAFKA_ROOT, KAFKA_OPLOGS, KAFKA_LOGS, KAFKA_CONF};
    }

    public String[] createRunDaemonCommandLine() {
        return new String[]{"-n", "--", "nohup", "env",
            "JMX_PORT=9192",
            "KAFKA_JVM_PERFORMANCE_OPTS='" + role.jvmOptions() + "'",
            "KAFKA_LOG4J_OPTS='-Dlog4j.configuration=file:" + ActionPaths.KAFKA_BROKER_LOG4J + "' ",
            "LOG_DIR=\"" + KAFKA_LOGS + "\"",
            ActionPaths.KAFKA_START_SCRIPT, ActionPaths.KAFKA_BROKER_PROPERTIES,
            ">" + ActionPaths.KAFKA_LOGS + "/stdout-stderr.txt", "2>&1", "</dev/null", "&"
        };
    }

    private Map<String, String> getDefaultConf() {
        Map<String, String> defaultConf;
        defaultConf = new HashMap<>();
        defaultConf.put("num.network.threads", "3");
        defaultConf.put("num.io.threads", "8");
        defaultConf.put("socket.send.buffer.bytes", "102400");
        defaultConf.put("socket.receive.buffer.bytes", "102400");
        defaultConf.put("socket.receive.buffer.bytes", "102400");
        defaultConf.put("socket.request.max.bytes", "104857600");
        defaultConf.put("num.partitions", "3");
        defaultConf.put("num.recovery.threads.per.data.dir", "1");
        defaultConf.put("log.retention.bytes", "104857600");
        defaultConf.put("zookeeper.connection.timeout.ms", "6000");
        return defaultConf;
    }

    private File writeBrokerConfig(DynamicVariableExpander expander,
                                   CastleCluster cluster,
                                   CastleNode node) throws Exception {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        Map<String, String> effectiveConf =
            expander.expand(CastleUtil.mergeConfig(role.conf(), getDefaultConf()));
        try {
            file = new File(cluster.env().workingDirectory(), String.format("broker-%d.properties",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("broker.id=%d%n", getBrokerId(cluster, node)));
            osw.write(String.format("listeners=%s://:%d%n", role.externalAuth(), BrokerRole.PORT));
            osw.write(String.format("advertised.listeners=%s://:%d%n", role.externalAuth(), BrokerRole.PORT));
            osw.write(String.format("inter.broker.listener.name=%s%n", role.externalAuth()));
            osw.write(String.format("log.dirs=%s%n", KAFKA_OPLOGS));
            osw.write(String.format("zookeeper.connect=%s%n", cluster.getZooKeeperConnectString()));
            for (Map.Entry<String, String> entry : effectiveConf.entrySet()) {
                osw.write(String.format("%s=%s%n", entry.getKey(), entry.getValue()));
            }
            success = true;
            return file;
        } finally {
            CastleUtil.closeQuietly(cluster.clusterLog(),
                osw, "temporary broker file OutputStreamWriter");
            CastleUtil.closeQuietly(cluster.clusterLog(),
                fos, "temporary broker file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    File writeBrokerLog4j(CastleCluster cluster,  CastleNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().workingDirectory(), String.format("broker-log4j-%d.properties",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("log4j.rootLogger=INFO, kafkaAppender%n"));
            osw.write(String.format("%n"));
            writeDailyRollingFileAppender(osw, "kafkaAppender", "server.log");
            writeDailyRollingFileAppender(osw, "stateChangeAppender", "state-change.log");
            writeDailyRollingFileAppender(osw, "requestAppender", "kafka-request.log");
            writeDailyRollingFileAppender(osw, "cleanerAppender", "log-cleaner.log");
            writeDailyRollingFileAppender(osw, "controllerAppender", "controller.log");
            writeDailyRollingFileAppender(osw, "authorizerAppender", "kafka-authorizer.log");
            osw.write(String.format("log4j.logger.org.I0Itec.zkclient.ZkClient=INFO%n"));
            osw.write(String.format("log4j.logger.org.apache.zookeeper=INFO%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka=INFO%n"));
            osw.write(String.format("log4j.logger.org.apache.kafka=INFO%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka.request.logger=WARN, requestAppender%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka.controller=TRACE, controllerAppender%n"));
            osw.write(String.format("log4j.additivity.kafka.controller=false%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka.log.LogCleaner=INFO, cleanerAppender%n"));
            osw.write(String.format("log4j.additivity.kafka.log.LogCleaner=false%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.state.change.logger=TRACE, stateChangeAppender%n"));
            osw.write(String.format("log4j.additivity.state.change.logger=false%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka.authorizer.logger=INFO, authorizerAppender%n"));
            osw.write(String.format("log4j.additivity.kafka.authorizer.logger=false%n"));
            success = true;
            return file;
        } finally {
            CastleUtil.closeQuietly(cluster.clusterLog(),
                osw, "temporary broker log4j file OutputStreamWriter");
            CastleUtil.closeQuietly(cluster.clusterLog(),
                fos, "temporary broker log4j file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    static void writeDailyRollingFileAppender(OutputStreamWriter osw, String appender,
                                              String logName) throws IOException {
        osw.write(String.format("log4j.appender.%s=org.apache.log4j.DailyRollingFileAppender%n", appender));
        osw.write(String.format("log4j.appender.%s.DatePattern='.'yyyy-MM-dd-HH%n", appender));
        osw.write(String.format("log4j.appender.%s.File=%s/%s%n", appender, KAFKA_LOGS, logName));
        osw.write(String.format("log4j.appender.%s.layout=org.apache.log4j.PatternLayout%n", appender));
        osw.write(String.format("log4j.appender.%s.layout.ConversionPattern=%s%n",
            appender, "[%d] %p %m (%c)%n"));
    }

    private int getBrokerId(CastleCluster cluster, CastleNode node) {
        for (Map.Entry<Integer, String> entry :
                cluster.nodesWithRole(BrokerRole.class).entrySet()) {
            if (entry.getValue().equals(node.nodeName())) {
                return entry.getKey();
            }
        }
        throw new RuntimeException("Node " + node.nodeName() + " does not have the broker role.");
    }

}
