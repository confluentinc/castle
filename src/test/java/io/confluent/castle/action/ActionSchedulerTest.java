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
import io.confluent.castle.cluster.CastleClusterSpec;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.cluster.CastleNodeSpec;
import io.confluent.castle.common.CastleLog;
import io.confluent.castle.role.MockCloudRole;
import io.confluent.castle.role.Role;
import io.confluent.castle.tool.CastleEnvironment;
import io.confluent.castle.tool.MockCastleEnvironment;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ActionSchedulerTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    private CastleCluster createCluster(int numNodes) throws Exception {
        Map<String, CastleNodeSpec> map = new HashMap<>();
        CastleNodeSpec specA = new CastleNodeSpec(
            Arrays.asList(new String[] {"mockCloud"}), null);
        map.put(String.format("node[0-%d]", numNodes - 1), specA);
        Map<String, Role> roles = new HashMap<>();
        roles.put("mockCloud", new MockCloudRole());
        CastleClusterSpec spec = new CastleClusterSpec(null, map, roles);
        return new CastleCluster(new MockCastleEnvironment(),
            CastleLog.fromDevNull("cluster", false), null, spec);
    }

    @Test
    public void testCreateDestroy() throws Throwable {
        CastleCluster cluster = createCluster(2);
        ActionScheduler.Builder schedulerBuilder = new ActionScheduler.Builder(cluster);
        try (ActionScheduler scheduler = schedulerBuilder.build()) {
            scheduler.await(1000, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testInvalidTargetName() throws Throwable {
        CastleCluster cluster = createCluster(2);
        ActionScheduler.Builder schedulerBuilder = new ActionScheduler.Builder(cluster);
        schedulerBuilder.addTargetName("unknownTarget");
        try {
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(1, TimeUnit.DAYS);
            }
            fail("Expected to get an exception about an unknown target.");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Unknown target"));
        }
    }

    @Test
    public void testRunActions() throws Throwable {
        CastleCluster cluster = createCluster(3);
        final CyclicBarrier barrier = new CyclicBarrier(3);
        final AtomicInteger numRun = new AtomicInteger(0);
        ActionScheduler.Builder schedulerBuilder =
            new ActionScheduler.Builder(cluster);
        for (final String nodeName : cluster.nodes().keySet()) {
            schedulerBuilder.addAction(new Action(
                new ActionId("testAction", nodeName),
                    new TargetId[0],
                    new String[0],
                    0) {
                @Override
                public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                    ActionScheduler.log.info("WATERMELON: running testAction");
                    numRun.incrementAndGet();
                    barrier.await();
                }
            });
        }
        schedulerBuilder.addTargetName("testAction");
        try (ActionScheduler scheduler = schedulerBuilder.build()) {
            scheduler.await(1, TimeUnit.DAYS);
        }
        assertEquals(3, numRun.get());
    }

    @Test
    public void testContainedDependencies() throws Throwable {
        CastleCluster cluster = createCluster(3);
        Map<String, AtomicInteger> vals = Collections.synchronizedMap(new HashMap<>());
        for (final String nodeName : cluster.nodes().keySet()) {
            vals.put(nodeName, new AtomicInteger(0));
        }
        vals.put("node0", new AtomicInteger(-1));
        ActionScheduler.Builder schedulerBuilder =
            new ActionScheduler.Builder(cluster);
        schedulerBuilder.addAction(new Action(
                new ActionId("foo", "node0"),
                new TargetId[0],
                new String[0],
                0) {
            @Override
            public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                AtomicInteger val = vals.get(node.nodeName());
                assertTrue(val.compareAndSet(-1, 0));
            }
        });
        for (final String nodeName : cluster.nodes().keySet()) {
            schedulerBuilder.addAction(new Action(
                new ActionId("bar", nodeName),
                new TargetId[] {
                    new TargetId("foo")
                },
                new String[] {
                    "baz",
                    "quux"
                },
                0) {
                @Override
                public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                    AtomicInteger val = vals.get(node.nodeName());
                    assertTrue(val.compareAndSet(0, 1));
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("baz", nodeName),
                new TargetId[] {},
                new String[] {},
                0) {
                @Override
                public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                    AtomicInteger val = vals.get(node.nodeName());
                    assertTrue(val.compareAndSet(1, 2));
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("quux", nodeName),
                new TargetId[] {
                    new TargetId("baz", nodeName)
                },
                new String[] {},
                0) {
                @Override
                public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                    AtomicInteger val = vals.get(node.nodeName());
                    assertTrue(val.compareAndSet(2, 3));
                }
            });
        }
        schedulerBuilder.addTargetName("foo");
        schedulerBuilder.addTargetName("bar");
        try (ActionScheduler scheduler = schedulerBuilder.build()) {
            scheduler.await(1000, TimeUnit.MILLISECONDS);
        }
        for (final String nodeName : cluster.nodes().keySet()) {
            assertEquals(3, vals.get(nodeName).get());
        }
    }

    @Test
    public void testAllDependency() throws Throwable {
        CastleCluster cluster = createCluster(3);
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger numBars = new AtomicInteger(0);
        ActionScheduler.Builder schedulerBuilder =
            new ActionScheduler.Builder(cluster);
        schedulerBuilder.addAction(new Action(
            new ActionId("baz", "node1"),
            new TargetId[0],
            new String[0],
            0) {
            @Override
            public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                count.incrementAndGet();
            }
        });
        for (final String nodeName : cluster.nodes().keySet()) {
            schedulerBuilder.addAction(new Action(
                new ActionId("foo", nodeName),
                new TargetId[0],
                new String[] {
                    "bar",
                    "quux"
                },
                0) {
                @Override
                public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                    count.incrementAndGet();
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("quux", nodeName),
                new TargetId[0],
                new String[] {},
                0) {
                @Override
                public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                    count.incrementAndGet();
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("bar", nodeName),
                new TargetId[]{
                    new TargetId("quux"),
                    new TargetId("baz", "node1")
                },
                new String[0],
                0) {
                @Override
                public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                    assertEquals(7, count.get());
                    numBars.incrementAndGet();
                }
            });
        }
        schedulerBuilder.addTargetName("foo");
        schedulerBuilder.addTargetName("baz:node1");
        try (ActionScheduler scheduler = schedulerBuilder.build()) {
            scheduler.await(1000, TimeUnit.MILLISECONDS);
        }
        assertEquals(3, numBars.get());
    }

    private static class ConcurrentAccessChecker {
        private final int maxConcurrentActions;
        private final AtomicInteger currentlyRunning = new AtomicInteger(0);
        private final AtomicInteger totalCalls = new AtomicInteger(0);

        ConcurrentAccessChecker(int maxConcurrentActions) {
            this.maxConcurrentActions = maxConcurrentActions;
        }

        void check() throws Exception {
            if (currentlyRunning.getAndIncrement() > maxConcurrentActions) {
                throw new RuntimeException("expected only " + maxConcurrentActions +
                    " task(s) running at once.");
            }
            Thread.sleep(1);
            currentlyRunning.decrementAndGet();
            totalCalls.incrementAndGet();
        }

        int totalCalls() {
            return totalCalls.get();
        }
    }

    @Test
    public void testMaxConcurrentActions() throws Throwable {
        CastleCluster cluster = createCluster(2);
        ActionScheduler.Builder schedulerBuilder =
            new ActionScheduler.Builder(cluster);
        ConcurrentAccessChecker concurrentAccessChecker = new ConcurrentAccessChecker(1);
        schedulerBuilder.addAction(new Action(
            new ActionId("foo", "node0"),
            new TargetId[0],
            new String[] { "bar", "quux", "baz" },
            0) {
            @Override
            public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                concurrentAccessChecker.check();
            }
        });
        schedulerBuilder.addAction(new Action(
            new ActionId("foo", "node1"),
            new TargetId[0],
            new String[] { "bar", "quux", "baz" },
            0) {
            @Override
            public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                concurrentAccessChecker.check();
            }
        });
        schedulerBuilder.addAction(new Action(
            new ActionId("bar", "node1"),
            new TargetId[] { new TargetId("node1") },
            new String[0],
            0) {
            @Override
            public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                concurrentAccessChecker.check();
            }
        });
        schedulerBuilder.addAction(new Action(
            new ActionId("quux", "node0"),
            new TargetId[0],
            new String[0],
            0) {
            @Override
            public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                concurrentAccessChecker.check();
            }
        });
        schedulerBuilder.addAction(new Action(
            new ActionId("baz", "node1"),
            new TargetId[] { new TargetId("bar", "node1") },
            new String[0],
            0) {
            @Override
            public void call(CastleCluster cluster, CastleNode node) throws Throwable {
                concurrentAccessChecker.check();
            }
        });
        schedulerBuilder.addTargetName("foo");
        try (ActionScheduler scheduler = schedulerBuilder.build()) {
            scheduler.await(1000, TimeUnit.MILLISECONDS);
        }
        assertEquals(5, concurrentAccessChecker.totalCalls());
    }
};
