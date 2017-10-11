/*-
 * -\-\-
 * async-google-pubsub-client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

/*
 * Copyright (c) 2011-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.google.cloud.pubsub.client.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.primitives.Ints;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static com.google.common.base.Optional.fromNullable;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.System.out;
import static java.util.Collections.unmodifiableMap;

/**
 * A simple progress meter. Prints average throughput and latencies for a set of metrics created
 * with {@link #group(String)} and {@link MetricGroup#metric(Object, String)}.
 */
class ProgressMeter {

  private static final int AVERAGE_WINDOW = 10;
  public static final int NANOS_PER_S = 1000000000;
  public static final double NANOS_PER_MS = 1000000.d;
  private final long interval = 1000;
  private final Thread printer;

  private volatile boolean run = true;
  private volatile Map<String, MetricGroup> groups = Maps.newLinkedHashMap();

  /**
   * Create a new progress meter.
   */
  public ProgressMeter() {
    printer = new ProgressPrinter();
  }

  /**
   * Stop the progress meter.
   */
  public void stop() {
    run = false;
    printer.interrupt();
  }

  public MetricGroup group(final String name) {
    MetricGroup group = groups.get(name);
    if (group == null) {
      synchronized (this) {
        group = groups.get(name);
        if (group == null) {
          final Map<String, MetricGroup> newGroups = Maps.newLinkedHashMap(groups);
          group = new MetricGroup();
          newGroups.put(name, group);
          groups = unmodifiableMap(newGroups);
        }
      }
    }
    return group;
  }

  private static class Delta {

    private final long ops;
    private final long interval;
    private final long latency;

    Delta(final long ops, final long interval, final long latency) {
      this.ops = ops;
      this.interval = interval;
      this.latency = latency;
    }
  }

  public class Metric implements Comparable<Metric> {

    private final Object[] name;
    private final String unit;
    private final LongAdder totalLatency = new LongAdder();
    private final LongAdder totalOperations = new LongAdder();
    private final Deque<Delta> deltas = Queues.newArrayDeque();
    private long prevTotalOperations = 0;
    private long prevTimestamp = System.nanoTime();
    private long prevTotalLatency = 0;

    private Metric(final Object[] name, final String unit) {
      this.name = name;
      this.unit = unit;
    }

    private void print(final Table table) {
      final long now = System.nanoTime();
      final long totalOperations = this.totalOperations.longValue();
      final long totalLatency = this.totalLatency.longValue();

      final long deltaOps = totalOperations - prevTotalOperations;
      final long deltaTime = now - prevTimestamp;
      final long deltaLatency = totalLatency - prevTotalLatency;

      deltas.add(new Delta(deltaOps, deltaTime, deltaLatency));

      if (deltas.size() > AVERAGE_WINDOW) {
        deltas.pop();
      }

      long windowOps = 0;
      long windowInterval = 0;
      long windowLatency = 0;

      for (final Delta d : deltas) {
        windowInterval += d.interval;
        windowOps += d.ops;
        windowLatency += d.latency;
      }

      // TODO (dano): this should all be thrown away and replaced with e.g. HdrHistogram so we can
      // TODO (dano): get percentiles instead of just averages.

      final long operations = deltaTime == 0 ? 0 : NANOS_PER_S * deltaOps / deltaTime;
      final long avgOps = windowInterval == 0 ? 0 : NANOS_PER_S * windowOps / windowInterval;
      final double latency = deltaOps == 0 ? 0 : deltaLatency / (NANOS_PER_MS * deltaOps);
      final double avgLatency = windowOps == 0 ? 0 : windowLatency / (NANOS_PER_MS * windowOps);

      table.row(row(name, " ",
                    format("%,12d", operations),
                    " (", format("%,9d", avgOps), " avg) ", unit, "/s ",
                    format("%,12.3f", latency),
                    " (", format("%,12.3f", avgLatency), " avg)", " ms latency ",
                    format("%,12d", totalOperations), " total"));

      prevTotalOperations = totalOperations;
      prevTimestamp = now;
      prevTotalLatency = totalLatency;
    }

    public Object[] row(Object[] first, Object... second) {
      Object[] result = new Object[first.length * 2 + second.length];
      for (int i = 0; i < first.length; i++) {
        result[i * 2] = first[i];
        result[i * 2 + 1] = " ";
      }
      System.arraycopy(second, 0, result, first.length * 2, second.length);
      return result;
    }

    /**
     * Increase this metric.
     *
     * @param delta   The amount to increase.
     * @param latency The total latency for the metric delta, if applicable.
     */
    public void add(final long delta, final long latency) {
      this.totalOperations.add(delta);
      this.totalLatency.add(latency);
    }

    /**
     * Increase this metric by one.
     *
     * @param latency The latency of the event.
     */
    public void inc(final long latency) {
      add(1, latency);
    }

    /**
     * Used to sort metrics by their names.
     */
    @Override
    public int compareTo(final Metric o) {
      for (int i = 0; i < name.length; i++) {
        if (i > o.name.length) {
          return 1;
        }
        final int d = name[i].toString().compareTo(o.name[i].toString());
        if (d != 0) {
          return d;
        }
      }

      return 0;
    }
  }

  private class ProgressPrinter extends Thread {

    private final long start = System.nanoTime();
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private ProgressPrinter() {
      setDaemon(true);
      start();
    }

    public void run() {
      while (run) {
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          continue;
        }
        print();
      }
    }

    private void print() {
      final Date date = new Date();
      final Table table = new Table();
      final long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
      table.header(dateFormat.format(date) + " (" + seconds + " s)");
      for (final MetricGroup group : groups.values()) {
        group.print(table);
      }
      table.print();
      out.println();
    }
  }

  public class MetricGroup {

    private ConcurrentMap<Object, Metric> metrics = Maps.newConcurrentMap();

    /**
     * Create a new metric tracked by this progress meter.
     *
     * @param unit The entity unit, being measured, e.g. "requests", "messages", "operations", etc.
     * @return A new {@link Metric}.
     */
    public Metric metric(final Object key, final String unit, final Object... name) {
      final Metric metric = metrics.get(key);
      if (metric == null) {
        final Metric newMetric = new Metric(name, unit);
        final Metric existingMetric = metrics.putIfAbsent(key, newMetric);
        return fromNullable(existingMetric).or(newMetric);
      }
      return metric;
    }

    public Metric metric(final Object key, final String unit) {
      return metric(key, unit, key);
    }

    private void print(final Table table) {
      for (final Metric metric : sorted(metrics.values())) {
        metric.print(table);
      }
    }

    private <T extends Comparable<T>> List<T> sorted(final Collection<T> values) {
      final List<T> list = Lists.newArrayList(values);
      Collections.sort(list);
      return list;
    }
  }

  private class Table {

    private int[] columns = new int[0];
    private final List<Object[]> rows = Lists.newArrayList();
    private String header;

    public void row(final Object... row) {
      columns = Ints.ensureCapacity(columns, row.length, row.length);
      for (int i = 0; i < row.length; i++) {
        row[i] = row[i].toString();
        columns[i] = max(columns[i], row[i].toString().length());
      }
      rows.add(row);
    }

    public void print() {
      final StringBuilder builder = new StringBuilder();
      if (header != null) {
        builder.append(header);
        builder.append('\n');
        for (int column : columns) {
          for (int i = 0; i < column; i++) {
            builder.append('-');
          }
        }
        builder.append('\n');
        out.print(builder.toString());
      }

      for (final Object[] row : rows) {
        for (int i = 0; i < row.length; i++) {
          final String cell = row[i].toString();
          final int padding = columns[i] - cell.length();
          for (int j = 0; j < padding; j++) {
            out.print(' ');
          }
          out.print(cell);
        }
        out.println();
      }
    }

    public void header(final String header) {
      this.header = header;
    }
  }
}