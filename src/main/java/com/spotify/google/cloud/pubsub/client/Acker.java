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

package com.spotify.google.cloud.pubsub.client;

import com.google.common.util.concurrent.MoreExecutors;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Acker {

  private final ScheduledExecutorService scheduler =
      MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));

  private final AtomicInteger size = new AtomicInteger();
  private final ConcurrentLinkedQueue<QueuedAck> queue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean scheduled = new AtomicBoolean();
  private final AtomicInteger outstanding = new AtomicInteger();

  private final Pubsub pubsub;
  private final String project;
  private final String subscription;
  private final int batchSize;
  private final int queueSize;
  private final long maxLatencyMs;

  private Acker(final Builder builder) {
    this.pubsub = Objects.requireNonNull(builder.pubsub, "pubsub");
    this.project = Objects.requireNonNull(builder.project, "project");
    this.subscription = Objects.requireNonNull(builder.subscription, "subscription");
    this.batchSize = builder.batchSize;
    this.queueSize = Optional.ofNullable(builder.queueSize).orElseGet(() -> batchSize * 10);
    this.maxLatencyMs = builder.maxLatencyMs;
  }

  public CompletableFuture<Void> acknowledge(final String ackId) {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    // Enforce queue size limit
    int currentSize;
    int newSize;
    do {
      currentSize = size.get();
      newSize = currentSize + 1;
      if (newSize > queueSize) {
        future.completeExceptionally(new QueueFullException());
        return future;
      }
    } while (!size.compareAndSet(currentSize, newSize));

    // Enqueue outgoing ack
    queue.add(new QueuedAck(ackId, future));

    // Reached the batch size? Send immediately.
    if (currentSize >= batchSize) {
      send();
      return future;
    }

    // Schedule later acking, allowing more acks to gather into a larger batch.
    if (scheduled.compareAndSet(false, true)) {
      try {
        scheduler.schedule(this::send, maxLatencyMs, MILLISECONDS);
      } catch (RejectedExecutionException ignore) {
        // Race with a call to close(). Ignore.
      }
    }
  }

  private void send() {
    // Clear the scheduled flag before sending.
    scheduled.set(false);

    final int currentOutstanding = outstanding.get();

    // Below outstanding limit so we can send immediately?
    if (currentOutstanding < concurrency) {
      sendBatch();
      return;
    }

    // Enqueue as pending for sending by the earliest available concurrent request slot
    pending = true;
    pendingTopics.offer(this);

    // Tell the listener that a topic became pending for sending as early as possible.
    listener.topicPending(Publisher.this, topic, currentOutstanding, concurrency);

    // Attempt to send pending to guard against losing a race while enqueuing this topic as pending.
    sendPending();
  }

  /**
   * An outgoing ack with the future that should be completed when the ack is complete.
   */
  private static class QueuedAck {

    private final String ackId;
    private final CompletableFuture<Void> future;

    public QueuedAck(final String ackId, final CompletableFuture<Void> future) {
      this.ackId = ackId;
      this.future = future;
    }
  }


  /**
   * Create a builder that can be used to build an {@link Acker}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder that can be used to build an {@link Acker}.
   */
  public static class Builder {

    private Pubsub pubsub;
    private String project;
    private String subscription;
    private int concurrency = 64;
    private int batchSize = 1000;
    private Integer queueSize;
    private long maxLatencyMs = 1000;

    /**
     * Set the {@link Pubsub} client to use. The client will be closed when this {@link Acker} is closed.
     *
     * <p>Note: The client should be configured to at least allow as many connections as the concurrency level of this
     * {@link Acker}.</p>
     */
    public Builder pubsub(final Pubsub pubsub) {
      this.pubsub = pubsub;
      return this;
    }

    /**
     * Set the Google Cloud project to ack on from.
     */
    public Builder project(final String project) {
      this.project = project;
      return this;
    }

    /**
     * The subscription to ack on from.
     */
    public Builder subscription(final String subscription) {
      this.subscription = subscription;
      return this;
    }

    /**
     * Set the Google Cloud Pub/Sub request concurrency level. Default is {@code 64}.
     */
    public Builder concurrency(final int concurrency) {
      this.concurrency = concurrency;
      return this;
    }

    /**
     * Set the Google Cloud Pub/Sub ack batch size. Default is {@code 1000}.
     */
    public Builder batchSize(final int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Set the ack queue size. Default is {@code batchSize * concurrency * 10}.
     */
    public Builder queueSize(final Integer queueSize) {
      this.queueSize = queueSize;
      return this;
    }

    /**
     * Set the maximum latency in millis before sending an incomplete Google Cloud Pub/Sub ack batch request.
     * Default is {@code 1000 ms}.
     */
    public Builder maxLatencyMs(final long maxLatencyMs) {
      this.maxLatencyMs = maxLatencyMs;
      return this;
    }

    /**
     * Build an {@link Acker}.
     */
    public Acker build() {
      return new Acker(this);
    }
  }
}
