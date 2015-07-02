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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A tool for publishing larger volumes of messages to Google Pub/Sub. Does concurrent per-topic batching in order to
 * provide good throughput with large volumes of messages across many different topics.
 */
public class Publisher implements Closeable {

  private final Pubsub pubsub;
  private final String project;
  private final int queueSize;
  private final int batchSize;
  private final int concurrency;

  private final AtomicInteger outstanding = new AtomicInteger();
  private final ConcurrentLinkedQueue<TopicQueue> pendingTopics = new ConcurrentLinkedQueue<>();
  private final ConcurrentMap<String, TopicQueue> topics = new ConcurrentHashMap<>();
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

  private Publisher(final Builder builder) {
    this.pubsub = Objects.requireNonNull(builder.pubsub, "pubsub");
    this.project = Objects.requireNonNull(builder.project, "project");
    this.concurrency = builder.concurrency;
    this.batchSize = builder.batchSize;
    this.queueSize = Optional.ofNullable(builder.queueSize).orElseGet(() -> batchSize * 10);
  }

  /**
   * Publish a message on a specific topic.
   *
   * @param topic   The topic name to publish on. Note that this is the short name, not the fully qualified name
   *                including project. The project to publish on is configured using the {@link Builder}.
   * @param message The message to publish.
   * @return A future that is fulfilled with the resulting Google Pub/Sub message ID when the message has been
   * successfully published.
   */
  public CompletableFuture<String> publish(final String topic, final Message message) {
    final TopicQueue queue = topics.computeIfAbsent(topic, TopicQueue::new);
    return queue.send(message);
  }

  /**
   * Close this {@link Publisher}. This will also close the underlying {@link Pubsub} client.
   */
  @Override
  public void close() {
    pubsub.close();
    closeFuture.complete(null);
  }

  /**
   * Get a future that is completed when this {@link Publisher} is closed.
   */
  public CompletableFuture<Void> closeFuture() {
    return closeFuture.thenApply(ignore -> null);
  }

  /**
   * Get the current number of outstanding (batch) requests to Google Pub/Sub.
   */
  public int outstandingRequests() {
    return outstanding.get();
  }

  /**
   * Get the Google Cloud project this {@link Publisher} is publishing to.
   */
  public String project() {
    return project;
  }

  /**
   * Get the concurrent Google Pub/Sub request limit.
   */
  public int concurrencyLimit() {
    return concurrency;
  }

  /**
   * Get the per-topic queue size.
   */
  public int queueSize() {
    return queueSize;
  }

  /**
   * Get the max Google Pub/Sub request batch size.
   */
  public int batchSize() {
    return batchSize;
  }

  /**
   * The per-topic queue of messages.
   */
  private class TopicQueue {

    private final AtomicInteger size = new AtomicInteger();
    private final ConcurrentLinkedQueue<QueuedMessage> queue = new ConcurrentLinkedQueue<>();
    private final String topic;

    private volatile boolean pending;

    private TopicQueue(final String topic) {
      this.topic = topic;
    }

    /**
     * Enqueue a message for sending on this topic queue.
     */
    private CompletableFuture<String> send(final Message message) {
      final CompletableFuture<String> future = new CompletableFuture<>();

      int currentSize;
      int newSize;
      do {
        currentSize = size.get();
        newSize = currentSize + 1;
        if (newSize > queueSize) {
          future.completeExceptionally(new QueueFullException());
        }
      } while (!size.compareAndSet(currentSize, newSize));

      queue.add(new QueuedMessage(message, future));

      if (!pending) {
        send();
      }

      return future;
    }

    /**
     * Send a batch of messages. If the concurrency level of the publisher has already been reached, enqueue this topic
     * onto the pending queue for later dispatch.
     */
    private void send() {
      // Too many outstanding already? Add to pending queue
      if (outstanding.get() >= concurrency) {
        pending = true;
        pendingTopics.offer(this);

        // Check if we lost a race while enqueing this topic and should proceed with sending immediately
        if (outstanding.get() >= concurrency) {
          return;
        }
      }

      // Good to go. Clear the pending flag.
      pending = false;

      final List<Message> batch = new ArrayList<>();
      final List<CompletableFuture<String>> futures = new ArrayList<>();

      // Drain queue up to batch size
      QueuedMessage message;
      while ((message = queue.poll()) != null && batch.size() < batchSize) {
        batch.add(message.message);
        futures.add(message.future);
      }

      // Was there anything to send?
      if (batch.size() == 0) {
        return;
      }

      // Decrement the queue size counter
      size.updateAndGet(i -> i - batch.size());

      // Send the batch request and increment the outstanding request counter
      outstanding.incrementAndGet();
      pubsub.publish(project, topic, batch).whenComplete(
          (List<String> messageIds, Throwable ex) -> {

            // Decrement the outstanding request counter
            outstanding.decrementAndGet();

            // Fail all futures if the batch request failed
            if (ex != null) {
              futures.forEach(f -> f.completeExceptionally(ex));
              return;
            }

            // Verify that the number of message id's and messages match up
            if (futures.size() != messageIds.size()) {
              futures.forEach(f -> f.completeExceptionally(
                  new PubsubException(
                      "message id count mismatch: " +
                      futures.size() + " != " + messageIds.size())));
            }

            // Complete each future with the appropriate message id
            for (int i = 0; i < futures.size(); i++) {
              final String messageId = messageIds.get(i);
              final CompletableFuture<String> future = futures.get(i);
              future.complete(messageId);
            }
          })

          // When batch is complete, process pending topics.
          .whenComplete((v, t) -> sendPending());
    }
  }

  /**
   * Send a pending topic, if any.
   */
  private void sendPending() {
    final TopicQueue queue = pendingTopics.poll();
    if (queue != null) {
      queue.send();
    }
  }

  /**
   * An outgoing message with the future that should be completed when the message has been published.
   */
  private static class QueuedMessage {

    private final Message message;
    private final CompletableFuture<String> future;

    public QueuedMessage(final Message message, final CompletableFuture<String> future) {
      this.message = message;
      this.future = future;
    }
  }

  /**
   * Create a builder that can be used to build a {@link Publisher}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder that can be used to build a {@link Publisher}.
   */
  public static class Builder {

    private Pubsub pubsub;
    private String project;
    private Integer queueSize;

    private int batchSize = 1000;
    private int concurrency = 64;

    /**
     * Set the {@link Pubsub} client to use. The client will be closed when this {@link Publisher} is closed.
     *
     * <p>Note: The client should be configured to at least allow as many connections as the concurrency level of this
     * {@link Publisher}.</p>
     */
    public Builder pubsub(final Pubsub pubsub) {
      this.pubsub = pubsub;
      return this;
    }

    /**
     * Set the Google Cloud project to publish to.
     */
    public Builder project(final String project) {
      this.project = project;
      return this;
    }

    /**
     * Set the maximum batch size. Default is {@code 1000}, which is also the maximum Google Cloud Pub/Sub batch size.
     */
    public Builder batchSize(final int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Set the per-topic queue size. Default is {@code batchSize * concurrency * 10}.
     */
    public Builder queueSize(final Integer queueSize) {
      this.queueSize = queueSize;
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
     * Build a {@link Publisher}.
     */
    public Publisher build() {
      return new Publisher(this);
    }
  }
}
