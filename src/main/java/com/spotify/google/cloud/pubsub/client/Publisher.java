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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A tool for publishing larger volumes of messages to Google Pub/Sub. Does concurrent per-topic batching in order to
 * provide good throughput with large volumes of messages across many different topics.
 *
 * <p> Messages are gathered into batches, bounded by specified batch size and maximum latency. The publisher waits up
 * to the specified max latency before sending a batch of messages for a topic. If enough messages to fill a batch are
 * submitted before the max latency deadline, then the batch is sent immediately.
 *
 * <p> This batching strategy trades publish request quota for increased publishing latency as outgoing messages might
 * spend more time waiting in the publisher per-topic queues before getting sent. The rationale for this strategy is to
 * avoid (expensive and empirically observed) excessive numbers of very small batch publish requests during off-peak.
 */
public class Publisher implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Publisher.class);

  /**
   * A listener for monitoring operations performed by the {@link Publisher}.
   */
  public interface Listener {

    /**
     * Called when a new {@link Publisher} is instantiated.
     *
     * @param publisher The {@link Publisher}
     */
    void publisherCreated(Publisher publisher);

    /**
     * Called when a {@link Publisher} is closed.
     *
     * @param publisher The {@link Publisher}
     */
    void publisherClosed(Publisher publisher);

    /**
     * Called when a {@link Publisher} receieves a new message for publication.
     *
     * @param publisher The {@link Publisher}
     * @param topic     The message topic.
     * @param message   The message.
     * @param future    The future result.
     */
    void publishingMessage(Publisher publisher, String topic, Message message, CompletableFuture<String> future);

    /**
     * Called when a {@link Publisher} is sending a batch of messages to Google Cloud Pub/Sub.
     *
     * @param publisher The {@link Publisher}
     * @param topic     The topic of the message batch.
     * @param batch     The batch of messages being sent.
     * @param future    The future result of the entire batch.
     */
    @Deprecated
    void sendingBatch(Publisher publisher, String topic, List<Message> batch, CompletableFuture<List<String>> future);

    /**
     * Called when a {@link Publisher} is sending a batch of messages to Google Cloud Pub/Sub.
     *
     * @param publisher The {@link Publisher}
     * @param topic     The topic of the message batch.
     * @param batch     The batch of messages being sent.
     * @param future    The future result of the entire batch.
     */
    default void sendingBatch(Publisher publisher, String topic, List<Message> batch,
                              PubsubFuture<List<String>> future) {
      sendingBatch(publisher, topic, batch, (CompletableFuture<List<String>>) future);
    }

    /**
     * Called when a topic is enqueued as pending for future batch sending due to the publisher hitting the concurrency
     * limit.
     *
     * @param publisher   The {@link Publisher}
     * @param topic       The topic.
     * @param outstanding The current number of outstanding batch requests to Google Cloud Pub/Sub.
     * @param concurrency The configured concurrency limit.
     */
    void topicPending(Publisher publisher, String topic, int outstanding, int concurrency);
  }

  private final Pubsub pubsub;
  private final String project;
  private final int queueSize;
  private final int batchSize;
  private final int concurrency;
  private final long maxLatencyMs;
  private final Listener listener;

  private final AtomicInteger outstanding = new AtomicInteger();
  private final ConcurrentLinkedQueue<TopicQueue> pendingTopics = new ConcurrentLinkedQueue<>();
  private final ConcurrentMap<String, TopicQueue> topics = new ConcurrentHashMap<>();
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
  private final ScheduledExecutorService scheduler =
      MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));
  private final AtomicInteger schedulerQueueSize = new AtomicInteger();

  private Publisher(final Builder builder) {
    this.pubsub = Objects.requireNonNull(builder.pubsub, "pubsub");
    this.project = Objects.requireNonNull(builder.project, "project");
    this.concurrency = builder.concurrency;
    this.batchSize = builder.batchSize;
    this.queueSize = Optional.ofNullable(builder.queueSize).orElseGet(() -> batchSize * 10);
    this.maxLatencyMs = builder.maxLatencyMs;
    this.listener = builder.listener == null ? new ListenerAdapter() : builder.listener;
    listener.publisherCreated(this);
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
    final CompletableFuture<String> future = queue.send(message);
    listener.publishingMessage(this, topic, message, future);
    return future;
  }

  /**
   * Close this {@link Publisher}. This will also close the underlying {@link Pubsub} client.
   */
  @Override
  public void close() {
    // TODO (dano): fail outstanding futures
    scheduler.shutdownNow();
    try {
      scheduler.awaitTermination(30, SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    closeFuture.complete(null);
    listener.publisherClosed(Publisher.this);
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
   * Get the total number of scheduled requests.
   */
  public int scheduledQueueSize() {
    return schedulerQueueSize.get();
  }

  /**
   * Get the current number of topics that are pending batch sending to Google Pub/Sub.
   */
  public int pendingTopics() {
    return pendingTopics.size();
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
  public int concurrency() {
    return concurrency;
  }

  /**
   * Get the current queue size for the given topic name. Returns 0 if the topic does not exist.
   *
   * @param topic  the topic name
   */
  public int topicQueueSize(final String topic) {
    final TopicQueue topicQueue = this.topics.get(topic);
    return topicQueue == null ? 0 : topicQueue.queue.size();
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
    private final AtomicBoolean scheduled = new AtomicBoolean();

    private TopicQueue(final String topic) {
      this.topic = topic;
    }

    /**
     * Enqueue a message for sending on this topic queue.
     */
    private CompletableFuture<String> send(final Message message) {
      final CompletableFuture<String> future = new CompletableFuture<>();

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

      // Enqueue outgoing message
      queue.add(new QueuedMessage(message, future));

      // Schedule future batch sending
      scheduleSend(newSize);

      return future;
    }

    /**
     * Schedule this topic for future enqueuing for batch sending. If the batch size has been reached, enqueue for
     * sending immediately.
     *
     * @param queueSize The current number of enqueued messages in this topic.
     */
    private void scheduleSend(final int queueSize) {

      // Bail if this topic is already enqueued for sending.
      if (pending) {
        return;
      }

      // Reached the batch size? Enqueue topic for sending immediately.
      if (queueSize >= batchSize) {
        enqueueSend();
        return;
      }

      // Schedule this topic for later enqueuing, allowing more messages to gather into a larger batch.
      if (scheduled.compareAndSet(false, true)) {
        try {
          scheduler.schedule(this::scheduledEnqueueSend, maxLatencyMs, MILLISECONDS);
          schedulerQueueSize.incrementAndGet();
        } catch (RejectedExecutionException ignore) {
          // Race with a call to close(). Ignore.
        }
      }
    }

    /**
     * Decrements the scheduled queue counter and enqueues the request.
     */
    private void scheduledEnqueueSend() {
      schedulerQueueSize.decrementAndGet();
      // Clear the scheduled flag before enqueuing or sending.
      scheduled.set(false);
      enqueueSendWithErrorLogging();
    }

    /**
     * A wrapper around enqueueSend which catches and logs any exceptions that are thrown. This is
     * called by the executor, which will silently swallow exceptions if we don't handle them here.
     */
    private void enqueueSendWithErrorLogging() {
      try {
        enqueueSend();
      } catch (Exception e) {
        log.error("Error while enqueueing or sending messages on background thread", e);
      }
    }

    /**
     * Enqueue this topic for batch sending. If the request concurrency level is below the limit, send immediately.
     */
    private void enqueueSend() {

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
     * Send a batch of messages.
     */
    private int sendBatch() {
      final List<Message> batch = new ArrayList<>();
      final List<CompletableFuture<String>> futures = new ArrayList<>();

      // Drain queue up to batch size
      while (batch.size() < batchSize) {
        final QueuedMessage message = queue.poll();
        if (message == null) {
          break;
        }
        batch.add(message.message);
        futures.add(message.future);
      }

      // Was there anything to send?
      if (batch.size() == 0) {
        return 0;
      }

      // Decrement the queue size counter
      size.updateAndGet(i -> i - batch.size());

      // Send the batch request and increment the outstanding request counter
      outstanding.incrementAndGet();
      final PubsubFuture<List<String>> batchFuture = pubsub.publish(project, topic, batch);
      listener.sendingBatch(Publisher.this, topic, unmodifiableList(batch), batchFuture);
      batchFuture.whenComplete(
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

      return batch.size();
    }
  }

  /**
   * Send any pending topics.
   */
  private void sendPending() {
    while (outstanding.get() < concurrency) {
      final TopicQueue queue = pendingTopics.poll();
      if (queue == null) {
        return;
      }

      queue.pending = false;
      final int sent = queue.sendBatch();

      // Did we send a whole batch? Then there might be more messages in the queue. Mark as pending again.
      if (sent == batchSize) {
        queue.pending = true;
        pendingTopics.offer(queue);
      }
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
    private Listener listener;
    private long maxLatencyMs = 100;

    /**
     * Set the {@link Pubsub} client to use.
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
     * Set the maximum latency in millis before sending an incomplete Google Cloud Pub/Sub publish batch request.
     * Default is {@code 100 ms}.
     */
    public Builder maxLatencyMs(final long maxLatencyMs) {
      this.maxLatencyMs = maxLatencyMs;
      return this;
    }

    /**
     * Set a {@link Listener} for monitoring operations performed by the {@link Publisher}.
     */
    public Builder listener(final Listener listener) {
      this.listener = listener;
      return this;
    }

    /**
     * Build a {@link Publisher}.
     */
    public Publisher build() {
      return new Publisher(this);
    }
  }

  public static class ListenerAdapter implements Listener {

    @Override
    public void publisherCreated(final Publisher publisher) {

    }

    @Override
    public void publisherClosed(final Publisher publisher) {

    }

    @Override
    public void publishingMessage(final Publisher publisher, final String topic, final Message message,
                                  final CompletableFuture<String> future) {
    }

    @Override
    public void sendingBatch(final Publisher publisher, final String topic, final List<Message> batch,
                             final PubsubFuture<List<String>> future) {
    }

    @Override
    public void sendingBatch(final Publisher publisher, final String topic, final List<Message> batch,
                             final CompletableFuture<List<String>> future) {

    }

    @Override
    public void topicPending(final Publisher publisher, final String topic, final int outstanding,
                             final int concurrency) {

    }
  }
}
