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

public class Publisher implements Closeable {

  private final Pubsub pubsub;
  private final String project;
  private final int queueSize;
  private final int batchSize;
  private final int concurrency;

  private final AtomicInteger outstanding = new AtomicInteger();

  private final ConcurrentMap<String, TopicQueue> topics = new ConcurrentHashMap<>();

  private Publisher(final Builder builder) {
    this.pubsub = Objects.requireNonNull(builder.pubsub, "pubsub");
    this.project = Objects.requireNonNull(builder.project, "project");
    this.concurrency = builder.concurrency;
    this.batchSize = builder.batchSize;
    this.queueSize = Optional.ofNullable(builder.queueSize).orElseGet(() -> batchSize * concurrency * 10);
  }

  public CompletableFuture<String> publish(final String topic, final Message message) {
    final TopicQueue queue = topics.computeIfAbsent(topic, TopicQueue::new);
    return queue.send(message);
  }

  @Override
  public void close() {
  }

  private class TopicQueue {

    private final AtomicInteger size = new AtomicInteger();
    private final ConcurrentLinkedQueue<QueuedMessage> queue = new ConcurrentLinkedQueue<>();
    private final String topic;

    public TopicQueue(final String topic) {
      this.topic = topic;
    }

    public CompletableFuture<String> send(final Message message) {
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

      send();

      return future;
    }

    public void send() {
      if (outstanding.get() >= concurrency) {
        return;
      }
      outstanding.incrementAndGet();

      final PublishRequestBuilder builder = PublishRequest.builder();
      final List<CompletableFuture<String>> futures = new ArrayList<>();

      // Drain queue up to batch size
      QueuedMessage message;
      while ((message = queue.poll()) != null && builder.messages().size() < batchSize) {
        builder.addMessage(message.message);
        futures.add(message.future);
      }

      // Was there anything to send?
      if (builder.messages().size() == 0) {
        return;
      }

      // Decrement the queue size counter
      size.updateAndGet(i -> i - builder.messages().size());

      // Send the batch
      final PublishRequest request = builder.build();
      pubsub.publish(project, topic, request).whenComplete(
          (PublishResponse response, Throwable ex) -> {
            outstanding.decrementAndGet();

            // Fail all futures if the batch request failed
            if (ex != null) {
              futures.forEach(f -> f.completeExceptionally(ex));
              return;
            }

            // Verify that the number of message id's and messages match up
            if (futures.size() != response.messageIds().size()) {
              futures.forEach(f -> f.completeExceptionally(
                  new PubsubException(
                      "message id count mismatch: " +
                      futures.size() + " != " + response.messageIds().size())));
            }

            // Complete each future with the appropriate message id
            for (int i = 0; i < futures.size(); i++) {
              final String messageId = response.messageIds().get(i);
              final CompletableFuture<String> future = futures.get(i);
              future.complete(messageId);
            }
          });
    }
  }

  private static class QueuedMessage {

    private final Message message;
    private final CompletableFuture<String> future;

    public QueuedMessage(final Message message, final CompletableFuture<String> future) {
      this.message = message;
      this.future = future;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Pubsub pubsub;
    private String project;
    private Integer queueSize;
    private int batchSize = 1000;
    private int concurrency = 64;

    public Builder pubsub(final Pubsub pubsub) {
      this.pubsub = pubsub;
      return this;
    }

    public Builder project(final String project) {
      this.project = project;
      return this;
    }

    public Builder queueSize(final Integer queueSize) {
      this.queueSize = queueSize;
      return this;
    }

    public Builder batchSize(final int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder concurrency(final int concurrency) {
      this.concurrency = concurrency;
      return this;
    }

    public Publisher build() {
      return new Publisher(this);
    }
  }
}
