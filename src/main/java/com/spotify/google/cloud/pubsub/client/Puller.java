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
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Puller implements Closeable {

  /**
   * A handler for received messages.
   */
  public interface MessageHandler {

    /**
     * Called when a {@link Puller} receives a message.
     *
     * @param puller       The {@link Puller}
     * @param subscription The subscription that the message was received on.
     * @param message      The message.
     * @return A future that should be completed when the message is to be acked.
     */
    CompletableFuture<Void> messageReceived(Puller puller, String subscription, ReceivedMessage message);
  }

  private static final Logger log = LoggerFactory.getLogger(Puller.class);

  private final ScheduledExecutorService scheduler =
      MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));

  private final Acker acker;

  private final Pubsub pubsub;
  private final String project;
  private final String subscription;
  private final MessageHandler handler;
  private final int concurrency;
  private final int batchSize;

  public Puller(final Builder builder) {
    this.pubsub = Objects.requireNonNull(builder.pubsub, "pubsub");
    this.project = Objects.requireNonNull(builder.project, "project");
    this.subscription = Objects.requireNonNull(builder.subscription, "subscription");
    this.handler = Objects.requireNonNull(builder.handler, "listener");
    this.concurrency = builder.concurrency;
    this.batchSize = builder.batchSize;

    // Set up a batching acker for sending acks
    this.acker = Acker.builder()
        .pubsub(pubsub)
        .project(project)
        .subscription(subscription)
        .batchSize(batchSize)
        .concurrency(concurrency)
        .build();

    // Start concurrent pulling
    for (int i = 0; i < concurrency; i++) {
      pull();
    }
  }

  @Override
  public void close() throws IOException {
    scheduler.shutdownNow();
    try {
      scheduler.awaitTermination(30, SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void pull() {
    pubsub.pull(project, subscription, false, batchSize)
        .whenComplete((messages, ex) -> {

          // Schedule a retry if the pull failed
          if (ex != null) {
            // TODO (dano): exponential backoff
            scheduler.schedule(this::pull, 1, TimeUnit.SECONDS);
            return;
          }

          // Kick off another pull
          pull();

          // Call handler for each received message
          for (final ReceivedMessage message : messages) {
            final CompletableFuture<Void> ackFuture;
            try {
              ackFuture = handler.messageReceived(this, subscription, message);
            } catch (Exception e) {
              log.error("Message handler threw exception", e);
              continue;
            }

            final String ackId = message.ackId();
            ackFuture.thenRun(() -> acker.acknowledge(ackId));
          }
        });
  }

  /**
   * Create a builder that can be used to build a {@link Puller}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder that can be used to build a {@link Puller}.
   */
  public static class Builder {

    private Pubsub pubsub;
    private String project;
    private String subscription;
    private MessageHandler handler;
    private int concurrency = 64;
    private int batchSize = 1000;

    /**
     * Set the {@link Pubsub} client to use. The client will be closed when this {@link Puller} is closed.
     *
     * <p>Note: The client should be configured to at least allow as many connections as the concurrency level of this
     * {@link Puller}.</p>
     */
    public Builder pubsub(final Pubsub pubsub) {
      this.pubsub = pubsub;
      return this;
    }

    /**
     * Set the Google Cloud project to pull from.
     */
    public Builder project(final String project) {
      this.project = project;
      return this;
    }

    /**
     * The subscription to pull from.
     */
    public Builder subscription(final String subscription) {
      this.subscription = subscription;
      return this;
    }

    /**
     * The handler to call for received messages.
     */
    public Builder messageHandler(final MessageHandler messageHandler) {
      this.handler = messageHandler;
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
     * Set the Google Cloud Pub/Sub pull batch size. Default is {@code 1000}.
     */
    public Builder batchSize(final int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Build a {@link Puller}.
     */
    public Puller build() {
      return new Puller(this);
    }
  }
}
