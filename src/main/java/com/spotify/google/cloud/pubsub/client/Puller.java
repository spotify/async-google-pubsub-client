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

package com.spotify.google.cloud.pubsub.client;

import com.google.common.util.concurrent.MoreExecutors;


import com.swrve.ratelimitedlogger.RateLimitedLog;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
     * @param ackId        The ack id.
     * @return A future that should be completed with the ack id when the message has been consumed.
     */
    CompletionStage<String> handleMessage(Puller puller, String subscription, Message message, String ackId);
  }

  private static final int MAX_LOG_RATE = 3;
  private static final Duration MAX_LOG_DURATION = Duration.millis(2000);

  private static final Logger logger = LoggerFactory.getLogger(Puller.class);
  private static final Logger LOG = RateLimitedLog.withRateLimit(logger)
      .maxRate(MAX_LOG_RATE)
      .every(MAX_LOG_DURATION)
      .build();


  private final ScheduledExecutorService scheduler =
      MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));

  private final Acker acker;

  private final Pubsub pubsub;
  private final String project;
  private final String subscription;
  private final MessageHandler handler;
  private final int concurrency;
  private final int batchSize;
  private final int maxOutstandingMessages;
  private final int maxAckQueueSize;
  private final long pullIntervalMillis;

  private final Backoff backoff;

  private final AtomicInteger outstandingRequests = new AtomicInteger();
  private final AtomicInteger outstandingMessages = new AtomicInteger();

  public Puller(final Builder builder) {
    this.pubsub = Objects.requireNonNull(builder.pubsub, "pubsub");
    this.project = Objects.requireNonNull(builder.project, "project");
    this.subscription = Objects.requireNonNull(builder.subscription, "subscription");
    this.handler = Objects.requireNonNull(builder.handler, "handler");
    this.concurrency = builder.concurrency;
    this.batchSize = builder.batchSize;
    this.maxOutstandingMessages = builder.maxOutstandingMessages;
    this.maxAckQueueSize = builder.maxAckQueueSize;
    this.pullIntervalMillis = builder.pullIntervalMillis;

    this.backoff = Backoff.builder()
        .initialInterval(builder.pullIntervalMillis)
        .maxBackoffMultiplier(builder.maxBackoffMultiplier)
        .build();

    // Set up a batching acker for sending acks
    this.acker = Acker.builder()
        .pubsub(pubsub)
        .project(project)
        .subscription(subscription)
        .batchSize(batchSize)
        .concurrency(concurrency)
        .queueSize(maxAckQueueSize)
        .build();

    // Start pulling
    pull();

    // Schedule pulling to compensate for failures and exceeding the outstanding message limit
    scheduler.scheduleWithFixedDelay(this::pull, pullIntervalMillis, pullIntervalMillis, MILLISECONDS);
  }

  @Override
  public void close() throws IOException {
    scheduler.shutdownNow();
    try {
      scheduler.awaitTermination(30, SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    acker.close();
  }

  public int maxAckQueueSize() {
    return maxAckQueueSize;
  }

  public int maxOutstandingMessages() {
    return maxOutstandingMessages;
  }

  public int outstandingMessages() {
    return outstandingMessages.get();
  }

  public int concurrency() {
    return concurrency;
  }

  public int outstandingRequests() {
    return outstandingRequests.get();
  }

  public int batchSize() {
    return batchSize;
  }

  public String subscription() {
    return subscription;
  }

  public String project() {
    return project;
  }

  public long pullIntervalMillis() {
    return pullIntervalMillis;
  }

  private void pull() {
    while (outstandingRequests.get() < concurrency &&
           outstandingMessages.get() < maxOutstandingMessages) {
      pullBatch();
    }
  }

  private void pullBatch() {
    outstandingRequests.incrementAndGet();

    pubsub.pull(project, subscription, true, batchSize)
        .whenComplete((messages, ex) -> {

          outstandingRequests.decrementAndGet();
          // Bail if pull failed
          if (ex != null) {
            if ( ex instanceof RequestFailedException && ((RequestFailedException)ex).statusCode() == 429 ) {
              LOG.debug("Going too fast, backing off");
            } else {
              LOG.error("Pull failed", ex);
            }
            backoff.sleep();
            return;
          }

          // we are good. Lets go at full speed again.
          backoff.reset();

          // Add entire batch to outstanding message count
          outstandingMessages.addAndGet(messages.size());

          // Call handler for each received message
          for (final ReceivedMessage message : messages) {
            final CompletionStage<String> handlerFuture;
            try {
              handlerFuture = handler.handleMessage(this, subscription, message.message(), message.ackId());
            } catch (Exception e) {
              outstandingMessages.decrementAndGet();
              LOG.error("Message handler threw exception", e);
              continue;
            }

            if (handlerFuture == null) {
              outstandingMessages.decrementAndGet();
              LOG.error("Message handler returned null");
              continue;
            }

            // Decrement the number of outstanding messages when handling is complete
            handlerFuture.whenComplete((ignore, throwable) -> outstandingMessages.decrementAndGet());

            // Ack when the message handling successfully completes
            handlerFuture.thenAccept(acker::acknowledge).exceptionally(throwable -> {
              if (!(throwable instanceof CancellationException)) {
                LOG.error("Acking pubsub threw exception", throwable);
              }
              return null;
            });
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
    private int maxOutstandingMessages = 64_000;
    private int maxAckQueueSize = 10 * batchSize;
    private long pullIntervalMillis = 1000;
    private int maxBackoffMultiplier = 0;

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
     * Set the limit of outstanding messages pending handling. Pulling is throttled when this limit is hit. Default is
     * {@code 64000}.
     */
    public Builder maxOutstandingMessages(final int maxOutstandingMessages) {
      this.maxOutstandingMessages = maxOutstandingMessages;
      return this;
    }

    /**
     * Set the max size for the queue of acks back to Google Cloud Pub/Sub. Default is {@code 10 * batchSize}.
     */
    public Builder maxAckQueueSize(final int maxAckQueueSize) {
      this.maxAckQueueSize = maxAckQueueSize;
      return this;
    }

    /**
     * Set the pull interval in millis. Default is {@code 1000} millis.
     */
    public Builder pullIntervalMillis(final long pullIntervalMillis) {
      this.pullIntervalMillis = pullIntervalMillis;
      return this;
    }

    /**
     * Set the maximum backoff multiplier. Default is {@code 0} (no backoff).
     */
    public Builder maxBackoffMultiplier(final int maxBackoffMultiplier) {
      this.maxBackoffMultiplier = maxBackoffMultiplier;
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
