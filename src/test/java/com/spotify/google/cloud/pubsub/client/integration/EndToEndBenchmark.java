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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.pubsub.PubsubScopes;

import com.spotify.google.cloud.pubsub.client.Message;
import com.spotify.google.cloud.pubsub.client.Publisher;
import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.google.cloud.pubsub.client.ReceivedMessage;
import com.spotify.logging.LoggingConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.zip.Deflater;

import static com.spotify.google.cloud.pubsub.client.integration.Util.nonGcmCiphers;
import static com.spotify.logging.LoggingConfigurator.Level.WARN;
import static java.util.stream.Collectors.toList;

public class EndToEndBenchmark {

  private static final int PUBLISHER_CONCURRENCY = 128;
  private static final int PULLER_CONCURRENCY = 128;

  private static final int MESSAGE_SIZE = 512;

  public static void main(final String... args) throws IOException, ExecutionException, InterruptedException {

    final String project = Util.defaultProject();

    GoogleCredential credential;

    // Use credentials from file if available
    try {
      credential = GoogleCredential
          .fromStream(new FileInputStream("credentials.json"))
          .createScoped(PubsubScopes.all());
    } catch (IOException e) {
      credential = GoogleCredential.getApplicationDefault()
          .createScoped(PubsubScopes.all());
    }

    final Pubsub pubsub = Pubsub.builder()
        .credential(credential)
        .compressionLevel(Deflater.BEST_SPEED)
        .enabledCipherSuites(nonGcmCiphers())
        .build();

    final Publisher publisher = Publisher.builder()
        .pubsub(pubsub)
        .concurrency(PUBLISHER_CONCURRENCY)
        .project(project)
        .build();

    LoggingConfigurator.configureDefaults("benchmark", WARN);

    final String topic = "test-" + Long.toHexString(ThreadLocalRandom.current().nextLong());
    final String subscription = "test-" + Long.toHexString(ThreadLocalRandom.current().nextLong());

    pubsub.createTopic(project, topic).get();
    pubsub.createSubscription(project, subscription, topic).get();

    final List<String> payloads = IntStream.range(0, 1024)
        .mapToObj(i -> {
          final StringBuilder s = new StringBuilder();
          while (s.length() < MESSAGE_SIZE) {
            s.append(ThreadLocalRandom.current().nextInt());
          }
          return Message.encode(s.toString());
        })
        .collect(toList());
    final int payloadIxMask = 1024 - 1;

    final Supplier<Message> generator = () -> Message.builder()
        .data(payloads.get(ThreadLocalRandom.current().nextInt() & payloadIxMask))
        .putAttribute("ts", Long.toHexString(System.nanoTime()))
        .build();

    final ProgressMeter meter = new ProgressMeter();
    final ProgressMeter.Metric publishes = meter.group("operations").metric("publishes", "messages");
    final ProgressMeter.Metric receives = meter.group("operations").metric("receives", "messages");

    for (int i = 0; i < 100000; i++) {
      publish(publisher, generator, topic, publishes);
    }

    // Pull concurrently and (asynchronously) publish a new message for every message received
    for (int i = 0; i < PULLER_CONCURRENCY; i++) {
      pull(project, pubsub, subscription, receives, () -> publish(publisher, generator, topic, publishes));
    }
  }

  private static void publish(final Publisher publisher, final Supplier<Message> generator,
                              final String topic, final ProgressMeter.Metric publishes) {
    final Message message = generator.get();
    final CompletableFuture<String> future = publisher.publish(topic, message);
    final long start = System.nanoTime();
    future.whenComplete((s, ex) -> {
      if (ex != null) {
        ex.printStackTrace();
        return;
      }
      final long end = System.nanoTime();
      final long latency = end - start;
      publishes.inc(latency);
    });
  }

  private static void pull(final String project, final Pubsub pubsub, final String subscription,
                           final ProgressMeter.Metric receives, final Runnable callback) {
    pubsub.pull(project, subscription, false, 1000)
        .whenComplete((messages, ex) -> {
          if (ex != null) {
            ex.printStackTrace();
            return;
          }
          // Immediately kick off another pull
          pull(project, pubsub, subscription, receives, callback);

          // Ack received messages
          final String[] ackIds = messages.stream().map(ReceivedMessage::ackId).toArray(String[]::new);
          pubsub.acknowledge(project, subscription, ackIds);

          // Account for and call callback for each received message
          for (final ReceivedMessage message : messages) {
            final String tsHex = message.message().attributes().get("ts");
            final long tsNanos = Long.valueOf(tsHex, 16);
            final long latencyNanos = System.nanoTime() - tsNanos;
            receives.inc(latencyNanos);
            callback.run();
          }
        });
  }
}
