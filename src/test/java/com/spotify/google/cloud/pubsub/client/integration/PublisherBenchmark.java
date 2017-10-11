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
import com.google.common.util.concurrent.Futures;

import com.spotify.google.cloud.pubsub.client.Message;
import com.spotify.google.cloud.pubsub.client.Publisher;
import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.logging.LoggingConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.zip.Deflater;

import static com.spotify.google.cloud.pubsub.client.integration.Util.TEST_NAME_PREFIX;
import static com.spotify.google.cloud.pubsub.client.integration.Util.nonGcmCiphers;
import static com.spotify.logging.LoggingConfigurator.Level.WARN;
import static java.util.stream.Collectors.toList;

public class PublisherBenchmark {

  private static final int MESSAGE_SIZE = 512;

  public static void main(final String... args) throws IOException {

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
        .concurrency(128)
        .project(project)
        .build();

    LoggingConfigurator.configureDefaults("benchmark", WARN);

    final String topicPrefix = TEST_NAME_PREFIX + ThreadLocalRandom.current().nextInt();

    final List<String> topics = IntStream.range(0, 100)
        .mapToObj(i -> topicPrefix + "-" + i)
        .collect(toList());

    topics.stream()
        .map(topic -> pubsub.createTopic(project, topic))
        .collect(toList())
        .forEach(Futures::getUnchecked);

    final List<Message> messages = IntStream.range(0, 1000)
        .mapToObj(i -> {
          final StringBuilder s = new StringBuilder();
          while (s.length() < MESSAGE_SIZE) {
            s.append(ThreadLocalRandom.current().nextInt());
          }
          return Message.ofEncoded(s.toString());
        })
        .collect(toList());

    final ProgressMeter meter = new ProgressMeter();
    final ProgressMeter.Metric requests = meter.group("operations").metric("publishes", "messages");

    for (int i = 0; i < 100000; i++) {
      benchSend(publisher, messages, topics, requests);
    }
  }

  private static void benchSend(final Publisher publisher, final List<Message> messages,
                                final List<String> topics, final ProgressMeter.Metric requests) {
    final String topic = topics.get(ThreadLocalRandom.current().nextInt(topics.size()));
    final Message message = messages.get(ThreadLocalRandom.current().nextInt(messages.size()));
    final CompletableFuture<String> future = publisher.publish(topic, message);
    final long start = System.nanoTime();
    future.whenComplete((s, ex) -> {
      if (ex != null) {
        ex.printStackTrace();
        return;
      }
      final long end = System.nanoTime();
      final long latency = end - start;
      requests.inc(latency);
      benchSend(publisher, messages, topics, requests);
    });
  }
}
