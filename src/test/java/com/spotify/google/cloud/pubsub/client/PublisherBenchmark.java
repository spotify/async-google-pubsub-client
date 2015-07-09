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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;

import com.spotify.logging.LoggingConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.Deflater;

import javax.net.ssl.SSLContext;

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
        .maxConnections(256)
        .compressionLevel(Deflater.BEST_SPEED)
        .enabledCipherSuites(nonGcmCiphers())
        .build();

    final Publisher publisher = Publisher.builder()
        .pubsub(pubsub)
        .concurrency(128)
        .project(project)
        .build();

    LoggingConfigurator.configureDefaults("benchmark", WARN);

    final String topicPrefix = "test-topic-" + ThreadLocalRandom.current().nextInt();

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
          final String data = BaseEncoding.base64().encode(utf8(s.toString()));
          return Message.builder().data(data).build();
        })
        .collect(toList());

    final ProgressMeter meter = new ProgressMeter("messages", true);

    for (int i = 0; i < 100000; i++) {
      benchSend(publisher, messages, topics, meter);
    }
  }

  private static byte[] utf8(final String s) {
    try {
      return s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw Throwables.propagate(e);
    }
  }

  private static void benchSend(final Publisher publisher, final List<Message> messages,
                                final List<String> topics, final ProgressMeter meter) {
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
      meter.inc(1, latency);
      benchSend(publisher, messages, topics, meter);
    });
  }

  /**
   * Use non-GCM ciphers for now as the GCM performance in Java 8 (pre 8u60) is not good.
   *
   * https://bugs.openjdk.java.net/browse/JDK-8069072
   */
  private static String[] nonGcmCiphers() {
    final SSLContext sslContext;
    try {
      sslContext = SSLContext.getDefault();
    } catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }

    final String[] defaultCiphers = sslContext.getDefaultSSLParameters().getCipherSuites();

    return Stream.of(defaultCiphers)
        .filter(cipher -> !cipher.contains("GCM"))
        .toArray(String[]::new);
  }
}
