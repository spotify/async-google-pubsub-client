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

import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.System.out;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * Tests publishing to the real Google Cloud Pub/Sub service.
 */
public class PublisherIT {

  private final String PROJECT = Util.defaultProject();

  private final String TOPIC = "test-topic-" + ThreadLocalRandom.current().nextLong();

  private Pubsub pubsub;
  private Publisher publisher;

  @Before
  public void setUp() throws ExecutionException, InterruptedException {
    pubsub = Pubsub.builder()
        .maxConnections(20)
        .build();
    publisher = Publisher.builder()
        .concurrency(20)
        .pubsub(pubsub)
        .project(PROJECT)
        .build();
    pubsub.createTopic(PROJECT, TOPIC).get();
  }

  @After
  public void tearDown() throws ExecutionException, InterruptedException {
    if (publisher != null) {
      publisher.close();
    }
    if (pubsub != null) {
      pubsub.deleteTopic(PROJECT, TOPIC).exceptionally(t -> null).get();
      pubsub.close();
    }
  }

  @Test
  public void testPublish()
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    final String data = BaseEncoding.base64().encode("hello world".getBytes("UTF-8"));
    final Message message = Message.builder().data(data).build();

    final List<CompletableFuture<String>> futures = range(0, 10)
        .mapToObj(i -> publisher.publish(TOPIC, message))
        .collect(toList());

    futures.stream()
        .map(Futures::getUnchecked)
        .forEach(id -> out.println("message id: " + id));
  }
}