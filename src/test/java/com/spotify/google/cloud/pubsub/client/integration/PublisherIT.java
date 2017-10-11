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

import com.google.common.util.concurrent.Futures;

import com.spotify.google.cloud.pubsub.client.Message;
import com.spotify.google.cloud.pubsub.client.Publisher;
import com.spotify.google.cloud.pubsub.client.Pubsub;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static com.spotify.google.cloud.pubsub.client.integration.Util.TEST_NAME_PREFIX;
import static java.lang.System.out;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * Tests publishing to the real Google Cloud Pub/Sub service.
 */
public class PublisherIT {

  private final String PROJECT = Util.defaultProject();

  private final String TOPIC = TEST_NAME_PREFIX + ThreadLocalRandom.current().nextLong();

  private Pubsub pubsub;
  private Publisher publisher;

  @Before
  public void setUp() throws ExecutionException, InterruptedException {
    pubsub = Pubsub.builder()
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
    final Message message = Message.ofEncoded("hello world");

    final List<CompletableFuture<String>> futures = range(0, 10)
        .mapToObj(i -> publisher.publish(TOPIC, message))
        .collect(toList());

    futures.stream()
        .map(Futures::getUnchecked)
        .forEach(id -> out.println("message id: " + id));
  }
}