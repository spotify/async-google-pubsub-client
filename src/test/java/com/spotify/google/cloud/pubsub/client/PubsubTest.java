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
import com.google.api.client.googleapis.util.Utils;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static com.spotify.google.cloud.pubsub.client.Util.TEST_TOPIC_PREFIX;
import static java.lang.System.out;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class PubsubTest {

  private static final int CONCURRENCY = 128;

  private final String PROJECT = Util.defaultProject();

  private final String TOPIC = TEST_TOPIC_PREFIX + ThreadLocalRandom.current().nextLong();

  private static GoogleCredential CREDENTIAL;

  private Pubsub pubsub;

  @BeforeClass
  public static void setUpCredentials() throws IOException {
    CREDENTIAL = GoogleCredential.getApplicationDefault(
        Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());
  }

  @Before
  public void setUp() {
    pubsub = Pubsub.builder()
        .maxConnections(CONCURRENCY)
        .credential(CREDENTIAL)
        .build();
  }

  @After
  public void tearDown() throws ExecutionException, InterruptedException {
    if (pubsub != null) {
      pubsub.deleteTopic(PROJECT, TOPIC).exceptionally(t -> null).get();
      pubsub.close();
    }
  }

  @Test
  public void testCreateGetListDeleteTopics()
      throws IOException, ExecutionException, InterruptedException {

    // Create topic
    final Topic expected = Topic.of(PROJECT, TOPIC);
    {
      final Topic topic = pubsub.createTopic(PROJECT, TOPIC).get();
      assertThat(topic, is(expected));
    }

    // Get topic
    {
      final Topic topic = pubsub.getTopic(PROJECT, TOPIC).get();
      assertThat(topic, is(expected));
    }

    // Verify that the topic is listed
    {
      final List<Topic> topics = topics();
      assertThat(topics, hasItem(expected));
    }

    // Delete topic
    {
      pubsub.deleteTopic(PROJECT, TOPIC).get();
    }

    // Verify that topic is gone
    {
      final Topic topic = pubsub.getTopic(PROJECT, TOPIC).get();
      assertThat(topic, is(nullValue()));
    }
    {
      final List<Topic> topics = topics();
      assertThat(topics, not(contains(expected)));
    }
  }

  private List<Topic> topics() throws ExecutionException, InterruptedException {
    final List<Topic> topics = new ArrayList<>();
    Optional<String> pageToken = Optional.empty();
    while (true) {
      final TopicList response = pubsub.listTopics(PROJECT, pageToken.orElse(null)).get();
      topics.addAll(response.topics());
      pageToken = response.nextPageToken();
      if (!pageToken.isPresent()) {
        break;
      }
    }
    return topics;
  }

  @Test
  public void testPublish() throws IOException, ExecutionException, InterruptedException {
    final String data = BaseEncoding.base64().encode("hello world".getBytes("UTF-8"));
    final Message message = new MessageBuilder().data(data).build();
    final List<String> response = pubsub.publish(PROJECT, "dano-test", message).get();
    out.println(response);
  }

  @Test
  @Ignore
  public void listAllTopics() throws ExecutionException, InterruptedException {
    topics().forEach(System.out::println);
  }

  @Test
  @Ignore
  public void cleanUpTestTopics() throws ExecutionException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY / 2);
    Optional<String> pageToken = Optional.empty();
    while (true) {
      final TopicList response = pubsub.listTopics(PROJECT, pageToken.orElse(null)).get();
      response.topics().stream()
          .map(Topic::name)
          .filter(t -> t.contains(TEST_TOPIC_PREFIX))
          .map(t -> executor.submit(() -> {
            System.out.println("Removing topic: " + t);
            return pubsub.deleteTopic(t).get();
          }))
          .collect(toList())
          .forEach(Futures::getUnchecked);
      pageToken = response.nextPageToken();
      if (!pageToken.isPresent()) {
        break;
      }
    }
  }
}