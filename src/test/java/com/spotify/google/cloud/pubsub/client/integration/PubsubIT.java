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
import com.google.api.client.googleapis.util.Utils;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;

import com.spotify.google.cloud.pubsub.client.Message;
import com.spotify.google.cloud.pubsub.client.MessageBuilder;
import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.google.cloud.pubsub.client.PubsubFuture;
import com.spotify.google.cloud.pubsub.client.ReceivedMessage;
import com.spotify.google.cloud.pubsub.client.Subscription;
import com.spotify.google.cloud.pubsub.client.SubscriptionList;
import com.spotify.google.cloud.pubsub.client.Topic;
import com.spotify.google.cloud.pubsub.client.TopicList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;

import static com.spotify.google.cloud.pubsub.client.integration.Util.TEST_NAME_PREFIX;
import static java.lang.Long.toHexString;
import static java.lang.System.out;
import static java.util.stream.Collectors.toList;
import static java.util.zip.Deflater.BEST_SPEED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests talking to the real Google Cloud Pub/Sub service.
 */
public class PubsubIT {

  private static final int CONCURRENCY = 128;

  private static final String PROJECT = Util.defaultProject();


  private static GoogleCredential CREDENTIAL;

  private Pubsub pubsub;
  private String topicName;
  private String subscriptionName;

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
    topicName = TEST_NAME_PREFIX + toHexString(ThreadLocalRandom.current().nextLong());
    subscriptionName = TEST_NAME_PREFIX + toHexString(ThreadLocalRandom.current().nextLong());
  }

  @After
  public void tearDown() throws ExecutionException, InterruptedException {
    if (pubsub != null) {
      pubsub.deleteSubscription(PROJECT, subscriptionName).exceptionally(t -> null).get();
      pubsub.deleteTopic(PROJECT, topicName).exceptionally(t -> null).get();
      pubsub.close();
    }
  }

  @Test
  public void testCreateGetListDeleteTopics() throws Exception {
    testCreateGetListDeleteTopics(pubsub);
  }

  private void testCreateGetListDeleteTopics(final Pubsub pubsub) throws Exception {

    // Create topic
    final Topic expected = Topic.of(PROJECT, topicName);
    {
      final Topic topic = pubsub.createTopic(PROJECT, topicName).get();
      assertThat(topic, is(expected));
    }

    // Get topic
    {
      final Topic topic = pubsub.getTopic(PROJECT, topicName).get();
      assertThat(topic, is(expected));
    }

    // Verify that the topic is listed
    {
      final List<Topic> topics = topics(pubsub);
      assertThat(topics, hasItem(expected));
    }

    // Delete topic
    {
      pubsub.deleteTopic(PROJECT, topicName).get();
    }

    // Verify that topic is gone
    {
      final Topic topic = pubsub.getTopic(PROJECT, topicName).get();
      assertThat(topic, is(nullValue()));
    }
    {
      final List<Topic> topics = topics(pubsub);
      assertThat(topics, not(contains(expected)));
    }
  }

  private static List<Topic> topics(final Pubsub pubsub) throws ExecutionException, InterruptedException {
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
  public void testCreateGetListDeleteSubscriptions() throws Exception {
    // Create topic to subscribe to
    final Topic topic = pubsub.createTopic(PROJECT, topicName).get();

    // Create subscription
    final Subscription expected = Subscription.of(PROJECT, subscriptionName, topicName);
    {
      final Subscription subscription = pubsub.createSubscription(PROJECT, subscriptionName, topicName).get();
      assertThat(subscription.name(), is(expected.name()));
      assertThat(subscription.topic(), is(expected.topic()));
    }

    // Get subscription
    {
      final Subscription subscription = pubsub.getSubscription(PROJECT, subscriptionName).get();
      assertThat(subscription.name(), is(expected.name()));
      assertThat(subscription.topic(), is(expected.topic()));
    }

    // Verify that the subscription is listed
    {
      final List<Subscription> subscriptions = subscriptions(pubsub);
      assertThat(subscriptions.stream()
                     .anyMatch(s -> s.name().equals(expected.name()) &&
                                    s.topic().equals(expected.topic())),
                 is(true));
    }

    // Delete subscription
    {
      pubsub.deleteSubscription(PROJECT, subscriptionName).get();
    }

    // Verify that subscription is gone
    {
      final Subscription subscription = pubsub.getSubscription(PROJECT, subscriptionName).get();
      assertThat(subscription, is(nullValue()));
    }
    {
      final List<Subscription> subscriptions = subscriptions(pubsub);
      assertThat(subscriptions.stream()
                     .noneMatch(s -> s.name().equals(expected.name())),
                 is(true));
    }
  }

  private static List<Subscription> subscriptions(final Pubsub pubsub) throws ExecutionException, InterruptedException {
    final List<Subscription> subscriptions = new ArrayList<>();
    Optional<String> pageToken = Optional.empty();
    while (true) {
      final SubscriptionList response = pubsub.listSubscriptions(PROJECT, pageToken.orElse(null)).get();
      subscriptions.addAll(response.subscriptions());
      pageToken = response.nextPageToken();
      if (!pageToken.isPresent()) {
        break;
      }
    }
    return subscriptions;
  }

  @Test
  public void testPublish() throws IOException, ExecutionException, InterruptedException {
    pubsub.createTopic(PROJECT, topicName).get();
    final String data = BaseEncoding.base64().encode("hello world".getBytes("UTF-8"));
    final Message message = new MessageBuilder().data(data).build();
    final List<String> response = pubsub.publish(PROJECT, topicName, message).get();
    out.println(response);
  }

  @Test
  public void testPullSingle() throws IOException, ExecutionException, InterruptedException {
    // Create topic and subscription
    pubsub.createTopic(PROJECT, topicName).get();
    pubsub.createSubscription(PROJECT, subscriptionName, topicName).get();

    // Publish a message
    final String data = BaseEncoding.base64().encode("hello world".getBytes("UTF-8"));
    final Message message = Message.of(data);
    final List<String> ids = pubsub.publish(PROJECT, topicName, message).get();
    final String id = ids.get(0);
    final List<ReceivedMessage> response = pubsub.pull(PROJECT, subscriptionName, false).get();

    // Verify received message
    assertThat(response.size(), is(1));
    assertThat(response.get(0).message().data(), is(data));
    assertThat(response.get(0).message().messageId().get(), is(id));
    assertThat(response.get(0).message().publishTime().get(), is(notNullValue()));
    assertThat(response.get(0).ackId(), not(isEmptyOrNullString()));

    // Modify ack deadline
    pubsub.modifyAckDeadline(PROJECT, subscriptionName, 30, response.get(0).ackId()).get();

    // Ack message
    pubsub.acknowledge(PROJECT, subscriptionName, response.get(0).ackId()).get();
  }

  @Test
  public void testPullBatch() throws IOException, ExecutionException, InterruptedException {
    pubsub.createTopic(PROJECT, topicName).get();
    pubsub.createSubscription(PROJECT, subscriptionName, topicName).get();
    final List<Message> messages = ImmutableList.of(Message.ofEncoded("m0"),
                                                    Message.ofEncoded("m1"),
                                                    Message.ofEncoded("m2"));
    final List<String> ids = pubsub.publish(PROJECT, topicName, messages).get();
    final Map<String, ReceivedMessage> received = new HashMap<>();

    // Pull until we've received 3 messages or time out. Store received messages in a map as they might be out of order.
    final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (true) {
      final List<ReceivedMessage> response = pubsub.pull(PROJECT, subscriptionName).get();
      for (final ReceivedMessage message : response) {
        received.put(message.message().messageId().get(), message);
      }
      if (received.size() >= 3) {
        break;
      }
      if (System.nanoTime() > deadlineNanos) {
        fail("timeout");
      }
    }

    // Verify received messages
    assertThat(received.size(), is(3));
    for (int i = 0; i < 3; i++) {
      final String id = ids.get(i);
      final ReceivedMessage receivedMessage = received.get(id);
      assertThat(receivedMessage.message().data(), is(messages.get(i).data()));
      assertThat(receivedMessage.message().messageId().get(), is(id));
      assertThat(receivedMessage.ackId(), not(isEmptyOrNullString()));
    }
    final List<String> ackIds = received.values().stream()
        .map(ReceivedMessage::ackId)
        .collect(Collectors.toList());

    // Batch modify ack deadline
    pubsub.modifyAckDeadline(PROJECT, subscriptionName, 30, ackIds).get();

    // Batch ack the messages
    pubsub.acknowledge(PROJECT, subscriptionName, ackIds).get();
  }

  @Test
  public void testBestSpeedCompressionPublish() throws IOException, ExecutionException, InterruptedException {
    pubsub = Pubsub.builder()
        .maxConnections(CONCURRENCY)
        .credential(CREDENTIAL)
        .compressionLevel(BEST_SPEED)
        .build();
    pubsub.createTopic(PROJECT, topicName).get();
    final String data = BaseEncoding.base64().encode(Strings.repeat("hello world", 100).getBytes("UTF-8"));
    final Message message = new MessageBuilder().data(data).build();
    final PubsubFuture<List<String>> future = pubsub.publish(PROJECT, topicName, message);
    out.println("raw size: " + data.length());
    out.println("payload size: " + future.payloadSize());
  }

  @Test
  public void testEnabledCipherSuites() throws Exception {
    pubsub.close();

    final String[] defaultCiphers = SSLContext.getDefault().getDefaultSSLParameters().getCipherSuites();
    final List<String> nonGcmCiphers = Stream.of(defaultCiphers)
        .filter(cipher -> !cipher.contains("GCM"))
        .collect(Collectors.toList());

    pubsub = Pubsub.builder()
        .maxConnections(CONCURRENCY)
        .credential(CREDENTIAL)
        .enabledCipherSuites(nonGcmCiphers)
        .build();

    testCreateGetListDeleteTopics(pubsub);
  }

  @Test
  @Ignore
  public void listAllTopics() throws ExecutionException, InterruptedException {
    topics(pubsub).stream().map(Topic::name).forEach(System.out::println);
  }

  @Test
  @Ignore
  public void listAllSubscriptions() throws ExecutionException, InterruptedException {
    subscriptions(pubsub).stream().map(s -> s.name() + ", topic=" + s.topic()).forEach(System.out::println);
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
          .filter(t -> t.contains(TEST_NAME_PREFIX))
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

  @Test
  @Ignore
  public void cleanUpTestSubscriptions() throws ExecutionException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY / 2);
    Optional<String> pageToken = Optional.empty();
    while (true) {
      final SubscriptionList response = pubsub.listSubscriptions(PROJECT, pageToken.orElse(null)).get();
      response.subscriptions().stream()
          .map(Subscription::name)
          .filter(s -> s.contains(TEST_NAME_PREFIX))
          .map(s -> executor.submit(() -> {
            System.out.println("Removing subscription: " + s);
            return pubsub.deleteSubscription(s).get();
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
