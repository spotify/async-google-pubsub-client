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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.Credential.AccessMethod;
import com.google.common.io.BaseEncoding;

import com.ning.http.client.AsyncHttpClientConfig;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import okio.Buffer;

import static java.lang.System.out;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PubsubTest {

  private static final String PROJECT = "test-project";
  private static final String TOPIC = "test-topic";

  private static final URI URI = java.net.URI.create("https://test.pubsub.foo/v17/");

  private final Credential credential = new Credential.Builder(mock(AccessMethod.class)).build();

  private static final String ACCESS_TOKEN = "token";

  @Mock Service.Factory serviceFactory;
  @Mock Service service;

  @Captor ArgumentCaptor<String> pathCaptor;
  @Captor ArgumentCaptor<Object> payloadCaptor;
  @Captor ArgumentCaptor<Class<?>> classCaptor;

  private Pubsub pubsub;
  private AtomicReference<Object> response = new AtomicReference<>();

  @Before
  public void setUp() throws IOException, URISyntaxException {
    credential.setAccessToken(ACCESS_TOKEN);
    pubsub = Pubsub.builder()
        .credential(credential)
        .uri(URI)
        .accessToken(() -> ACCESS_TOKEN)
        .service((AsyncHttpClientConfig config, URI uri, Supplier<String> accessToken) -> service)
        .build();

    when(service.get(pathCaptor.capture(), classCaptor.capture()))
        .thenAnswer(args -> response.get());
    when(service.post(pathCaptor.capture(), payloadCaptor.capture(), classCaptor.capture()))
        .thenAnswer(args -> response.get());
    when(service.put(pathCaptor.capture(), payloadCaptor.capture(), classCaptor.capture()))
        .thenAnswer(args -> response.get());
    when(service.delete(pathCaptor.capture()))
        .thenAnswer(args -> response.get());
  }

  @Test
  public void testDefaults() {
    Pubsub.builder()
        .credential(credential)
        .uri(URI)
        .accessToken(() -> ACCESS_TOKEN)
        .service((AsyncHttpClientConfig config, URI uri, Supplier<String> accessToken) -> service)
        .build();
  }

  @Test
  public void testCreateTopic() throws Exception {
    final Topic expected = Topic.of(PROJECT, TOPIC);
    final Topic topic = pubsub.createTopic(PROJECT, TOPIC).get();
    assertThat(topic, is(expected));
  }

  private static void testCreateGetListDeleteTopics(final Pubsub pubsub) throws Exception {

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
      final List<Topic> topics = topics(pubsub);
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
      final List<Topic> topics = topics(pubsub);
      assertThat(topics, not(contains(expected)));
    }
  }

  private static List<Topic> topics(final Pubsub pubsub)
      throws ExecutionException, InterruptedException {
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
  public void testFailureRecovery()
      throws IOException, URISyntaxException, ExecutionException, InterruptedException {

    final Message m1 = Message.builder().data("1").build();
    final Message m2 = Message.builder().data("2").build();

    // Fail first request
    response.set(exceptionalFuture(new TimeoutException()));
    final CompletableFuture<List<String>> f1 = pubsub.publish("test", "t1", m1);

    try {
      f1.get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }

    // Verify that a subsequent request is successful
    response.set(completedFuture(PublishResponse.of("id2")));
    final CompletableFuture<List<String>> f2 = pubsub.publish("test", "t2", m2);
    final List<String> ids2 = f2.get();
    assertThat(ids2, contains("id2"));
  }

  private <T> CompletableFuture<T> exceptionalFuture(final Throwable ex) {
    final CompletableFuture<T> failure = new CompletableFuture<>();
    failure.completeExceptionally(ex);
    return failure;
  }

  private Buffer buffer(final byte[] bytes) {
    return new Buffer().write(bytes);
  }
}
