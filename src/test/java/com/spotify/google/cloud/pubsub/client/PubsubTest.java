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

import com.google.api.client.auth.oauth2.Credential;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import okio.Buffer;

import static com.google.common.net.HttpHeaders.ACCEPT_ENCODING;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONNECTION;
import static com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class PubsubTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  public static final String PROJECT = "test-project";
  public static final String TOPIC_1 = "topic-1";
  public static final String TOPIC_2 = "topic-2";
  public static final String SUBSCRIPTION_1 = "subscription-1";
  public static final String SUBSCRIPTION_2 = "subscription-2";

  public static final String BASE_PATH = "/v1/";

  private final Credential credential = new Credential.Builder(mock(Credential.AccessMethod.class)).build();

  private static final String ACCESS_TOKEN = "token";

  private final MockWebServer server = new MockWebServer();
  private URL baseUrl;

  private Pubsub pubsub;

  @Before
  public void setUp() throws Exception {
    server.start();
    baseUrl = server.getUrl(BASE_PATH);
    credential.setAccessToken(ACCESS_TOKEN);

    pubsub = Pubsub.builder()
        .uri(baseUrl.toURI())
        .credential(credential)
        .build();
  }

  @After
  public void tearDown() throws Exception {
    pubsub.close();
  }

  @Test
  public void testClose() throws Exception {
    assertThat(pubsub.closeFuture().isDone(), is(false));
    pubsub.close();
    pubsub.closeFuture().get(10, SECONDS);
  }

  @Test
  public void testListTopics() throws Exception {
    final PubsubFuture<TopicList> future = pubsub.listTopics(PROJECT);

    final String expectedPath = BASE_PATH + "projects/" + PROJECT + "/topics";

    assertThat(future.operation(), is("list topics"));
    assertThat(future.method(), is("GET"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("GET"));
    assertThat(request.getPath(), is(expectedPath));
    assertRequestHeaders(request);

    final TopicList response = TopicList.of(Topic.of(PROJECT, TOPIC_1), Topic.of(PROJECT, TOPIC_2));
    server.enqueue(new MockResponse().setBody(json(response)));

    final TopicList topicList = future.get(10, SECONDS);
    assertThat(topicList, is(response));
  }

  @Test
  public void testListTopicsWithPageToken() throws Exception {
    final String pageToken = "foo";
    final String nextPageToken = "foo";
    final PubsubFuture<TopicList> future = pubsub.listTopics(PROJECT, pageToken);

    final String expectedPath = BASE_PATH + "projects/" + PROJECT + "/topics?pageToken=" + pageToken;

    assertThat(future.operation(), is("list topics"));
    assertThat(future.method(), is("GET"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("GET"));
    assertThat(request.getPath(), is(expectedPath));
    assertRequestHeaders(request);

    final TopicList response = TopicList.builder()
        .nextPageToken(nextPageToken)
        .topics(Topic.of(PROJECT, TOPIC_1), Topic.of(PROJECT, TOPIC_2))
        .build();
    server.enqueue(new MockResponse().setBody(json(response)));

    final TopicList topicList = future.get(10, SECONDS);
    assertThat(topicList, is(response));
  }

  @Test
  public void testGetTopic() throws Exception {
    final PubsubFuture<Topic> future = pubsub.getTopic(PROJECT, TOPIC_1);

    final String expectedPath = BASE_PATH + "projects/" + PROJECT + "/topics/" + TOPIC_1;

    assertThat(future.operation(), is("get topic"));
    assertThat(future.method(), is("GET"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("GET"));
    assertThat(request.getPath(), is(expectedPath));
    assertRequestHeaders(request);

    final Topic response = Topic.of(PROJECT, TOPIC_1);
    server.enqueue(new MockResponse().setBody(json(response)));

    final Topic topic = future.get(10, SECONDS);
    assertThat(topic, is(response));
  }

  @Test
  public void testCreateTopic() throws Exception {
    final PubsubFuture<Topic> future = pubsub.createTopic(PROJECT, TOPIC_1);

    final String expectedPath = BASE_PATH + Topic.canonicalTopic(PROJECT, TOPIC_1);

    assertThat(future.operation(), is("create topic"));
    assertThat(future.method(), is("PUT"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("PUT"));
    assertThat(request.getPath(), is(expectedPath));
    assertThat(request.getHeader(CONTENT_LENGTH), is(String.valueOf(0)));
    assertRequestHeaders(request);

    final Topic response = Topic.of(PROJECT, TOPIC_1);
    server.enqueue(new MockResponse().setBody(json(response)));

    final Topic topic = future.get(10, SECONDS);
    assertThat(topic, is(response));
  }

  @Test
  public void testDeleteTopic() throws Exception {
    final PubsubFuture<Void> future = pubsub.deleteTopic(PROJECT, TOPIC_1);

    final String expectedPath = BASE_PATH + Topic.canonicalTopic(PROJECT, TOPIC_1);

    assertThat(future.operation(), is("delete topic"));
    assertThat(future.method(), is("DELETE"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("DELETE"));
    assertThat(request.getPath(), is(expectedPath));
    assertRequestHeaders(request);

    server.enqueue(new MockResponse());

    future.get(10, SECONDS);
  }

  @Test
  public void testListSubscriptions() throws Exception {
    final PubsubFuture<SubscriptionList> future = pubsub.listSubscriptions(PROJECT);

    final String expectedPath = BASE_PATH + "projects/" + PROJECT + "/subscriptions";

    assertThat(future.operation(), is("list subscriptions"));
    assertThat(future.method(), is("GET"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("GET"));
    assertThat(request.getPath(), is(expectedPath));
    assertRequestHeaders(request);

    final SubscriptionList response = SubscriptionList.of(Subscription.of(PROJECT, SUBSCRIPTION_1, TOPIC_1),
                                                          Subscription.of(PROJECT, SUBSCRIPTION_2, TOPIC_2));
    server.enqueue(new MockResponse().setBody(json(response)));

    final SubscriptionList subscriptionList = future.get(10, SECONDS);
    assertThat(subscriptionList, is(response));
  }

  @Test
  public void testListSubscriptionsWithPageToken() throws Exception {
    final String pageToken = "foo";
    final String nextPageToken = "foo";
    final PubsubFuture<SubscriptionList> future = pubsub.listSubscriptions(PROJECT, pageToken);

    final String expectedPath = BASE_PATH + "projects/" + PROJECT + "/subscriptions?pageToken=" + pageToken;

    assertThat(future.operation(), is("list subscriptions"));
    assertThat(future.method(), is("GET"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("GET"));
    assertThat(request.getPath(), is(expectedPath));
    assertRequestHeaders(request);

    final SubscriptionList response = SubscriptionList.builder()
        .nextPageToken(nextPageToken)
        .subscriptions(Subscription.of(PROJECT, SUBSCRIPTION_1, TOPIC_1),
                       Subscription.of(PROJECT, SUBSCRIPTION_2, TOPIC_2))
        .build();
    server.enqueue(new MockResponse().setBody(json(response)));

    final SubscriptionList subscriptionList = future.get(10, SECONDS);
    assertThat(subscriptionList, is(response));
  }

  @Test
  public void testGetSubscription() throws Exception {
    final PubsubFuture<Subscription> future = pubsub.getSubscription(PROJECT, SUBSCRIPTION_1);

    final String expectedPath = BASE_PATH + "projects/" + PROJECT + "/subscriptions/" + SUBSCRIPTION_1;

    assertThat(future.operation(), is("get subscription"));
    assertThat(future.method(), is("GET"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("GET"));
    assertThat(request.getPath(), is(expectedPath));
    assertRequestHeaders(request);

    final Subscription response = Subscription.of(PROJECT, SUBSCRIPTION_1, TOPIC_1);
    server.enqueue(new MockResponse().setBody(json(response)));

    final Subscription subscription = future.get(10, SECONDS);
    assertThat(subscription, is(response));
  }

  @Test
  public void testCreateSubscription() throws Exception {
    final PubsubFuture<Subscription> future = pubsub.createSubscription(PROJECT, SUBSCRIPTION_1, TOPIC_1);

    final String expectedPath = BASE_PATH + Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION_1);

    assertThat(future.operation(), is("create subscription"));
    assertThat(future.method(), is("PUT"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(greaterThan(0L)));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("PUT"));
    assertThat(request.getPath(), is(expectedPath));
    assertThat(request.getHeader(CONTENT_ENCODING), is("gzip"));
    assertThat(request.getHeader(CONTENT_LENGTH), is(String.valueOf(future.payloadSize())));
    assertThat(request.getHeader(CONTENT_TYPE), is("application/json; charset=UTF-8"));
    assertRequestHeaders(request);

    final Subscription response = Subscription.of(PROJECT, SUBSCRIPTION_1, TOPIC_1);
    server.enqueue(new MockResponse().setBody(json(response)));

    final Subscription subscription = future.get(10, SECONDS);
    assertThat(subscription, is(response));
  }

  @Test
  public void testDeleteSubscription() throws Exception {
    final PubsubFuture<Void> future = pubsub.deleteSubscription(PROJECT, SUBSCRIPTION_1);

    final String expectedPath = BASE_PATH + Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION_1);

    assertThat(future.operation(), is("delete subscription"));
    assertThat(future.method(), is("DELETE"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(0L));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("DELETE"));
    assertThat(request.getPath(), is(expectedPath));
    assertRequestHeaders(request);

    server.enqueue(new MockResponse());

    future.get(10, SECONDS);
  }

  @Test
  public void testPublishSingle() throws Exception {
    final Message[] messages = {Message.of("m0")};
    final String[] ids = {"id0"};
    testPublish(messages, ids);
  }

  @Test
  public void testPublishNonBase64ShouldFail() throws Exception {
    final Message badMessage = Message.of("foo-bar");
    exception.expect(IllegalArgumentException.class);
    pubsub.publish(PROJECT, TOPIC_1, badMessage);
  }

  @Test
  public void testPublishBatch() throws Exception {
    final Message[] messages = {Message.of("m0"), Message.of("m1"), Message.of("m2")};
    final String[] ids = {"id0", "id1", "id2"};
    testPublish(messages, ids);
  }

  private void testPublish(final Message[] messages, final String[] ids)
      throws InterruptedException, ExecutionException, TimeoutException {
    final PubsubFuture<List<String>> future = pubsub.publish(PROJECT, TOPIC_1, messages);

    final String expectedPath = BASE_PATH + Topic.canonicalTopic(PROJECT, TOPIC_1) + ":publish";

    assertThat(future.operation(), is("publish"));
    assertThat(future.method(), is("POST"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(greaterThan(0L)));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("POST"));
    assertThat(request.getPath(), is(expectedPath));
    assertThat(request.getHeader(CONTENT_ENCODING), is("gzip"));
    assertThat(request.getHeader(CONTENT_LENGTH), is(String.valueOf(future.payloadSize())));
    assertThat(request.getHeader(CONTENT_TYPE), is("application/json; charset=UTF-8"));

    assertRequestHeaders(request);

    final ImmutableMap<String, ImmutableList<String>> response = ImmutableMap.of(
        "messageIds", ImmutableList.copyOf(ids));
    server.enqueue(new MockResponse().setBody(json(response)));
    final List<String> messageIds = future.get(10, SECONDS);
    assertThat(messageIds, contains(ids));
  }

  @Test
  public void testPullEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    testPull();
  }

  @Test
  public void testPullSingle() throws InterruptedException, ExecutionException, TimeoutException {
    testPull(ReceivedMessage.ofEncoded("a0", "m0"));
  }

  @Test
  public void testPullBatch() throws InterruptedException, ExecutionException, TimeoutException {
    testPull(ReceivedMessage.ofEncoded("a0", "m0"),
             ReceivedMessage.ofEncoded("a1", "m1"),
             ReceivedMessage.ofEncoded("a2", "m2"));
  }

  private void testPull(final ReceivedMessage... messages)
      throws InterruptedException, ExecutionException, TimeoutException {
    final PubsubFuture<List<ReceivedMessage>> future = pubsub.pull(PROJECT, SUBSCRIPTION_1);

    final String expectedPath = BASE_PATH + Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION_1) + ":pull";

    assertThat(future.operation(), is("pull"));
    assertThat(future.method(), is("POST"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(greaterThan(0L)));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("POST"));
    assertThat(request.getPath(), is(expectedPath));
    assertThat(request.getHeader(CONTENT_ENCODING), is("gzip"));
    assertThat(request.getHeader(CONTENT_LENGTH), is(String.valueOf(future.payloadSize())));
    assertThat(request.getHeader(CONTENT_TYPE), is("application/json; charset=UTF-8"));
    assertThat(request.getHeader(ACCEPT_ENCODING), containsString("gzip"));

    assertRequestHeaders(request);

    final ImmutableMap<String, ImmutableList<ReceivedMessage>> response = ImmutableMap.of(
        "receivedMessages", ImmutableList.copyOf(messages));
    server.enqueue(new MockResponse().setBody(json(response)));
    final List<ReceivedMessage> receivedMessages = future.get(10, SECONDS);
    if (messages.length == 0) {
      assertThat(receivedMessages, is(empty()));
    } else {
      assertThat(receivedMessages, contains(messages));
    }
  }

  @Test
  public void testAcknowledgeSingle() throws InterruptedException, ExecutionException, TimeoutException {
    testAcknowledge("a0");
  }

  @Test
  public void testAcknowledgeBatch() throws InterruptedException, ExecutionException, TimeoutException {
    testAcknowledge("a0", "a1", "a2");
  }

  private void testAcknowledge(final String... ackIds)
      throws InterruptedException, ExecutionException, TimeoutException {
    final PubsubFuture<Void> future = pubsub.acknowledge(PROJECT, SUBSCRIPTION_1, ackIds);

    final String expectedPath =
        BASE_PATH + Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION_1) + ":acknowledge";

    assertThat(future.operation(), is("acknowledge"));
    assertThat(future.method(), is("POST"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(greaterThan(0L)));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("POST"));
    assertThat(request.getPath(), is(expectedPath));
    assertThat(request.getHeader(CONTENT_ENCODING), is("gzip"));
    assertThat(request.getHeader(CONTENT_LENGTH), is(String.valueOf(future.payloadSize())));
    assertThat(request.getHeader(CONTENT_TYPE), is("application/json; charset=UTF-8"));

    assertRequestHeaders(request);

    server.enqueue(new MockResponse());
    future.get(10, SECONDS);
  }

  @Test
  public void testModifyAckDeadlineSingle() throws Exception {
    testModifyAckDeadline(17, "a0");
  }

  @Test
  public void testModifyAckDeadlineBatch() throws Exception {
    testModifyAckDeadline(17, "a0", "a1", "a2");
  }

  private void testModifyAckDeadline(final int ackDeadlineSeconds, final String... ackIds) throws Exception {
    final PubsubFuture<Void> future = pubsub.modifyAckDeadline(PROJECT, SUBSCRIPTION_1, ackDeadlineSeconds, ackIds);

    final String expectedPath =
        BASE_PATH + Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION_1) + ":modifyAckDeadline";

    assertThat(future.operation(), is("modify ack deadline"));
    assertThat(future.method(), is("POST"));
    assertThat(future.uri(), is(server.getUrl(expectedPath).toString()));
    assertThat(future.payloadSize(), is(greaterThan(0L)));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("POST"));
    assertThat(request.getPath(), is(expectedPath));
    assertThat(request.getHeader(CONTENT_ENCODING), is("gzip"));
    assertThat(request.getHeader(CONTENT_LENGTH), is(String.valueOf(future.payloadSize())));
    assertThat(request.getHeader(CONTENT_TYPE), is("application/json; charset=UTF-8"));

    assertRequestHeaders(request);

    server.enqueue(new MockResponse());
    future.get(10, SECONDS);
  }

  @Test()
  public void testRequestFailure() throws Exception {
    final PubsubFuture<TopicList> future = pubsub.listTopics(PROJECT);
    server.enqueue(new MockResponse().setStatus("HTTP/1.1 500 ONOES"));
    final RequestFailedException failure = future.handle((v, ex) -> (RequestFailedException) ex).get();
    assertThat(failure, is(notNullValue()));
    assertThat(failure.statusCode(), is(500));
    assertThat(failure.statusMessage(), is("ONOES"));
  }

  @Test
  public void testPublishTimeoutFailureRecovery() throws Exception {

    pubsub = Pubsub.builder()
        .uri(baseUrl.toURI())
        .maxConnections(1)
        .credential(credential)
        .requestTimeout(10)
        .readTimeout(10)
        .build();

    final Message m1 = Message.of("1");
    final Message m2 = Message.of("2");

    // Time out first request
    final PubsubFuture<List<String>> f1 = pubsub.publish(PROJECT, "t1", m1);
    final TimeoutException timeout = f1.handle((v, ex) -> (TimeoutException) ex).get();
    assertThat(timeout, is(notNullValue()));

    server.enqueue(new MockResponse().setBody(buffer(Json.write(PublishResponse.of("id1")))));

    // Verify that a subsequent request is successful
    server.enqueue(new MockResponse().setBody(buffer(Json.write(PublishResponse.of("id2")))));
    final CompletableFuture<List<String>> f2 = pubsub.publish("test", "t2", m2);
    final List<String> ids2 = f2.get();
    assertThat(ids2, contains("id2"));
  }

  private void assertRequestHeaders(final RecordedRequest request) {
    assertThat(request.getHeader(USER_AGENT), anyOf(is("Spotify Google-HTTP-Java-Client/1.21.0 (gzip)"),
                                                    is("Spotify-Google-Pubsub-Java-Client/1.0.0 (gzip)")));
    assertThat(request.getHeader(AUTHORIZATION), is("Bearer " + ACCESS_TOKEN));
    assertThat(request.getHeader(ACCEPT_ENCODING), anyOf(is("gzip,deflate"),
                                                         is("gzip")));
    assertThat(request.getHeader(CONNECTION), is("keep-alive"));
  }

  private static Buffer json(final Object o) {
    return buffer(Json.write(o));
  }

  private static Buffer buffer(final byte[] bytes) {
    return new Buffer().write(bytes);
  }
}
