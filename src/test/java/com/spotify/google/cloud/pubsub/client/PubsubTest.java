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
import org.junit.Test;
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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class PubsubTest {

  public static final String PROJECT = "test-project";
  public static final String TOPIC_1 = "topic-1";
  public static final String TOPIC_2 = "topic-2";

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

  public void assertRequestHeaders(final RecordedRequest request) {
    assertThat(request.getHeader(USER_AGENT), is("Spotify-Google-Pubsub-Java-Client/1.0.0 (gzip)"));
    assertThat(request.getHeader(AUTHORIZATION), is("Bearer " + ACCESS_TOKEN));
    assertThat(request.getHeader(ACCEPT_ENCODING), is("gzip,deflate"));
    assertThat(request.getHeader(CONNECTION), is("keep-alive"));
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
    assertThat(future.payloadSize(), is(greaterThan(0L)));

    final RecordedRequest request = server.takeRequest(10, SECONDS);

    assertThat(request.getMethod(), is("PUT"));
    assertThat(request.getPath(), is(expectedPath));
    assertThat(request.getHeader(CONTENT_ENCODING), is("gzip"));
    assertThat(request.getHeader(CONTENT_LENGTH), is(String.valueOf(future.payloadSize())));
    assertThat(request.getHeader(CONTENT_TYPE), is("application/json; charset=UTF-8"));
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
  public void testPublishSingle() throws Exception {
    final Message[] messages = {Message.of("m0")};
    final String[] ids = {"id0"};
    testPublish(messages, ids);
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

  private static Buffer json(final Object o) {
    return buffer(Json.write(o));
  }

  private static Buffer buffer(final byte[] bytes) {
    return new Buffer().write(bytes);
  }
}
