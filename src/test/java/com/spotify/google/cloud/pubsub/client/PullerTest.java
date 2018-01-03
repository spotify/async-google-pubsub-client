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
 * Copyright (c) 2011-2016 Spotify AB
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

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullerTest {

  private static final String BASE_URI = "https://mock-pubsub/v1/";
  private static final String SUBSCRIPTION = "test-subscription";
  private static final String PROJECT = "test-project";

  @Mock Pubsub pubsub;

  @Mock Puller.MessageHandler handler;

  @Captor ArgumentCaptor<PubsubFuture<List<String>>> batchFutureCaptor;

  final BlockingQueue<Request> requestQueue = new LinkedBlockingQueue<>();

  private Puller puller;

  @Before
  public void setUp() {
    setUpPubsubClient();
  }

  @After
  public void tearDown() throws Exception {
    puller.close();
  }

  @Test
  public void testConfigurationGetters() throws Exception {
    puller = Puller.builder()
        .project(PROJECT)
        .subscription(SUBSCRIPTION)
        .pubsub(pubsub)
        .messageHandler(handler)
        .concurrency(3)
        .maxOutstandingMessages(4)
        .batchSize(5)
        .maxAckQueueSize(10)
        .pullIntervalMillis(1000)
        .build();

    assertThat(puller.maxAckQueueSize(), is(10));
    assertThat(puller.concurrency(), is(3));
    assertThat(puller.maxOutstandingMessages(), is(4));
    assertThat(puller.batchSize(), is(5));
    assertThat(puller.subscription(), is(SUBSCRIPTION));
    assertThat(puller.project(), is(PROJECT));
    assertThat(puller.pullIntervalMillis(), is(1000L));
  }

  @Test
  public void testPulling() throws Exception {
    puller = Puller.builder()
        .project(PROJECT)
        .subscription(SUBSCRIPTION)
        .pubsub(pubsub)
        .messageHandler(handler)
        .concurrency(2)
        .build();

    // Immediately handle all messages
    when(handler.handleMessage(any(Puller.class), any(String.class), any(Message.class), anyString()))
        .thenAnswer(invocation -> {
          String ackId = invocation.getArgumentAt(3, String.class);
          return CompletableFuture.completedFuture(ackId);
        });

    final Request r1 = requestQueue.take();
    final Request r2 = requestQueue.take();

    assertThat(puller.outstandingRequests(), is(2));

    // Verify that concurrency limit is not exceeded
    final Request unexpected = requestQueue.poll(1, SECONDS);
    assertThat(unexpected, is(nullValue()));

    // Complete first request
    final List<ReceivedMessage> b1 = asList(ReceivedMessage.of("i1", "m1"),
                                            ReceivedMessage.of("i2", "m2"));
    r1.future.succeed(b1);
    verify(handler, timeout(1000)).handleMessage(puller, SUBSCRIPTION, b1.get(0).message(), b1.get(0).ackId());
    verify(handler, timeout(1000)).handleMessage(puller, SUBSCRIPTION, b1.get(1).message(), b1.get(1).ackId());

    // Verify that another request is made
    final Request r3 = requestQueue.take();

    assertThat(puller.outstandingRequests(), is(2));

    // Complete second request
    final List<ReceivedMessage> b2 = asList(ReceivedMessage.of("i3", "m3"),
                                            ReceivedMessage.of("i4", "m4"));
    r2.future.succeed(b2);
    verify(handler, timeout(1000)).handleMessage(puller, SUBSCRIPTION, b2.get(0).message(), b2.get(0).ackId());
    verify(handler, timeout(1000)).handleMessage(puller, SUBSCRIPTION, b2.get(1).message(), b2.get(1).ackId());
  }

  @Test
  public void testMaxOutstandingMessagesLimit() throws Exception {
    puller = Puller.builder()
        .project(PROJECT)
        .subscription(SUBSCRIPTION)
        .pubsub(pubsub)
        .messageHandler(handler)
        .maxOutstandingMessages(3)
        .concurrency(2)
        .build();

    final BlockingQueue<CompletableFuture<Void>> futures = new LinkedBlockingQueue<>();

    when(handler.handleMessage(any(Puller.class), any(String.class), any(Message.class), anyString()))
        .thenAnswer(invocation -> {
          final CompletableFuture<Void> f = new CompletableFuture<>();
          futures.add(f);
          final String ackId = invocation.getArgumentAt(3, String.class);
          return f.thenApply(ignore -> ackId);
        });

    final Request r1 = requestQueue.take();
    final Request r2 = requestQueue.take();

    assertThat(puller.outstandingRequests(), is(2));

    // Complete requests without immediately handling messages
    final List<ReceivedMessage> b1 = asList(ReceivedMessage.of("i1", "m1"),
                                            ReceivedMessage.of("i2", "m2"),
                                            ReceivedMessage.of("i3", "m3"),
                                            ReceivedMessage.of("i4", "m4"));
    r1.future.succeed(b1);

    // Verify that no new request are made until messages are handled
    assertThat(requestQueue.poll(1, SECONDS), is(nullValue()));
    assertThat(puller.outstandingRequests(), is(1));
    assertThat(puller.outstandingMessages(), is(4));

    // Complete handling of a single message and verify no pull is made
    futures.take().complete(null);
    assertThat(requestQueue.poll(1, SECONDS), is(nullValue()));
    assertThat(puller.outstandingRequests(), is(1));
    assertThat(puller.outstandingMessages(), is(3));

    // Complete handling of another message and verify that a pull request is made
    futures.take().complete(null);
    final Request r3 = requestQueue.take();
    assertThat(puller.outstandingRequests(), is(2));
    assertThat(puller.outstandingMessages(), is(2));
  }

  @Test
  public void verifyCloseWillClosePubsubClient() throws Exception {
    puller = Puller.builder()
        .project(PROJECT)
        .subscription(SUBSCRIPTION)
        .pubsub(pubsub)
        .messageHandler(handler)
        .build();
    puller.close();
    verify(pubsub).close();
  }

  private void setUpPubsubClient() {
    reset(pubsub);
    when(pubsub.pull(anyString(), anyString(), anyBoolean(), anyInt()))
        .thenAnswer(invocation -> {
          final String project = invocation.getArgumentAt(0, String.class);
          final String subscription = invocation.getArgumentAt(1, String.class);
          final boolean returnImmediately = invocation.getArgumentAt(2, Boolean.class);
          final int maxMessages = invocation.getArgumentAt(3, Integer.class);
          final String canonicalSubscription = Subscription.canonicalSubscription(project, subscription);
          final String uri = BASE_URI + canonicalSubscription + ":pull";
          final RequestInfo requestInfo = RequestInfo.builder()
              .operation("pull")
              .method("POST")
              .uri(uri)
              .payloadSize(4711)
              .build();
          final PubsubFuture<List<ReceivedMessage>> future = new PubsubFuture<>(requestInfo);
          requestQueue.add(new Request(future));
          return future;
        });
  }

  private static class Request {

    final PubsubFuture<List<ReceivedMessage>> future;

    Request(final PubsubFuture<List<ReceivedMessage>> future) {
      this.future = future;
    }
  }

}
