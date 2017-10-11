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

import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.collect.Iterables.concat;
import static com.spotify.google.cloud.pubsub.client.AssertWithTimeout.assertThatWithin;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PublisherTest {

  @Mock Pubsub pubsub;
  @Mock Publisher.Listener listener;

  @Captor ArgumentCaptor<PubsubFuture<List<String>>> batchFutureCaptor;

  private final ConcurrentMap<String, BlockingQueue<Request>> topics = new ConcurrentHashMap<>();

  private Publisher publisher;

  @Before
  public void setUp() {
    setUpPubsubClient();

    publisher = Publisher.builder()
        .project("test")
        .pubsub(pubsub)
        .listener(listener)
        .build();
  }

  @After
  public void tearDown() throws Exception {
    publisher.close();
  }

  @Test
  public void testConfigurationGetters() {
    final Publisher publisher = Publisher.builder()
        .pubsub(pubsub)
        .project("test")
        .concurrency(11)
        .batchSize(12)
        .queueSize(13)
        .build();

    assertThat(publisher.project(), is("test"));
    assertThat(publisher.concurrency(), is(11));
    assertThat(publisher.batchSize(), is(12));
    assertThat(publisher.queueSize(), is(13));
  }

  @Test
  public void testOutstandingRequests() throws InterruptedException, ExecutionException {
    final LinkedBlockingQueue<Request> t1 = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<Request> t2 = new LinkedBlockingQueue<>();
    topics.put("t1", t1);
    topics.put("t2", t2);

    final Message m1 = Message.of("1");
    final Message m2 = Message.of("2");

    // Verify that the outstanding requests before publishing anything is 0
    assertThat(publisher.outstandingRequests(), is(0));

    // Publish a message and verify that the outstanding request counter rises to 1
    final CompletableFuture<String> f1 = publisher.publish("t1", m1);
    assertThatWithin(5, SECONDS, publisher::outstandingRequests, is(1));

    // Publish another message and verify that the outstanding request counter rises to 2
    final CompletableFuture<String> f2 = publisher.publish("t2", m2);
    assertThatWithin(5, SECONDS, publisher::outstandingRequests, is(2));

    // Respond to the first request and verify that the outstanding request counter falls to 1
    t1.take().future.succeed(singletonList("id1"));
    assertThatWithin(5, SECONDS, publisher::outstandingRequests, is(1));

    // Respond to the second request and verify that the outstanding request counter falls to 0
    t2.take().future.succeed(singletonList("id2"));
    assertThatWithin(5, SECONDS, publisher::outstandingRequests, is(0));
  }

  @Test
  public void testLatencyBoundedBatchingSingleMessage() throws InterruptedException, ExecutionException {
    final LinkedBlockingQueue<Request> t = new LinkedBlockingQueue<>();
    topics.put("t", t);

    final Message m = Message.of("1");

    // Publish two messages
    publisher.publish("t", m);

    // Check that the publisher eventually times out gathering messages for the batch and sends the single message.
    final Request request = t.take();
    assertThat(request.messages.size(), is(1));
  }

  @Test
  public void testLatencyBoundedBatchingTwoMessages() throws InterruptedException, ExecutionException {
    final LinkedBlockingQueue<Request> t = new LinkedBlockingQueue<>();
    topics.put("t", t);

    final Message m1 = Message.of("1");
    final Message m2 = Message.of("2");

    // Publish two messages
    publisher.publish("t", m1);
    publisher.publish("t", m2);

    // Check that the publisher eventually times out gathering messages for the batch and sends just the two messages.
    final Request request = t.take();
    assertThat(request.messages.size(), is(2));
  }

  @Test
  public void testSizeBoundedBatching() throws InterruptedException, ExecutionException {
    final LinkedBlockingQueue<Request> t = new LinkedBlockingQueue<>();
    topics.put("t", t);

    publisher = Publisher.builder()
        .project("test")
        .pubsub(pubsub)
        .batchSize(2)
        .maxLatencyMs(DAYS.toMillis(1))
        .build();

    final Message m1 = Message.of("1");
    final Message m2 = Message.of("2");

    // Publish a single message
    publisher.publish("t", m1);

    // Verify that the batch is not sent
    Thread.sleep(1000);
    verify(pubsub, never()).publish(anyString(), anyString(), anyListOf(Message.class));

    // Send one more message, completing the batch.
    publisher.publish("t", m2);

    // Check that the batch got sent.
    verify(pubsub, timeout(5000)).publish(anyString(), anyString(), anyListOf(Message.class));
    final Request request = t.take();
    assertThat(request.messages.size(), is(2));
  }

  @Test
  public void testPendingTopics() throws InterruptedException, ExecutionException {
    final LinkedBlockingQueue<Request> t1 = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<Request> t2 = new LinkedBlockingQueue<>();
    topics.put("t1", t1);
    topics.put("t2", t2);

    publisher = Publisher.builder()
        .project("test")
        .pubsub(pubsub)
        .listener(listener)
        .concurrency(1)
        .maxLatencyMs(1)
        .build();

    final Message m1 = Message.of("1");
    final Message m2 = Message.of("2");

    // Verify that the pending topics before publishing anything is 0
    assertThat(publisher.pendingTopics(), is(0));

    // Publish a message and verify that the pending topics is still 0
    final CompletableFuture<String> f1 = publisher.publish("t1", m1);
    verify(pubsub, timeout(1000)).publish("test", "t1", singletonList(m1));
    assertThat(publisher.pendingTopics(), is(0));

    // Publish a message on a different topic and verify that the pending topics counter rises to 1
    publisher.publish("t2", m2);
    assertThatWithin(5, SECONDS, publisher::pendingTopics, is(1));

    // Respond to the first request and verify that the pending topics falls to 0
    t1.take().future.succeed(singletonList("id1"));
    assertThatWithin(5, SECONDS, publisher::pendingTopics, is(0));
  }

  @Test
  public void testListener() throws InterruptedException, ExecutionException {
    setUpPubsubClient();

    publisher = Publisher.builder()
        .project("test")
        .pubsub(pubsub)
        .listener(listener)
        .concurrency(1)
        .build();

    final LinkedBlockingQueue<Request> t1 = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<Request> t2 = new LinkedBlockingQueue<>();
    topics.put("t1", t1);
    topics.put("t2", t2);

    final Message m1 = Message.of("1");
    final Message m2a = Message.of("2a");
    final Message m2b = Message.of("2b");

    // Verify that the listener got called when the publisher was created
    verify(listener, timeout(5000)).publisherCreated(publisher);

    // Publish a message and verify that the listener got called
    final CompletableFuture<String> f1 = publisher.publish("t1", m1);
    verify(listener, timeout(5000)).publishingMessage(publisher, "t1", m1, f1);
    verify(listener, timeout(5000)).sendingBatch(
        eq(publisher), eq("t1"), eq(singletonList(m1)), batchFutureCaptor.capture());

    // Publish two messages on a different topic and verify that the listener got told that the topic is pending
    final CompletableFuture<String> f2a = publisher.publish("t2", m2a);
    final CompletableFuture<String> f2b = publisher.publish("t2", m2b);
    verify(listener, timeout(5000)).publishingMessage(publisher, "t2", m2a, f2a);
    verify(listener, timeout(5000)).publishingMessage(publisher, "t2", m2b, f2b);
    verify(listener, timeout(5000)).topicPending(publisher, "t2", 1, 1);

    // Respond to the first request and verify that the batch future is completed
    t1.take().future.succeed(singletonList("id1"));
    final List<String> batchIds1 = batchFutureCaptor.getValue().get();
    assertThat(batchIds1, contains("id1"));

    // verify that the listener got called for the second batch
    verify(listener, timeout(5000)).sendingBatch(
        eq(publisher), eq("t2"), eq(asList(m2a, m2b)), batchFutureCaptor.capture());

    // Respond to the second requests and verify that the batch future is completed
    t2.take().future.succeed(asList("id2a", "id2b"));
    final List<String> batchIds2 = batchFutureCaptor.getValue().get();
    assertThat(batchIds2, contains("id2a", "id2b"));

    // Close the publisher and verify that the listener got called
    publisher.close();
    verify(listener, timeout(5000)).publisherClosed(publisher);
  }

  @Test
  public void testListenerAdapter() throws Exception {
    final CompletableFuture<Void> created = new CompletableFuture<>();
    final CompletableFuture<Void> closed = new CompletableFuture<>();

    final Publisher.ListenerAdapter listener = new Publisher.ListenerAdapter() {
      @Override
      public void publisherCreated(final Publisher publisher) {
        created.complete(null);
      }

      @Override
      public void publisherClosed(final Publisher publisher) {
        closed.complete(null);
      }
    };

    publisher = Publisher.builder()
        .project("test")
        .pubsub(pubsub)
        .listener(listener)
        .concurrency(1)
        .build();

    assertThat(created.isDone(), is(true));

    publisher.close();
    publisher.closeFuture().get();

    assertThat(closed.isDone(), is(true));
  }

  @Test
  public void testQueueSize() throws InterruptedException, ExecutionException {
    final LinkedBlockingQueue<Request> t = new LinkedBlockingQueue<>();
    topics.put("t", t);

    final Message m0 = Message.of("0");
    final Message m1 = Message.of("1");
    final Message m2 = Message.of("2");

    publisher = Publisher.builder()
        .project("test")
        .pubsub(pubsub)
        .listener(listener)
        .concurrency(1)
        .queueSize(1)
        .build();

    // Send one message to saturate the concurrency (queue size == 0)
    final CompletableFuture<String> f0 = publisher.publish("t", m0);
    verify(pubsub, timeout(1000)).publish(eq("test"), eq("t"), eq(singletonList(m0)));

    // Send one message to occupy the queue (queue size == 1)
    final CompletableFuture<String> f1 = publisher.publish("t", m1);

    // Send a message that should fast fail due to the queue being full
    final CompletableFuture<String> f2 = publisher.publish("t", m2);
    assertThat(f2.isCompletedExceptionally(), is(true));
    assertThat(exception(f2), is(instanceOf(QueueFullException.class)));

    // Complete the first request
    t.take().future.succeed(singletonList("id0"));

    // Verify that the second one is sent and complete it
    verify(pubsub, timeout(1000)).publish(eq("test"), eq("t"), eq(singletonList(m1)));
    t.take().future.succeed(singletonList("id1"));

    // Verify that the fast-failed request was not sent
    Thread.sleep(1000);
    verify(pubsub, never()).publish(anyString(), anyString(), eq(singletonList(m2)));
  }

  @Test
  public void verifyConcurrentBacklogConsumption() throws Exception {
    final LinkedBlockingQueue<Request> t = new LinkedBlockingQueue<>();
    topics.put("t", t);

    publisher = Publisher.builder()
        .project("test")
        .pubsub(pubsub)
        .listener(listener)
        .concurrency(2)
        .batchSize(2)
        .queueSize(100)
        .build();

    // Saturate concurrency with two messages
    final Message m0a = Message.of("0a");
    final CompletableFuture<String> f0a = publisher.publish("t", m0a);
    final Request r0a = t.take();

    final Message m0b = Message.of("0b");
    final CompletableFuture<String> f0b = publisher.publish("t", m0b);
    final Request r0b = t.take();

    // Enqueue enough for at least two more batches
    final List<Message> m1 = range(0, 4).mapToObj(String::valueOf).map(Message::of).collect(toList());
    final List<CompletableFuture<String>> f1 = m1.stream().map(m -> publisher.publish("t", m)).collect(toList());

    // Complete the first two requests
    r0a.future.succeed(singletonList("0a"));
    r0b.future.succeed(singletonList("0b"));

    // Verify that two batches kicked off concurrently and that we got all four messages in the two batches
    final Request r1a = t.poll(30, SECONDS);
    final Request r1b = t.poll(30, SECONDS);
    assertThat(r1a, is(notNullValue()));
    assertThat(r1b, is(notNullValue()));
    final Set<Message> r1received = ImmutableSet.copyOf(concat(r1a.messages, r1b.messages));
    assertThat(r1received, is(ImmutableSet.copyOf(m1)));
  }

  private Throwable exception(final CompletableFuture<?> f) {
    if (!f.isCompletedExceptionally()) {
      throw new IllegalArgumentException();
    }
    try {
      f.get();
    } catch (InterruptedException e) {
      return e;
    } catch (ExecutionException e) {
      return e.getCause();
    }
    return null;
  }

  private void setUpPubsubClient() {
    reset(pubsub);
    when(pubsub.publish(anyString(), anyString(), anyListOf(Message.class)))
        .thenAnswer(invocation -> {
          final String topic = (String) invocation.getArguments()[1];
          @SuppressWarnings("unchecked") final List<Message> messages =
              (List<Message>) invocation.getArguments()[2];
          final RequestInfo requestInfo = RequestInfo.builder()
              .operation("publish")
              .method("POST")
              .uri("/publish")
              .payloadSize(4711)
              .build();
          final PubsubFuture<List<String>> future = new PubsubFuture<>(requestInfo);
          final BlockingQueue<Request> queue = topics.get(topic);
          queue.add(new Request(messages, future));
          return future;
        });
  }

  private static class Request {

    final List<Message> messages;
    final PubsubFuture<List<String>> future;

    Request(final List<Message> messages,
            final PubsubFuture<List<String>> future) {
      this.messages = messages;
      this.future = future;
    }
  }

}
