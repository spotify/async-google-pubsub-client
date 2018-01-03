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

import static com.google.common.collect.Iterables.concat;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AckerTest {

  private static final String BASE_URI = "https://mock-pubsub/v1/";

  @Mock Pubsub pubsub;

  @Captor ArgumentCaptor<PubsubFuture<List<String>>> batchFutureCaptor;

  final BlockingQueue<Request> requestQueue = new LinkedBlockingQueue<>();

  private Acker acker;

  @Before
  public void setUp() {
    setUpPubsubClient();

    acker = Acker.builder()
        .project("test")
        .subscription("subscription")
        .pubsub(pubsub)
        .build();
  }

  @After
  public void tearDown() throws Exception {
    acker.close();
  }

  @Test
  public void testLatencyBoundedBatchingSingleMessage() throws InterruptedException, ExecutionException {
    // Ack a message
    acker.acknowledge("m1");

    // Check that the acker eventually times out gathering id's for the batch and sends the single id.
    final Request request = requestQueue.take();
    assertThat(request.ids.size(), is(1));
  }

  @Test
  public void testLatencyBoundedBatchingTwoMessages() throws InterruptedException, ExecutionException {
    // Ack two messages
    acker.acknowledge("m1");
    acker.acknowledge("m2");

    // Check that the acker eventually times out gathering id's for the batch and sends the two id's.
    final Request request = requestQueue.take();
    assertThat(request.ids, contains("m1", "m2"));
  }

  @Test
  public void testSizeBoundedBatching() throws InterruptedException, ExecutionException {
    acker = Acker.builder()
        .project("test")
        .subscription("subscription")
        .pubsub(pubsub)
        .batchSize(2)
        .maxLatencyMs(DAYS.toMillis(1))
        .build();

    // Ack a single message
    acker.acknowledge("m1");

    // Verify that the batch is not sent
    Thread.sleep(1000);
    verify(pubsub, never()).acknowledge(anyString(), anyString(), anyListOf(String.class));
    verify(pubsub, never()).acknowledge(anyString(), anyString(), anyListOf(String.class));
    verify(pubsub, never()).acknowledge(anyString(), anyString(), any(String[].class));

    // Ack one more message, completing the batch.
    acker.acknowledge("m2");

    // Check that the batch got sent.
    verify(pubsub, timeout(5000)).acknowledge(anyString(), anyString(), anyListOf(String.class));
    final Request request = requestQueue.take();
    assertThat(request.ids.size(), is(2));
  }

  @Test
  public void verifyConcurrentBacklogConsumption() throws Exception {

    acker = Acker.builder()
        .project("test")
        .subscription("subscription")
        .pubsub(pubsub)
        .concurrency(2)
        .batchSize(2)
        .queueSize(100)
        .build();

    // Saturate concurrency with two id's
    acker.acknowledge("a0");
    final Request ra0 = requestQueue.take();

    acker.acknowledge("a1");
    final Request ra1 = requestQueue.take();

    // Enqueue enough for at least two more batches
    final List<String> m1 = range(0, 4).mapToObj(i -> "b" + i).collect(toList());
    m1.forEach(acker::acknowledge);

    // Complete the first two requests
    ra0.future.succeed(null);
    ra1.future.succeed(null);

    // Verify that two batches kicked off concurrently and that we got all four messages in the two batches
    final Request r1a = requestQueue.poll(30, TimeUnit.SECONDS);
    final Request r1b = requestQueue.poll(30, TimeUnit.SECONDS);
    assertThat(r1a, is(notNullValue()));
    assertThat(r1b, is(notNullValue()));
    final Set<String> r1received = ImmutableSet.copyOf(concat(r1a.ids, r1b.ids));
    assertThat(r1received, is(ImmutableSet.copyOf(m1)));
  }

  @Test
  public void verifyCloseWillClosePubsubClient() throws Exception {
    acker = Acker.builder()
        .project("test")
        .subscription("subscription")
        .pubsub(pubsub)
        .build();
    acker.close();
    verify(pubsub).close();
  }

  private void setUpPubsubClient() {
    reset(pubsub);
    when(pubsub.acknowledge(anyString(), anyString(), anyListOf(String.class)))
        .thenAnswer(invocation -> {
          final String project = invocation.getArgumentAt(0, String.class);
          final String subscription = invocation.getArgumentAt(1, String.class);
          @SuppressWarnings("unchecked") final List<String> ackIds =
              (List<String>) invocation.getArgumentAt(2, List.class);
          final String canonicalSubscription = Subscription.canonicalSubscription(project, subscription);
          final String uri = BASE_URI + canonicalSubscription + ":acknowledge";
          final RequestInfo requestInfo = RequestInfo.builder()
              .operation("acknowledge")
              .method("POST")
              .uri(uri)
              .payloadSize(4711)
              .build();
          final PubsubFuture<Void> future = new PubsubFuture<>(requestInfo);
          requestQueue.add(new Request(ackIds, future));
          return future;
        });
  }

  private static class Request {

    final List<String> ids;
    final PubsubFuture<Void> future;

    Request(final List<String> ids,
            final PubsubFuture<Void> future) {
      this.ids = ids;
      this.future = future;
    }
  }

}
