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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PublisherFailureTest {

  @Mock Pubsub pubsub;

  Publisher publisher;

  final ConcurrentMap<String, BlockingQueue<PubsubFuture<List<String>>>> topics = new ConcurrentHashMap<>();

  @Before
  public void setUp() {
    publisher = Publisher.builder()
        .pubsub(pubsub)
        .project("test")
        .batchSize(1)
        .concurrency(1)
        .build();

    final RequestInfo requestInfo = RequestInfo.builder()
        .operation("publish")
        .method("POST")
        .uri("/publish")
        .payloadSize(4711)
        .build();

    when(pubsub.publish(anyString(), anyString(), anyListOf(Message.class)))
        .thenAnswer(invocation -> {
          final String topic = (String) invocation.getArguments()[1];
          final PubsubFuture<List<String>> future = new PubsubFuture<>(requestInfo);
          final BlockingQueue<PubsubFuture<List<String>>> queue = topics.get(topic);
          queue.add(future);
          return future;
        });
  }

  @Test
  public void testTimeout() throws InterruptedException, ExecutionException {
    final LinkedBlockingQueue<PubsubFuture<List<String>>> t1 = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<PubsubFuture<List<String>>> t2 = new LinkedBlockingQueue<>();
    topics.put("t1", t1);
    topics.put("t2", t2);

    final Message m1 = Message.of("1");
    final Message m2 = Message.of("2");

    // Fail the first request
    final CompletableFuture<String> f1 = publisher.publish("t1", m1);
    final PubsubFuture<List<String>> r1 = t1.take();
    r1.fail(new TimeoutException());

    // Verify that the second request goes through
    final CompletableFuture<String> f2 = publisher.publish("t2", m2);
    final PubsubFuture<List<String>> r2 = t2.take();
    r2.succeed(singletonList("id2"));
    final String id2 = f2.get();
    assertThat(id2, is(r2.get().get(0)));
  }
}
