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

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import okio.Buffer;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class PubsubFailureTest {

  private final Credential credential = new Credential.Builder(mock(Credential.AccessMethod.class)).build();

  private static final String ACCESS_TOKEN = "token";

  @Before
  public void setUp() {
    credential.setAccessToken(ACCESS_TOKEN);
  }

  @Test
  public void test() throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    final MockWebServer server = new MockWebServer();

    server.start();

    final URL baseUrl = server.getUrl("/v1/");

    final Pubsub pubsub = Pubsub.builder()
        .uri(baseUrl.toURI())
        .maxConnections(1)
        .credential(credential)
        .requestTimeout(10)
        .readTimeout(10)
        .build();

    final Message m1 = Message.builder().data("1").build();
    final Message m2 = Message.builder().data("2").build();

    // Fail first request
    final CompletableFuture<List<String>> f1 = pubsub.publish("test", "t1", m1);
    try {
      f1.get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
    server.enqueue(new MockResponse().setBody(buffer(Json.write(PublishResponse.of("id1")))));

    // Verify that a subsequent request is successful
    server.enqueue(new MockResponse().setBody(buffer(Json.write(PublishResponse.of("id2")))));
    final CompletableFuture<List<String>> f2 = pubsub.publish("test", "t2", m2);
    final List<String> ids2 = f2.get();
    assertThat(ids2, contains("id2"));
  }

  private Buffer buffer(final byte[] bytes) {
    return new Buffer().write(bytes);
  }
}
