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

package com.spotify.google.cloud.pubsub.client;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubsubFutureTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock RequestInfo requestInfo;

  @Test
  public void testToCompletableFutureErrorPropagation() throws Exception {
    final PubsubFuture<Void> f1 = new PubsubFuture<>(requestInfo);
    final PubsubFuture<Void> f2 = f1.toCompletableFuture();

    final Exception cause = new Exception();

    f1.fail(cause);

    assertThat(f2.isDone(), is(true));
    assertThat(f2.isCompletedExceptionally(), is(true));

    exception.expectCause(is(cause));

    f2.get();
  }

  @Test
  public void testToCompletableFutureResultPropagation() throws Exception {
    final PubsubFuture<String> f1 = new PubsubFuture<>(requestInfo);
    final PubsubFuture<String> f2 = f1.toCompletableFuture();

    final String value = "foo";

    f1.succeed(value);

    assertThat(f2.get(), is(value));
  }

  @Test
  public void testCompletionExceptionPropagation() throws Exception {
    final PubsubFuture<String> f1 = new PubsubFuture<>(requestInfo);
    final PubsubFuture<String> f2 = f1.thenApply(r -> r);

    final AtomicReference<Throwable> whenCompleteThrowable = new AtomicReference<>();
    f2.whenComplete((v, t) -> whenCompleteThrowable.set(t));

    final Exception cause = new Exception();
    f1.fail(cause);

    assertThat(f2.isDone(), is(true));
    assertThat(f2.isCompletedExceptionally(), is(true));

    assertThat(whenCompleteThrowable.get(), instanceOf(CompletionException.class));
    assertThat(whenCompleteThrowable.get().getCause(), is(cause));

    exception.expectCause(is(cause));

    f2.get();
  }
}
