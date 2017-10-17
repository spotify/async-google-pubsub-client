package com.spotify.google.cloud.pubsub.client;

import static org.hamcrest.CoreMatchers.instanceOf;

import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PubsubFutureTest {

  @Rule
  public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void propagateFailure() throws Exception {
    PubsubFuture<String> pubsub = PubsubFuture.of(RequestInfo.builder().operation("op").method("method").uri("uri").build());

    exceptions.expectCause(instanceOf(RuntimeException.class));

    pubsub.fail(new RuntimeException("e"));

    pubsub.toCompletableFuture().get(1, TimeUnit.SECONDS);
  }
}
