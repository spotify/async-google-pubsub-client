package com.spotify.google.cloud.pubsub.client;

import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.System.out;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class PublisherTest {


  private final String PROJECT = Util.defaultProject();

  private final String TOPIC = "test-topic-" + ThreadLocalRandom.current().nextLong();

  private Pubsub pubsub;
  private Publisher publisher;

  @Before
  public void setUp() throws ExecutionException, InterruptedException {
    pubsub = Pubsub.create();
    publisher = Publisher.builder()
        .pubsub(pubsub)
        .project(PROJECT)
        .build();
    pubsub.createTopic(PROJECT, TOPIC).get();
  }

  @After
  public void tearDown() throws ExecutionException, InterruptedException {
    if (publisher != null) {
      publisher.close();
    }
    if (pubsub != null) {
      pubsub.deleteTopic(PROJECT, TOPIC).exceptionally(t -> null).get();
      pubsub.close();
    }
  }

  @Test
  public void testPublish()
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    final String data = BaseEncoding.base64().encode("hello world".getBytes("UTF-8"));
    final Message message = Message.builder().data(data).build();

    final List<CompletableFuture<String>> futures = range(0, 10)
        .mapToObj(i -> publisher.publish(TOPIC, message))
        .collect(toList());

    futures.stream()
        .map(Futures::getUnchecked)
        .forEach(id -> out.println("message id: " + id));
  }

  @Ignore
  @Test
  public void benchPublish()
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    final String data = BaseEncoding.base64().encode("hello world".getBytes("UTF-8"));
    final Message message = Message.builder().data(data).build();
    final ProgressMeter meter = new ProgressMeter("messages", true);

    for (int i = 0; i < 5000; i++) {
      benchSend(publisher, message, meter);
    }

    while (true) {
      Thread.sleep(1000);
    }
  }

  private void benchSend(final Publisher publisher, final Message message,
                         final ProgressMeter meter) {
    final CompletableFuture<String> future = publisher.publish(TOPIC, message);
    final long start = System.nanoTime();
    future.whenComplete((s, ex) -> {
      if (ex != null) {
        ex.printStackTrace();
        return;
      }
      final long end = System.nanoTime();
      final long latency = end - start;
      meter.inc(1, latency);
      benchSend(publisher, message, meter);
    });
  }
}