package com.spotify.google.cloud.pubsub.client;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.common.io.BaseEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static com.spotify.google.cloud.pubsub.client.Util.TEST_TOPIC_PREFIX;
import static java.lang.System.out;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class PubsubTest {

  private final String PROJECT = Util.defaultProject();

  private final String TOPIC = TEST_TOPIC_PREFIX + ThreadLocalRandom.current().nextLong();

  private static GoogleCredential CREDENTIAL;

  private Pubsub pubsub;

  @BeforeClass
  public static void setUpCredentials() throws IOException {
    CREDENTIAL = GoogleCredential.getApplicationDefault(
        Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());
  }

  @Before
  public void setUp() {
    pubsub = Pubsub.builder()
        .credential(CREDENTIAL)
        .build();
  }

  @After
  public void tearDown() throws ExecutionException, InterruptedException {
    if (pubsub != null) {
      pubsub.deleteTopic(PROJECT, TOPIC).exceptionally(t -> null).get();
      pubsub.close();
    }
  }

  @Test
  public void testCreateGetListDeleteTopics()
      throws IOException, ExecutionException, InterruptedException {
    // Create topic
    final Topic expected = new TopicBuilder()
        .name("projects/" + PROJECT + "/topics/" + TOPIC)
        .build();
    {
      final Topic topic = pubsub.createTopic(PROJECT, TOPIC).get();
      assertThat(topic, is(expected));
    }

    // Get topic
    {
      final Topic topic = pubsub.getTopic(PROJECT, TOPIC).get();
      assertThat(topic, is(expected));
    }

    // Verify that the topic is listed
    {
      final List<Topic> topics = topics();
      assertThat(topics, hasItem(expected));
    }

    // Delete topic
    {
      pubsub.deleteTopic(PROJECT, TOPIC).get();
    }

    // Verify that topic is gone
    {
      final Topic topic = pubsub.getTopic(PROJECT, TOPIC).get();
      assertThat(topic, is(nullValue()));
    }
    {
      final List<Topic> topics = topics();
      assertThat(topics, not(contains(expected)));
    }
  }

  private List<Topic> topics() throws ExecutionException, InterruptedException {
    final List<Topic> topics = new ArrayList<>();
    Optional<String> pageToken = Optional.empty();
    while (true) {
      final TopicList response = pubsub.listTopics(PROJECT, pageToken.orElse(null)).get();
      topics.addAll(response.topics());
      pageToken = response.nextPageToken();
      if (!pageToken.isPresent()) {
        break;
      }
    }
    return topics;
  }

  @Test
  public void testPublish() throws IOException, ExecutionException, InterruptedException {
    final String data = BaseEncoding.base64().encode("hello world".getBytes("UTF-8"));
    final PublishRequest req = new PublishRequestBuilder()
        .messages(new MessageBuilder().data(data).build())
        .build();
    final PublishResponse response = pubsub.publish(PROJECT, "dano-test", req).get();
    out.println(response);
  }

  @Test
  @Ignore
  public void cleanUpTestTopics() throws ExecutionException, InterruptedException {
    Optional<String> pageToken = Optional.empty();
    while (true) {
      final TopicList response = pubsub.listTopics(PROJECT, pageToken.orElse(null)).get();
      response.topics().stream()
          .map(Topic::name)
          .filter(t -> t.startsWith(TEST_TOPIC_PREFIX))
          .forEach(t -> pubsub.deleteTopic(PROJECT, t));
      pageToken = response.nextPageToken();
      if (!pageToken.isPresent()) {
        break;
      }
    }
  }
}