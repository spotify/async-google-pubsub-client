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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.getExitingScheduledExecutorService;
import static com.spotify.google.cloud.pubsub.client.Message.isEncoded;
import static com.spotify.google.cloud.pubsub.client.Pubsub.ResponseReader.VOID;
import static com.spotify.google.cloud.pubsub.client.Subscription.canonicalSubscription;
import static com.spotify.google.cloud.pubsub.client.Subscription.validateCanonicalSubscription;
import static com.spotify.google.cloud.pubsub.client.Topic.canonicalTopic;
import static com.spotify.google.cloud.pubsub.client.Topic.validateCanonicalTopic;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.GZIP;
import static org.jboss.netty.handler.codec.http.HttpMethod.POST;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.SSLSocketFactory;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An async low-level Google Cloud Pub/Sub client.
 */
public class Pubsub implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Pubsub.class);

  private static final String VERSION = "1.0.0";
  private static final String USER_AGENT =
      "Spotify-Google-Pubsub-Java-Client/" + VERSION + " (gzip)";

  private static final Object NO_PAYLOAD = new Object();

  private static final String CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform";
  private static final String PUBSUB = "https://www.googleapis.com/auth/pubsub";
  private static final List<String> SCOPES = ImmutableList.of(CLOUD_PLATFORM, PUBSUB);
  private static final String APPLICATION_JSON_UTF8 = "application/json; charset=UTF-8";

  private static final int DEFAULT_PULL_MAX_MESSAGES = 1000;
  private static final boolean DEFAULT_PULL_RETURN_IMMEDIATELY = true;

  private final AsyncHttpClient client;
  private final String baseUri;
  private final Credential credential;
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

  private final ScheduledExecutorService scheduler = getExitingScheduledExecutorService(
      new ScheduledThreadPoolExecutor(1));

  private final ExecutorService executor = getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newCachedThreadPool());

  private volatile String accessToken;
  private final int compressionLevel;
  private NetHttpTransport transport;

  private Pubsub(final Builder builder) {
    final AsyncHttpClientConfig config = builder.clientConfig.build();

    log.debug("creating new pubsub client with config:");
    log.debug("uri: {}", builder.uri);
    log.debug("connect timeout: {}", config.getConnectTimeout());
    log.debug("read timeout: {}", config.getReadTimeout());
    log.debug("request timeout: {}", config.getRequestTimeout());
    log.debug("max connections: {}", config.getMaxConnections());
    log.debug("max connections per host: {}", config.getMaxConnectionsPerHost());
    log.debug("enabled cipher suites: {}", Arrays.toString(config.getEnabledCipherSuites()));
    log.debug("response compression enforced: {}", config.isCompressionEnforced());
    log.debug("request compression level: {}", builder.compressionLevel);
    log.debug("accept any certificate: {}", config.isAcceptAnyCertificate());
    log.debug("follows redirect: {}", config.isFollowRedirect());
    log.debug("pooled connection TTL: {}", config.getConnectionTTL());
    log.debug("pooled connection idle timeout: {}", config.getPooledConnectionIdleTimeout());
    log.debug("pooling connections: {}", config.isAllowPoolingConnections());
    log.debug("pooling SSL connections: {}", config.isAllowPoolingSslConnections());
    log.debug("user agent: {}", config.getUserAgent());
    log.debug("max request retry: {}", config.getMaxRequestRetry());

    final SSLSocketFactory sslSocketFactory =
        new ConfigurableSSLSocketFactory(config.getEnabledCipherSuites(),
            (SSLSocketFactory) SSLSocketFactory.getDefault());

    this.transport = new NetHttpTransport.Builder()
        .setSslSocketFactory(sslSocketFactory)
        .build();

    this.client = new AsyncHttpClient(config);

    this.compressionLevel = builder.compressionLevel;

    if (builder.credential == null) {
      this.credential = scoped(defaultCredential());
    } else {
      this.credential = scoped(builder.credential);
    }

    this.baseUri = builder.uri.toString();

    // Get initial access token
    refreshAccessToken();
    if (accessToken == null) {
      throw new RuntimeException("Failed to get access token");
    }

    // Wake up every 10 seconds to check if access token has expired
    scheduler.scheduleAtFixedRate(this::refreshAccessToken, 10, 10, SECONDS);
  }

  private Credential scoped(final Credential credential) {
    if (credential instanceof GoogleCredential) {
      return scoped((GoogleCredential) credential);
    }
    return credential;
  }

  private Credential scoped(final GoogleCredential credential) {
    if (credential.createScopedRequired()) {
      return credential.createScoped(SCOPES);
    }
    return credential;
  }

  private static Credential defaultCredential() {
    try {
      return GoogleCredential.getApplicationDefault(
          Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Close this {@link Pubsub} client.
   */
  @Override
  public void close() {
    executor.shutdown();
    scheduler.shutdown();
    client.close();
    closeFuture.complete(null);
  }

  /**
   * Get a future that is completed when this {@link Pubsub} client is closed.
   */
  public CompletableFuture<Void> closeFuture() {
    return closeFuture;
  }

  /**
   * Refresh the Google Cloud API access token, if necessary.
   */
  private void refreshAccessToken() {
    final Long expiresIn = credential.getExpiresInSeconds();

    // trigger refresh if token is about to expire
    String accessToken = credential.getAccessToken();
    if (accessToken == null || expiresIn != null && expiresIn <= 60) {
      try {
        credential.refreshToken();
        accessToken = credential.getAccessToken();
      } catch (final IOException e) {
        log.error("Failed to fetch access token", e);
      }
    }
    if (accessToken != null) {
      this.accessToken = accessToken;
    }
  }

  /**
   * List the Pub/Sub topics in a project. This will get the first page of topics. To enumerate all topics you might
   * have to make further calls to {@link #listTopics(String, String)} with the page token in order to get further
   * pages.
   *
   * @param project The Google Cloud project.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<TopicList> listTopics(final String project) {
    final String path = "projects/" + project + "/topics";
    return get("list topics", path, readJson(TopicList.class));
  }

  /**
   * Get a page of Pub/Sub topics in a project using a specified page token.
   *
   * @param project   The Google Cloud project.
   * @param pageToken A token for the page of topics to get.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<TopicList> listTopics(final String project,
                                            final String pageToken) {
    final String query = (pageToken == null) ? "" : "?pageToken=" + pageToken;
    final String path = "projects/" + project + "/topics" + query;
    return get("list topics", path, readJson(TopicList.class));
  }

  /**
   * Create a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to create.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Topic> createTopic(final String project,
                                         final String topic) {
    return createTopic(canonicalTopic(project, topic));
  }

  /**
   * Create a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to create.
   * @return A future that is completed when this request is completed.
   */
  private PubsubFuture<Topic> createTopic(final String canonicalTopic) {
    validateCanonicalTopic(canonicalTopic);
    return put("create topic", canonicalTopic, NO_PAYLOAD, readJson(Topic.class));
  }

  /**
   * Get a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to get.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public PubsubFuture<Topic> getTopic(final String project, final String topic) {
    return getTopic(canonicalTopic(project, topic));
  }

  /**
   * Get a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to get.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public PubsubFuture<Topic> getTopic(final String canonicalTopic) {
    validateCanonicalTopic(canonicalTopic);
    return get("get topic", canonicalTopic, readJson(Topic.class));
  }

  /**
   * Delete a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to delete.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public PubsubFuture<Void> deleteTopic(final String project,
                                        final String topic) {
    return deleteTopic(canonicalTopic(project, topic));
  }

  /**
   * Delete a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to delete.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public PubsubFuture<Void> deleteTopic(final String canonicalTopic) {
    validateCanonicalTopic(canonicalTopic);
    return delete("delete topic", canonicalTopic, VOID);
  }

  /**
   * Create a Pub/Sub subscription.
   *
   * @param project          The Google Cloud project.
   * @param subscriptionName The name of the subscription to create.
   * @param topic            The name of the topic to subscribe to.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Subscription> createSubscription(final String project,
                                                       final String subscriptionName,
                                                       final String topic) {
    return createSubscription(canonicalSubscription(project, subscriptionName), canonicalTopic(project, topic));
  }

  /**
   * List the Pub/Sub subscriptions in a project. This will get the first page of subscriptions. To enumerate all
   * subscriptions you might have to make further calls to {@link #listTopics(String, String)} with the page token in
   * order to get further pages.
   *
   * @param project The Google Cloud project.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<SubscriptionList> listSubscriptions(final String project) {
    final String path = "projects/" + project + "/subscriptions";
    return get("list subscriptions", path, readJson(SubscriptionList.class));
  }

  /**
   * Get a page of Pub/Sub subscriptions in a project using a specified page token.
   *
   * @param project   The Google Cloud project.
   * @param pageToken A token for the page of subscriptions to get.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<SubscriptionList> listSubscriptions(final String project,
                                                          final String pageToken) {
    final String query = (pageToken == null) ? "" : "?pageToken=" + pageToken;
    final String path = "projects/" + project + "/subscriptions" + query;
    return get("list subscriptions", path, readJson(SubscriptionList.class));
  }

  /**
   * Create a Pub/Sub subscription.
   *
   * @param canonicalSubscriptionName The canonical (including project) name of the scubscription to create.
   * @param canonicalTopic            The canonical (including project) name of the topic to subscribe to.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Subscription> createSubscription(final String canonicalSubscriptionName,
                                                       final String canonicalTopic) {
    return createSubscription(Subscription.of(canonicalSubscriptionName, canonicalTopic));
  }

  /**
   * Create a Pub/Sub subscription.
   *
   * @param subscription The subscription to create.
   * @return A future that is completed when this request is completed.
   */
  private PubsubFuture<Subscription> createSubscription(final Subscription subscription) {
    return createSubscription(subscription.name(), subscription);
  }

  /**
   * Create a Pub/Sub subscription.
   *
   * @param canonicalSubscriptionName The canonical (including project) name of the subscription to create.
   * @param subscription              The subscription to create.
   * @return A future that is completed when this request is completed.
   */
  private PubsubFuture<Subscription> createSubscription(final String canonicalSubscriptionName,
                                                        final Subscription subscription) {
    validateCanonicalSubscription(canonicalSubscriptionName);
    return put("create subscription", canonicalSubscriptionName, SubscriptionCreateRequest.of(subscription),
        readJson(Subscription.class));
  }

  /**
   * Get a Pub/Sub subscription.
   *
   * @param project      The Google Cloud project.
   * @param subscription The name of the subscription to get.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public PubsubFuture<Subscription> getSubscription(final String project, final String subscription) {
    return getSubscription(canonicalSubscription(project, subscription));
  }

  /**
   * Get a Pub/Sub subscription.
   *
   * @param canonicalSubscriptionName The canonical (including project) name of the subscription to get.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public PubsubFuture<Subscription> getSubscription(final String canonicalSubscriptionName) {
    validateCanonicalSubscription(canonicalSubscriptionName);
    return get("get subscription", canonicalSubscriptionName, readJson(Subscription.class));
  }

  /**
   * Delete a Pub/Sub subscription.
   *
   * @param project      The Google Cloud project.
   * @param subscription The name of the subscription to delete.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public PubsubFuture<Void> deleteSubscription(final String project,
                                               final String subscription) {
    return deleteSubscription(canonicalSubscription(project, subscription));
  }

  /**
   * Delete a Pub/Sub subscription.
   *
   * @param canonicalSubscriptionName The canonical (including project) name of the subscription to delete.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public PubsubFuture<Void> deleteSubscription(final String canonicalSubscriptionName) {
    validateCanonicalSubscription(canonicalSubscriptionName);
    return delete("delete subscription", canonicalSubscriptionName, VOID);
  }

  /**
   * Publish a batch of messages.
   *
   * @param project  The Google Cloud project.
   * @param topic    The topic to publish on.
   * @param messages The batch of messages.
   * @return a future that is completed with a list of message ID's for the published messages.
   */
  public PubsubFuture<List<String>> publish(final String project, final String topic,
                                            final Message... messages) {
    return publish(project, topic, asList(messages));
  }

  /**
   * Publish a batch of messages.
   *
   * @param project  The Google Cloud project.
   * @param topic    The topic to publish on.
   * @param messages The batch of messages.
   * @return a future that is completed with a list of message ID's for the published messages.
   */
  public PubsubFuture<List<String>> publish(final String project, final String topic,
                                            final List<Message> messages) {
    return publish0(messages, Topic.canonicalTopic(project, topic));
  }

  /**
   * Publish a batch of messages.
   *
   * @param messages       The batch of messages.
   * @param canonicalTopic The canonical topic to publish on.
   * @return a future that is completed with a list of message ID's for the published messages.
   */
  public PubsubFuture<List<String>> publish(final List<Message> messages, final String canonicalTopic) {
    Topic.validateCanonicalTopic(canonicalTopic);
    return publish0(messages, canonicalTopic);
  }

  /**
   * Publish a batch of messages.
   *
   * @param messages       The batch of messages.
   * @param canonicalTopic The canonical topic to publish on.
   * @return a future that is completed with a list of message ID's for the published messages.
   */
  private PubsubFuture<List<String>> publish0(final List<Message> messages, final String canonicalTopic) {
    final String path = canonicalTopic + ":publish";
    for (final Message message : messages) {
      if (!isEncoded(message)) {
        throw new IllegalArgumentException("Message data must be Base64 encoded: " + message);
      }
    }
    return post("publish", path, PublishRequest.of(messages), readJson(PublishResponse.class)
        .andThen(PublishResponse::messageIds));
  }

  /**
   * Pull a batch of messages.
   *
   * @param project      The Google Cloud project.
   * @param subscription The subscription to pull from.
   * @return a future that is completed with a list of received messages.
   */
  public PubsubFuture<List<ReceivedMessage>> pull(final String project, final String subscription) {
    return pull(project, subscription, DEFAULT_PULL_RETURN_IMMEDIATELY, DEFAULT_PULL_MAX_MESSAGES);
  }

  /**
   * Pull a batch of messages.
   *
   * @param project           The Google Cloud project.
   * @param subscription      The subscription to pull from.
   * @param returnImmediately {@code true} to return immediately if the queue is empty. {@code false} to wait for at
   *                          least one message before returning.
   * @return a future that is completed with a list of received messages.
   */
  public PubsubFuture<List<ReceivedMessage>> pull(final String project, final String subscription,
                                                  final boolean returnImmediately) {
    return pull(project, subscription, returnImmediately, DEFAULT_PULL_MAX_MESSAGES);
  }

  /**
   * Pull a batch of messages.
   *
   * @param project           The Google Cloud project.
   * @param subscription      The subscription to pull from.
   * @param returnImmediately {@code true} to return immediately if the queue is empty. {@code false} to wait for at
   *                          least one message before returning.
   * @param maxMessages       Maximum number of messages to return in batch.
   * @return a future that is completed with a list of received messages.
   */
  public PubsubFuture<List<ReceivedMessage>> pull(final String project, final String subscription,
                                                  final boolean returnImmediately, final int maxMessages) {
    return pull(Subscription.canonicalSubscription(project, subscription), returnImmediately, maxMessages);
  }

  /**
   * Pull a batch of messages.
   *
   * @param canonicalSubscriptionName The canonical (including project name) subscription to pull from.
   * @return a future that is completed with a list of received messages.
   */
  public PubsubFuture<List<ReceivedMessage>> pull(final String canonicalSubscriptionName) {
    return pull(canonicalSubscriptionName, DEFAULT_PULL_RETURN_IMMEDIATELY);
  }

  /**
   * Pull a batch of messages.
   *
   * @param canonicalSubscriptionName The canonical (including project name) subscription to pull from.
   * @param returnImmediately         {@code true} to return immediately if the queue is empty. {@code false} to wait
   *                                  for at least one message before returning.
   * @return a future that is completed with a list of received messages.
   */
  public PubsubFuture<List<ReceivedMessage>> pull(final String canonicalSubscriptionName,
                                                  final boolean returnImmediately) {
    return pull(canonicalSubscriptionName, returnImmediately, DEFAULT_PULL_MAX_MESSAGES);
  }

  /**
   * Pull a batch of messages.
   *
   * @param canonicalSubscriptionName The canonical (including project name) subscription to pull from.
   * @param returnImmediately         {@code true} to return immediately if the queue is empty. {@code false} to wait
   *                                  for at least one message before returning.
   * @param maxMessages               Maximum number of messages to return in batch.
   * @return a future that is completed with a list of received messages.
   */
  public PubsubFuture<List<ReceivedMessage>> pull(final String canonicalSubscriptionName,
                                                  final boolean returnImmediately, final int maxMessages) {
    final String path = canonicalSubscriptionName + ":pull";
    final PullRequest req = PullRequest.builder()
        .returnImmediately(returnImmediately)
        .maxMessages(maxMessages)
        .build();
    return pull(path, req);
  }

  /**
   * Pull a batch of messages.
   *
   * @param pullRequest The pull request.
   * @return a future that is completed with a list of received messages.
   */
  public PubsubFuture<List<ReceivedMessage>> pull(final String path, final PullRequest pullRequest) {
    // TODO (dano): use async client when chunked encoding is fixed
//    return post("pull", path, pullRequest, PullResponse.class)
//        .thenApply(PullResponse::receivedMessages);
    return requestJavaNet("pull", POST, path, pullRequest,
        readJson(PullResponse.class).andThen(PullResponse::receivedMessages));
  }

  /**
   * Acknowledge a batch of received messages.
   *
   * @param project      The Google Cloud project.
   * @param subscription The subscription to acknowledge messages on.
   * @param ackIds       List of message ID's to acknowledge.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Void> acknowledge(final String project, final String subscription, final String... ackIds) {
    return acknowledge(project, subscription, asList(ackIds));
  }

  /**
   * Acknowledge a batch of received messages.
   *
   * @param project      The Google Cloud project.
   * @param subscription The subscription to acknowledge messages on.
   * @param ackIds       List of message ID's to acknowledge.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Void> acknowledge(final String project, final String subscription, final List<String> ackIds) {
    return acknowledge(Subscription.canonicalSubscription(project, subscription), ackIds);
  }

  /**
   * Acknowledge a batch of received messages.
   *
   * @param canonicalSubscriptionName The canonical (including project name) subscription to acknowledge messages on.
   * @param ackIds                    List of message ID's to acknowledge.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Void> acknowledge(final String canonicalSubscriptionName, final List<String> ackIds) {
    final String path = canonicalSubscriptionName + ":acknowledge";
    final AcknowledgeRequest req = AcknowledgeRequest.builder()
        .ackIds(ackIds)
        .build();
    return post("acknowledge", path, req, VOID);
  }

  /**
   * Modify the ack deadline for a list of received messages.
   *
   * @param project            The Google Cloud project.
   * @param subscription       The subscription of the received message to modify the ack deadline on.
   * @param ackDeadlineSeconds The new ack deadline.
   * @param ackIds             List of message ID's to modify the ack deadline on.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Void> modifyAckDeadline(final String project, final String subscription,
                                              final int ackDeadlineSeconds, final String... ackIds) {
    return modifyAckDeadline(project, subscription, ackDeadlineSeconds, asList(ackIds));
  }

  /**
   * Modify the ack deadline for a list of received messages.
   *
   * @param project            The Google Cloud project.
   * @param subscription       The subscription of the received message to modify the ack deadline on.
   * @param ackDeadlineSeconds The new ack deadline.
   * @param ackIds             List of message ID's to modify the ack deadline on.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Void> modifyAckDeadline(final String project, final String subscription,
                                              final int ackDeadlineSeconds, final List<String> ackIds) {
    return modifyAckDeadline(Subscription.canonicalSubscription(project, subscription), ackDeadlineSeconds, ackIds);
  }

  /**
   * Modify the ack deadline for a list of received messages.
   *
   * @param canonicalSubscriptionName The canonical (including project name) subscription of the received message to
   *                                  modify the ack deadline on.
   * @param ackDeadlineSeconds        The new ack deadline.
   * @param ackIds                    List of message ID's to modify the ack deadline on.
   * @return A future that is completed when this request is completed.
   */
  public PubsubFuture<Void> modifyAckDeadline(final String canonicalSubscriptionName, final int ackDeadlineSeconds,
                                              final List<String> ackIds) {
    final String path = canonicalSubscriptionName + ":modifyAckDeadline";
    final ModifyAckDeadlineRequest req = ModifyAckDeadlineRequest.builder()
        .ackDeadlineSeconds(ackDeadlineSeconds)
        .ackIds(ackIds)
        .build();
    return post("modify ack deadline", path, req, VOID);
  }

  /**
   * Make a GET request.
   */
  private <T> PubsubFuture<T> get(final String operation, final String path, final ResponseReader<T> responseReader) {
    return request(operation, HttpMethod.GET, path, responseReader);
  }

  /**
   * Make a POST request.
   */
  private <T> PubsubFuture<T> post(final String operation, final String path, final Object payload,
                                   final ResponseReader<T> responseReader) {
    return request(operation, HttpMethod.POST, path, payload, responseReader);
  }

  /**
   * Make a PUT request.
   */
  private <T> PubsubFuture<T> put(final String operation, final String path, final Object payload,
                                  final ResponseReader<T> responseReader) {
    return request(operation, HttpMethod.PUT, path, payload, responseReader);
  }

  /**
   * Make a DELETE request.
   */
  private <T> PubsubFuture<T> delete(final String operation, final String path, final ResponseReader<T> responseReader) {
    return request(operation, HttpMethod.DELETE, path, responseReader);
  }

  /**
   * Make an HTTP request.
   */
  private <T> PubsubFuture<T> request(final String operation, final HttpMethod method, final String path,
                                      final ResponseReader<T> responseReader) {
    return request(operation, method, path, NO_PAYLOAD, responseReader);
  }

  /**
   * Make an HTTP request.
   */
  private <T> PubsubFuture<T> request(final String operation, final HttpMethod method, final String path,
      final Object payload, final ResponseReader<T> responseReader) {

    final String uri = baseUri + path;
    final RequestBuilder builder = new RequestBuilder()
        .setUrl(uri)
        .setMethod(method.toString())
        .setHeader("Authorization", "Bearer " + accessToken)
        .setHeader("User-Agent", USER_AGENT);

    final long payloadSize;
    if (payload != NO_PAYLOAD) {
      final byte[] json = gzipJson(payload);
      payloadSize = json.length;
      builder.setHeader(CONTENT_ENCODING, GZIP);
      builder.setHeader(CONTENT_LENGTH, String.valueOf(json.length));
      builder.setHeader(CONTENT_TYPE, APPLICATION_JSON_UTF8);
      builder.setBody(json);
    } else {
      builder.setHeader(CONTENT_LENGTH, String.valueOf(0));
      payloadSize = 0;
    }

    final Request request = builder.build();

    final RequestInfo requestInfo = RequestInfo.builder()
        .operation(operation)
        .method(method.toString())
        .uri(uri)
        .payloadSize(payloadSize)
        .build();

    final PubsubFuture<T> future = new PubsubFuture<>(requestInfo);
    client.executeRequest(request, new AsyncHandler<Void>() {
      private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

      @Override
      public void onThrowable(final Throwable t) {
        future.fail(t);
      }

      @Override
      public STATE onBodyPartReceived(final HttpResponseBodyPart bodyPart) throws Exception {
        bytes.write(bodyPart.getBodyPartBytes());
        return STATE.CONTINUE;
      }

      @Override
      public STATE onStatusReceived(final HttpResponseStatus status) throws Exception {

        // Return null for 404'd GET & DELETE requests
        if (status.getStatusCode() == 404 && method == HttpMethod.GET || method == HttpMethod.DELETE) {
          future.succeed(null);
          return STATE.ABORT;
        }

        // Fail on non-2xx responses
        final int statusCode = status.getStatusCode();
        if (!(statusCode >= 200 && statusCode < 300)) {
          future.fail(new RequestFailedException(status.getStatusCode(), status.getStatusText()));
          return STATE.ABORT;
        }

        if (responseReader == VOID) {
          future.succeed(null);
          return STATE.ABORT;
        }

        return STATE.CONTINUE;
      }

      @Override
      public STATE onHeadersReceived(final HttpResponseHeaders headers) throws Exception {
        return STATE.CONTINUE;
      }

      @Override
      public Void onCompleted() throws Exception {
        if (future.isDone()) {
          return null;
        }
        try {
          future.succeed(responseReader.read(bytes.toByteArray()));
        } catch (Exception e) {
          future.fail(e);
        }
        return null;
      }
    });

    return future;
  }

  /**
   * Make an HTTP request using {@link java.net.HttpURLConnection}.
   */
  private <T> PubsubFuture<T> requestJavaNet(final String operation, final HttpMethod method, final String path,
      final Object payload, final ResponseReader<T> responseReader) {

    final HttpRequestFactory requestFactory = transport.createRequestFactory();

    final String uri = baseUri + path;

    final HttpHeaders headers = new HttpHeaders();
    final HttpRequest request;
    try {
      request = requestFactory.buildRequest(method.getName(), new GenericUrl(URI.create(uri)), null);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    headers.setAuthorization("Bearer " + accessToken);
    headers.setUserAgent("Spotify");

    final long payloadSize;
    if (payload != NO_PAYLOAD) {
      final byte[] json = gzipJson(payload);
      payloadSize = json.length;
      headers.setContentEncoding(GZIP);
      headers.setContentLength((long) json.length);
      headers.setContentType(APPLICATION_JSON_UTF8);
      request.setContent(new ByteArrayContent(APPLICATION_JSON_UTF8, json));
    } else {
      payloadSize = 0;
    }

    request.setHeaders(headers);

    final RequestInfo requestInfo = RequestInfo.builder()
        .operation(operation)
        .method(method.toString())
        .uri(uri)
        .payloadSize(payloadSize)
        .build();

    final PubsubFuture<T> future = new PubsubFuture<>(requestInfo);

    executor.execute(() -> {
      final HttpResponse response;
      try {
        response = request.execute();
      } catch (IOException e) {
        future.fail(e);
        return;
      }

      // Return null for 404'd GET & DELETE requests
      if (response.getStatusCode() == 404 && method == HttpMethod.GET || method == HttpMethod.DELETE) {
        future.succeed(null);
        return;
      }

      // Fail on non-2xx responses
      final int statusCode = response.getStatusCode();
      if (!(statusCode >= 200 && statusCode < 300)) {
        future.fail(new RequestFailedException(response.getStatusCode(), response.getStatusMessage()));
        return;
      }

      if (responseReader == VOID) {
        future.succeed(null);
        return;
      }

      try {

        future.succeed(responseReader.read(ByteStreams.toByteArray(response.getContent())));
      } catch (Exception e) {
        future.fail(e);
      }
    });

    return future;
  }

  private byte[] gzipJson(final Object payload) {
    // TODO (dano): cache and reuse deflater
    try (
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final GZIPOutputStream gzip = new GZIPOutputStream(bytes) {
          {
            this.def.setLevel(compressionLevel);
          }
        }
    ) {
      Json.write(gzip, payload);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Create a new {@link Pubsub} client with default configuration.
   */
  public static Pubsub create() {
    return builder().build();
  }

  /**
   * Create a new {@link Builder} that can be used to build a new {@link Pubsub} client.
   */
  public static Builder builder() {
    return new Builder();
  }


  /**
   * A {@link Builder} that can be used to build a new {@link Pubsub} client.
   */
  public static class Builder {

    private static final URI DEFAULT_URI = URI.create("https://pubsub.googleapis.com/v1/");

    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 30000;

    private final AsyncHttpClientConfig.Builder clientConfig = new AsyncHttpClientConfig.Builder()
        .setCompressionEnforced(true)
        .setUseProxySelector(true)
        .setRequestTimeout(DEFAULT_REQUEST_TIMEOUT_MS)
        .setReadTimeout(DEFAULT_REQUEST_TIMEOUT_MS);

    private Credential credential;
    private URI uri = DEFAULT_URI;
    private int compressionLevel = Deflater.DEFAULT_COMPRESSION;

    private Builder() {
    }

    /**
     * Creates a new {@code Pubsub}.
     */
    public Pubsub build() {
      return new Pubsub(this);
    }

    /**
     * Set the maximum time in milliseconds the client will can wait when connecting to a remote host.
     *
     * @param connectTimeout the connect timeout in milliseconds.
     * @return this config builder.
     */
    public Builder connectTimeout(final int connectTimeout) {
      clientConfig.setConnectTimeout(connectTimeout);
      return this;
    }

    /**
     * Set the Gzip compression level to use, 0-9 or -1 for default.
     *
     * @param compressionLevel The compression level to use.
     * @return this config builder.
     * @see Deflater#setLevel(int)
     * @see Deflater#DEFAULT_COMPRESSION
     * @see Deflater#BEST_COMPRESSION
     * @see Deflater#BEST_SPEED
     */
    public Builder compressionLevel(final int compressionLevel) {
      checkArgument(compressionLevel > -1 && compressionLevel <= 9, "compressionLevel must be -1 or 0-9.");
      this.compressionLevel = compressionLevel;
      return this;
    }

    /**
     * Set the connection read timeout in milliseconds.
     *
     * @param readTimeout the read timeout in milliseconds.
     * @return this config builder.
     */
    public Builder readTimeout(final int readTimeout) {
      clientConfig.setReadTimeout(readTimeout);
      return this;
    }

    /**
     * Set the request timeout in milliseconds.
     *
     * @param requestTimeout the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder requestTimeout(final int requestTimeout) {
      clientConfig.setRequestTimeout(requestTimeout);
      return this;
    }

    /**
     * Set the maximum number of connections client will open. Default is unlimited (-1).
     *
     * @param maxConnections the maximum number of connections.
     * @return this config builder.
     */
    public Builder maxConnections(final int maxConnections) {
      clientConfig.setMaxConnections(maxConnections);
      return this;
    }

    /**
     * Set the maximum number of milliseconds a pooled connection will be reused. -1 for no limit.
     *
     * @param pooledConnectionTTL the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder pooledConnectionTTL(final int pooledConnectionTTL) {
      clientConfig.setConnectionTTL(pooledConnectionTTL);
      return this;
    }

    /**
     * Set the maximum number of milliseconds an idle pooled connection will be be kept.
     *
     * @param pooledConnectionIdleTimeout the timeout in milliseconds.
     * @return this config builder.
     */
    public Builder pooledConnectionIdleTimeout(final int pooledConnectionIdleTimeout) {
      clientConfig.setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout);
      return this;
    }

    /**
     * Set whether to allow connection pooling or not. Default is true.
     *
     * @param allowPoolingConnections the maximum number of connections.
     * @return this config builder.
     */
    public Builder allowPoolingConnections(final boolean allowPoolingConnections) {
      clientConfig.setAllowPoolingConnections(allowPoolingConnections);
      clientConfig.setAllowPoolingSslConnections(allowPoolingConnections);
      return this;
    }

    /**
     * Set Google Cloud API credentials to use. Set to null to use application default credentials.
     *
     * @param credential the credentials used to authenticate.
     */
    public Builder credential(final Credential credential) {
      this.credential = credential;
      return this;
    }

    /**
     * Set cipher suites to enable for SSL/TLS.
     *
     * @param enabledCipherSuites The cipher suites to enable.
     */
    public Builder enabledCipherSuites(final String... enabledCipherSuites) {
      clientConfig.setEnabledCipherSuites(enabledCipherSuites);
      return this;
    }

    /**
     * Set cipher suites to enable for SSL/TLS.
     *
     * @param enabledCipherSuites The cipher suites to enable.
     */
    public Builder enabledCipherSuites(final List<String> enabledCipherSuites) {
      clientConfig.setEnabledCipherSuites(enabledCipherSuites.toArray(new String[enabledCipherSuites.size()]));
      return this;
    }

    /**
     * The Google Cloud Pub/Sub API URI. By default, this is the Google Cloud Pub/Sub provider, however you may run a
     * local Developer Server.
     *
     * @param uri the service to connect to.
     */
    public Builder uri(final URI uri) {
      checkNotNull(uri, "uri");
      checkArgument(uri.getRawQuery() == null, "illegal service uri: %s", uri);
      checkArgument(uri.getRawFragment() == null, "illegal service uri: %s", uri);
      this.uri = uri;
      return this;
    }
  }

  /**
   * A {@link ResponseReader} that parses a response payload as Json into a specified {@link Class}.
   * @param cls The {@link Class} to parse the Json payload as.
   */
  private <T> ResponseReader<T> readJson(Class<T> cls) {
    return payload -> Json.read(payload, cls);
  }

  /**
   * A function that takes a payload byte array and parses it into some object.
   */
  @FunctionalInterface
  interface ResponseReader<T> {

    /**
     * A marker {@link ResponseReader} that completely disables reading of the response payload.
     */
    ResponseReader<Void> VOID = bytes -> null;

    /**
     * Parse a byte array into an object.
     */
    T read(byte[] bytes) throws Exception;

    /**
     * Perform additional transformation on the parsed object.
     */
    default <U> ResponseReader<U> andThen(Function<T, U> f) {
      return bytes -> f.apply(read(bytes));
    }
  }
}
