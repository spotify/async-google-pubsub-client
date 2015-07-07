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
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.annotations.VisibleForTesting;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.spotify.google.cloud.pubsub.client.Topic.canonicalTopic;
import static com.spotify.google.cloud.pubsub.client.Topic.validateCanonicalTopic;
import static java.util.Arrays.asList;

/**
 * An async low-level Google Cloud Pub/Sub client.
 */
public class Pubsub implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Pubsub.class);

  private final Service service;

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
    log.debug("compression enforced: {}", config.isCompressionEnforced());
    log.debug("accept any certificate: {}", config.isAcceptAnyCertificate());
    log.debug("follows redirect: {}", config.isFollowRedirect());
    log.debug("pooled connection TTL: {}", config.getConnectionTTL());
    log.debug("pooled connection idle timeout: {}", config.getPooledConnectionIdleTimeout());
    log.debug("pooling connections: {}", config.isAllowPoolingConnections());
    log.debug("pooling SSL connections: {}", config.isAllowPoolingSslConnections());
    log.debug("user agent: {}", config.getUserAgent());
    log.debug("max request retry: {}", config.getMaxRequestRetry());

    this.service = builder.service.createService(config, builder.uri, builder.accessToken.get());
  }

  /**
   * Close this {@link Pubsub} client.
   */
  @Override
  public void close() {
    service.close();
  }

  /**
   * Get a future that is completed when this {@link Pubsub} client is closed.
   */
  public CompletableFuture<Void> closeFuture() {
    return service.closeFuture();
  }

  /**
   * List the Pub/Sub topics in a project. This will get the first page of topics. To enumerate all topics you might
   * have to make further calls to {@link #listTopics(String, String)} with the page token in order to get further
   * pages.
   *
   * @param project The Google Cloud project.
   * @return A future that is completed when this request is completed.
   */
  public CompletableFuture<TopicList> listTopics(final String project) {
    final String uri = "/projects/" + project + "/topics";
    return service.get(uri, TopicList.class);
  }

  /**
   * Get a page of Pub/Sub topics in a project using a specified page token.
   *
   * @param project   The Google Cloud project.
   * @param pageToken A token for the page of topics to get.
   * @return A future that is completed when this request is completed.
   */
  public CompletableFuture<TopicList> listTopics(final String project,
                                                 final String pageToken) {
    final StringBuilder uri = new StringBuilder()
        .append("/projects/").append(project).append("/topics");
    if (pageToken != null) {
      uri.append("?pageToken=").append(pageToken);
    }
    return service.get(uri.toString(), TopicList.class);
  }

  /**
   * Create a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to create.
   * @return A future that is completed when this request is completed.
   */
  public CompletableFuture<Topic> createTopic(final String project,
                                              final String topic) {
    return createTopic(canonicalTopic(project, topic));
  }

  /**
   * Create a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to create.
   * @return A future that is completed when this request is completed.
   */
  public CompletableFuture<Topic> createTopic(final String canonicalTopic) {
    return createTopic(canonicalTopic, Topic.of(canonicalTopic));
  }

  /**
   * Create a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to create.
   * @param req            The payload of the create request. This seems to be ignore in the current API version.
   * @return A future that is completed when this request is completed.
   */
  private CompletableFuture<Topic> createTopic(final String canonicalTopic,
                                               final Topic req) {
    validateCanonicalTopic(canonicalTopic);
    final String uri = "/" + canonicalTopic;
    return service.put(uri, req, Topic.class);
  }

  /**
   * Get a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to get.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public CompletableFuture<Topic> getTopic(final String project, final String topic) {
    final String uri = "/" + canonicalTopic(project, topic);
    return service.get(uri, Topic.class);
  }

  /**
   * Get a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to get.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public CompletableFuture<Topic> getTopic(final String canonicalTopic) {
    validateCanonicalTopic(canonicalTopic);
    final String uri = "/" + canonicalTopic;
    return service.get(uri, Topic.class);
  }

  /**
   * Delete a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to delete.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public CompletableFuture<Void> deleteTopic(final String project,
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
  public CompletableFuture<Void> deleteTopic(final String canonicalTopic) {
    validateCanonicalTopic(canonicalTopic);
    final String uri = "/" + canonicalTopic;
    return service.delete(uri);
  }

  /**
   * Publish a batch of messages.
   *
   * @param project  The Google Cloud project.
   * @param topic    The topic to publish on.
   * @param messages The batch of messages.
   * @return a future that is completed with a list of message ID's for the published messages.
   */
  public CompletableFuture<List<String>> publish(final String project, final String topic,
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
  public CompletableFuture<List<String>> publish(final String project, final String topic,
                                                 final List<Message> messages) {
    final String uri = "/projects/" + project + "/topics/" + topic + ":publish";
    return service.post(uri, PublishRequest.of(messages), PublishResponse.class)
        .thenApply(PublishResponse::messageIds);
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

    private final AsyncHttpClientConfig.Builder clientConfig = new AsyncHttpClientConfig.Builder();

    private URI uri = DEFAULT_URI;
    private Service.Factory service = Service::new;
    private Supplier<Credential> credential = () -> defaultCredential();
    private Supplier<Supplier<String>> accessToken = () -> new AccessTokenSupplier(credential.get());

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
     * Set the maximum number of connections client will open.
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
      return this;
    }

    /**
     * Set Google Cloud API credentials to use.
     *
     * @param credential the credentials used to authenticate.
     */
    public Builder credential(final Credential credential) {
      this.credential = () -> credential;
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
      this.uri = uri;
      return this;
    }

    /**
     * Set a factory function to use for creating the {@link AsyncHttpClient} that will be used to talk to Google
     * Pub/Sub. Currently for testing purposes only.
     */
    @VisibleForTesting
    Builder service(final Service.Factory service) {
      this.service = service;
      return this;
    }

    /**
     * Set a supplier for access tokens to use for authentication when talking to Google Pub/Sub. Currently for testing
     * purposes only.
     */
    @VisibleForTesting
    Builder accessToken(final Supplier<String> accessToken) {
      this.accessToken = () -> accessToken;
      return this;
    }

    /**
     * Get the application default Google credential.
     */
    private static Credential defaultCredential() {
      try {
        return GoogleCredential.getApplicationDefault(
            Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
