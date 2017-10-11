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

package com.spotify.google.cloud.pubsub.client.integration;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.spotify.google.cloud.pubsub.client.Subscription;
import com.spotify.logging.LoggingConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.spotify.logging.LoggingConfigurator.Level.WARN;

public class GoogleClientPullBenchmark {

  private static final int PULLER_CONCURRENCY = 128;

  public static void main(final String... args) throws IOException, ExecutionException, InterruptedException {

    final String project = Util.defaultProject();

    GoogleCredential credential;

    // Use credentials from file if available
    try {
      credential = GoogleCredential
          .fromStream(new FileInputStream("credentials.json"))
          .createScoped(PubsubScopes.all());
    } catch (IOException e) {
      credential = GoogleCredential.getApplicationDefault()
          .createScoped(PubsubScopes.all());
    }

    LoggingConfigurator.configureDefaults("benchmark", WARN);

    final String subscription = System.getenv("GOOGLE_PUBSUB_SUBSCRIPTION");
    if (subscription == null) {
      System.err.println("Please specify a subscription using the GOOGLE_PUBSUB_SUBSCRIPTION environment variable.");
      System.exit(1);
    }

    System.out.println("Consuming from GOOGLE_PUBSUB_SUBSCRIPTION='" + subscription + "'");

    final Pubsub pubsub = new Pubsub.Builder(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(), credential)
        .setApplicationName("pull-benchmark")
        .build();

    final Pubsub.Projects.Subscriptions subscriptions = pubsub.projects().subscriptions();

    final String canonicalSubscription = Subscription.canonicalSubscription(project, subscription);

    final ProgressMeter meter = new ProgressMeter();
    final ProgressMeter.Metric receives = meter.group("operations").metric("receives", "messages");

    final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    // Pull concurrently
    for (int i = 0; i < PULLER_CONCURRENCY; i++) {
      pull(subscriptions, canonicalSubscription, receives, executor);
    }
  }

  private static void pull(final Pubsub.Projects.Subscriptions subscriptions,
                           final String subscription, final ProgressMeter.Metric receives,
                           final ListeningExecutorService executor) {

    final long start = System.nanoTime();

    executor.submit(() -> {
      final PullRequest pullRequest = new PullRequest().setMaxMessages(1000).setReturnImmediately(false);
      try {
        // Blocking pull
        final PullResponse response = subscriptions.pull(subscription, pullRequest).execute();
        final long end = System.nanoTime();
        final long latency = end - start;
        final List<ReceivedMessage> messages = response.getReceivedMessages();
        receives.add(messages.size(), latency);

        // Ack received messages
        final List<String> ackIds = messages.stream().map(ReceivedMessage::getAckId).collect(Collectors.toList());
        executor.submit(() -> {
          final AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(ackIds);
          try {
            subscriptions.acknowledge(subscription, ackRequest).execute();
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
      } catch (IOException e) {
        e.printStackTrace();
      }

      // Kick off another pull
      pull(subscriptions, subscription, receives, executor);
    });
  }

}
