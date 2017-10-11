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
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.google.cloud.pubsub.client.Topic.canonicalTopic;
import static com.spotify.google.cloud.pubsub.client.Topic.validateCanonicalTopic;

import io.norberg.automatter.AutoMatter;
import java.util.Optional;
import java.util.regex.Pattern;

@AutoMatter
public interface Subscription {

  Pattern PATTERN = Pattern.compile("^projects/[^/]*/subscriptions/[^/]*$");

  String PROJECTS = "projects";
  String SUBSCRIPTIONS = "subscriptions";

  String name();

  String topic();

  Optional<PushConfig> pushConfig();

  Optional<Integer> ackDeadlineSeconds();

  static SubscriptionBuilder builder() {
    return new SubscriptionBuilder();
  }

  static Subscription of(String project, String name, String topic) {
    return of(canonicalSubscription(project, name), canonicalTopic(project, topic));
  }

  static Subscription of(String canonicalSubscription, String canonicalTopic) {
    validateCanonicalSubscription(canonicalSubscription);
    validateCanonicalTopic(canonicalTopic);
    return builder().name(canonicalSubscription).topic(canonicalTopic).build();
  }

  static String canonicalSubscription(final String project, final String subscription) {
    checkArgument(!isNullOrEmpty(project) && !project.contains("/"), "illegal project: %s", project);
    checkArgument(!isNullOrEmpty(subscription) && !subscription.contains("/") && !subscription.startsWith("goog"),
        "illegal subscription: %s", subscription);
    return PROJECTS + '/' + project + '/' + SUBSCRIPTIONS + '/' + subscription;
  }

  static void validateCanonicalSubscription(final String canonicalSubscription) {
    checkArgument(PATTERN.matcher(canonicalSubscription).matches(), "malformed subscription: %s",
        canonicalSubscription);
  }
}

