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

import io.norberg.automatter.AutoMatter;
import java.util.Optional;

@AutoMatter
interface SubscriptionCreateRequest {

  String topic();

  Optional<PushConfig> pushConfig();

  Optional<Integer> ackDeadlineSeconds();

  static SubscriptionCreateRequestBuilder builder() {
    return new SubscriptionCreateRequestBuilder();
  }

  static SubscriptionCreateRequest of(Subscription subscription) {
    return builder()
        .topic(subscription.topic())
        .pushConfig(subscription.pushConfig())
        .ackDeadlineSeconds(subscription.ackDeadlineSeconds())
        .build();
  }
}
