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
