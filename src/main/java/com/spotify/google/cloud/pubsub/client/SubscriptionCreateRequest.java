package com.spotify.google.cloud.pubsub.client;

import io.norberg.automatter.AutoMatter;
import java.util.Optional;

@AutoMatter
public interface SubscriptionCreateRequest {

  String topic();

  Optional<PushConfig> pushConfig();

  Optional<Integer> ackDeadlineSeconds();

}
