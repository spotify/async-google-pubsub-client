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
 * Copyright (c) 2011-2016 Spotify AB
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

package com.spotify.google.cloud.pubsub.client.example;

import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.google.cloud.pubsub.client.Puller;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.spotify.google.cloud.pubsub.client.Puller.MessageHandler;
import static com.spotify.google.cloud.pubsub.client.Puller.builder;

public class PullerExample {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Pubsub pubsub = Pubsub.builder()
        .build();

    final MessageHandler handler = (puller, subscription, message, ackId) -> {
      System.out.println("got message: " + message);
      return CompletableFuture.completedFuture(ackId);
    };

    final Puller puller = builder()
        .pubsub(pubsub)
        .project("my-google-cloud-project")
        .subscription("my-subscription")
        .concurrency(32)
        .messageHandler(handler)
        .build();
  }

}
