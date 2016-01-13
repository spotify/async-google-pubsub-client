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

import com.spotify.google.cloud.pubsub.client.Message;
import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.google.cloud.pubsub.client.Puller;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class PullerExample {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Pubsub pubsub = Pubsub.builder()
        .build();

    final Puller.MessageHandler handler = new MyMessageHandler();

//    final Acker acker = Acker.builder()
//        .pubsub(pubsub)
//        .project("my-google-cloud-project")
//        .concurrency(128)
//        .build();

    final Puller pullar = Puller.builder()
        .pubsub(pubsub)
//        .acker(acker)
        .project("my-google-cloud-project")
        .subscription("my-subscription")
        .concurrency(128)
        .messageHandler(handler)
        .build();
  }

  private static class MyMessageHandler implements Puller.MessageHandler {

    @Override
    public CompletionStage<String> messageReceived(final Puller puller, final String subscription,
                                                   final Message message, final String ackId) {
      return CompletableFuture.completedFuture(ackId);
    }
  }
}
