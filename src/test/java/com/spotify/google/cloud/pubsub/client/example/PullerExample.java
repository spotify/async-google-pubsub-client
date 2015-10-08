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

    final Puller puller = Puller.builder()
        .pubsub(pubsub)
        .project("my-google-cloud-project")
        .subscription("my-subscription")
        .concurrency(32)
        .messageHandler(handler)
        .build();
  }

  private static class MyMessageHandler implements Puller.MessageHandler {

    @Override
    public CompletionStage<String> handleMessage(final Puller puller, final String subscription,
                                                 final Message message, final String ackId) {
      System.out.println("got message: " + message);
      return CompletableFuture.completedFuture(ackId);
    }
  }
}
