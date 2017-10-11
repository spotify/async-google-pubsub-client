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

package com.spotify.google.cloud.pubsub.client.example;

import com.spotify.google.cloud.pubsub.client.Message;
import com.spotify.google.cloud.pubsub.client.Publisher;
import com.spotify.google.cloud.pubsub.client.Pubsub;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class PublisherExample {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Pubsub pubsub = Pubsub.builder()
        .build();

    final Publisher publisher = Publisher.builder()
        .pubsub(pubsub)
        .project("my-google-cloud-project")
        .concurrency(128)
        .build();

    // A never ending stream of messages...
    final Iterable<MessageAndTopic> messageStream = incomingMessages();

    // Publish incoming messages
    messageStream.forEach(m -> publisher.publish(m.topic, m.message));
  }

  private static Iterable<MessageAndTopic> incomingMessages() {
    // Some never ending stream of messages...
    return Collections.emptyList();
  }

  static class MessageAndTopic {

    String topic;
    Message message;
  }
}
