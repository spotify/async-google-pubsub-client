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
import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.google.cloud.pubsub.client.ReceivedMessage;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.spotify.google.cloud.pubsub.client.Message.encode;
import static java.util.Arrays.asList;

public class PubsubExample {

  public static void main(String[] args) throws ExecutionException, InterruptedException, UnsupportedEncodingException {
    final Pubsub pubsub = Pubsub.create();

    // Create a topic
    pubsub.createTopic("my-google-cloud-project", "the-topic").get();

    // Create a subscription
    pubsub.createSubscription("my-google-cloud-project", "the-subscription-name", "the-topic").get();

    // Create a batch of messages
    final List<Message> messages = asList(
        Message.builder()
            .attributes("type", "foo")
            .data(encode("hello foo"))
            .build(),
        Message.builder()
            .attributes("type", "bar")
            .data(encode("hello foo"))
            .build());

    // Publish the messages
    final List<String> messageIds = pubsub.publish("my-google-cloud-project", "the-topic", messages).get();
    System.out.println("Message IDs: " + messageIds);

    // Pull the message
    final List<ReceivedMessage> received = pubsub.pull("my-google-cloud-project", "the-subscription").get();
    System.out.println("Received Messages: " + received);

    // Ack the received messages
    final List<String> ackIds = received.stream().map(ReceivedMessage::ackId).collect(Collectors.toList());
    pubsub.acknowledge("my-google-cloud-project", "the-subscription", ackIds).get();
  }
}
