async-google-pubsub-client
==========================

A performant Google Pub/Sub (https://cloud.google.com/pubsub/) client.

What
----

A low level Pub/Sub client and a concurrent per-topic batching Publisher. 

Why
---
The official Google Cloud Pub/Sub client library was now performant enough for our purposes due to blocking I/O etc.

Usage
-----

### Pubsub Client

```java
// Create a topic
pubsub.createTopic("my-google-cloud-project", "the-topic").get();

// Create a batch of messages
final List<Message> messages = asList(
    Message.builder()
        .putAttribute("type", "foo")
        .data(base64().encode("hello foo".getBytes("UTF-8")))
        .build(),
    Message.builder()
        .putAttribute("type", "bar")
        .data(base64().encode("hello foo".getBytes("UTF-8")))
        .build());

// Publish the messages
final List<String> messageIds = pubsub.publish("my-google-cloud-project", "the-topic", messages).get();

System.out.println("Message IDs: " + messageIds);
```

### Publisher

```java
final Pubsub pubsub = Pubsub.builder()
    .maxConnections(256)
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
```

### `pom.xml`

```xml
<dependency>
  <groupId>com.spotify</groupId>
  <artifactId>async-google-pubsub-client</artifactId>
  <version>1.0</version>
</dependency>
```


Todo
----
* Implement a consumer
