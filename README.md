async-google-pubsub-client
==========================

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.spotify/async-google-pubsub-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/async-google-pubsub-client) [![Build Status](https://travis-ci.org/spotify/async-google-pubsub-client.svg?branch=master)](https://travis-ci.org/spotify/async-google-pubsub-client) [![codecov.io](http://codecov.io/github/spotify/async-google-pubsub-client/coverage.svg?branch=master)](http://codecov.io/github/spotify/async-google-pubsub-client?branch=master)

A performant [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/) client and batch publisher.

What
----

A low level Pub/Sub client and a concurrent per-topic batching Publisher.

The client uses async-http-client with the Netty provider for making efficient and async HTTP requests to the Google Cloud Pub/Sub api. 

The publisher is implemented on top of the async Pub/Sub client and concurrently gathers individual messages into per-topic batches which are then pushed to Google Cloud Pub/Sub at a specified desired request concurrency level in order to achieve both low-latency and high throughput.

Why
---
The official Google Cloud Pub/Sub client library was not performant enough for our purposes due to blocking I/O etc.

Usage
-----

### Pubsub Client

```java
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
```

### Publisher

```java
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
```

### Puller

```java
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
```

### `pom.xml`

```xml
<dependency>
  <groupId>com.spotify</groupId>
  <artifactId>async-google-pubsub-client</artifactId>
  <version>1.31</version>
</dependency>
```


Publisher Benchmark
-------------------

Note: This benchmark uses a lot of quota and network bandwidth.

```
$ mvn exec:exec -Dexec.executable="java" -Dexec.classpathScope="test" -Dexec.args="-cp %classpath com.spotify.google.cloud.pubsub.client.integration.PublisherBenchmark"
[INFO] Scanning for projects...
[INFO] Inspecting build with total of 1 modules...
[INFO] Installing Nexus Staging features:
[INFO]   ... total of 1 executions of maven-deploy-plugin replaced with nexus-staging-maven-plugin
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building async-google-pubsub-client 1.13-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:exec (default-cli) @ async-google-pubsub-client ---
2015-09-29 18:31:44 (1 s)
----------------------------------------------------------------------------------------------------------------
publishes         1,235 (    1,235 avg) messages/s      934.888 (     934.888 avg) ms latency        1,237 total

... warmup ...

2015-09-29 18:31:53 (10 s)
----------------------------------------------------------------------------------------------------------------
publishes       198,902 (  156,137 avg) messages/s      503.912 (     620.260 avg) ms latency    1,565,391 total

2015-09-29 18:31:54 (11 s)
----------------------------------------------------------------------------------------------------------------
publishes       212,755 (  177,264 avg) messages/s      475.023 (     602.638 avg) ms latency    1,778,331 total

...
```

End To End Benchmark
-------------------

Note: This benchmark uses a lot of quota and network bandwidth.

```
$ mvn exec:exec -Dexec.executable="java" -Dexec.classpathScope="test" -Dexec.args="-cp %classpath com.spotify.google.cloud.pubsub.client.integration.EndToEndBenchmark"
[INFO] Scanning for projects...
[INFO] Inspecting build with total of 1 modules...
[INFO] Installing Nexus Staging features:
[INFO]   ... total of 1 executions of maven-deploy-plugin replaced with nexus-staging-maven-plugin
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building async-google-pubsub-client 1.13-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:exec (default-cli) @ async-google-pubsub-client ---
2015-09-29 18:29:12 (1 s)
----------------------------------------------------------------------------------------------------------------
publishes        15,230 (   15,230 avg) messages/s      650.532 (     650.532 avg) ms latency       15,224 total
 receives             0 (        0 avg) messages/s        0.000 (       0.000 avg) ms latency            0 total

... warmup ...

2015-09-29 18:29:27 (16 s)
----------------------------------------------------------------------------------------------------------------
publishes        85,455 (   79,706 avg) messages/s      588.480 (     659.066 avg) ms latency      980,385 total
 receives        78,382 (   81,186 avg) messages/s    1,112.738 (   1,319.294 avg) ms latency      939,036 total

2015-09-29 18:29:28 (17 s)
----------------------------------------------------------------------------------------------------------------
publishes       107,998 (   84,596 avg) messages/s      597.299 (     645.375 avg) ms latency    1,088,490 total
 receives       103,196 (   83,902 avg) messages/s    1,071.667 (   1,212.498 avg) ms latency    1,042,383 total
...
```

Pulling Benchmark
-------------------

Note: This benchmark uses a lot of quota and network bandwidth.

Set the `GOOGLE_PUBSUB_SUBSCRIPTION` env var to the name of a subscription to consume from.

```
$ mvn exec:exec -Dexec.executable="java" -Dexec.classpathScope="test" -Dexec.args="-cp %classpath com.spotify.google.cloud.pubsub.client.integration.PullerBenchmark"
```

Releasing
---------

We tag releases on github and publish release jars to maven central hosted by
Sonatype: <http://central.sonatype.org>

### Prerequisites


1. Sonatype credentials for publishing to maven central. Apply for permission
   to publish jars on the `com.spotify` group id.
   See <http://central.sonatype.org/pages/ossrh-guide.html>.

2. Add the sonatype credentials to `~/.m2/settings.xml`

        <server>
          <id>ossrh</id>
          <username>YOUR_SONATYPE_USER</username>
          <password>YOUR_SONATYPE_PASS</password>
        </server>

3. Set up GnuPG. See <http://central.sonatype.org/pages/working-with-pgp-signatures.html>.
   Make sure that you've distributed your public key to a key server.


### Performing a Release

Have your GnuPG password ready. Both prepare and perform steps will ask you for it.

*Note:* The current tests run during both `prepare` and `perform` include
        integration tests against the real Google Pub/Sub API. Verify
        that you have a suitable default project and credentials
        configured with the `gcloud` cli.

1. Tag and push a new release to github:

        mvn release:prepare

2. Publish the signed jar to maven central:

        mvn release:perform


Todo
----
* Implement a high level consumer (raw pull/ack support is there)
* Implement retries on auth failure
