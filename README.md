async-google-pubsub-client
==========================

A performant Google Pub/Sub (https://cloud.google.com/pubsub/) client.

What
----

A low level Pub/Sub client and a concurrent per-topic batching Publisher.

Why
---
The official Google Cloud Pub/Sub client library was not performant enough for our purposes due to blocking I/O etc.

Usage
-----

### Pubsub Client

```java
// Create a topic
pubsub.createTopic("my-google-cloud-project", "the-topic").get();

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
  <version>1.1.1</version>
</dependency>
```


Publisher Benchmark
-------------------

Note: This benchmark uses a lot of quota and network bandwidth.

```
$ mvn exec:java -Dexec.mainClass="com.spotify.google.cloud.pubsub.client.integration.PublisherBenchmark" -Dexec.classpathScope="test"
[INFO] Scanning for projects...
[INFO] ------------------------------------------------------------------------
[INFO] Detecting the operating system and CPU architecture
[INFO] ------------------------------------------------------------------------
[INFO] os.detected.name: osx
[INFO] os.detected.arch: x86_64
[INFO] os.detected.classifier: osx-x86_64
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building async-google-pubsub-client 1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:java (default-cli) @ async-google-pubsub-client ---
   1s:          180 messages/s.       615.707 ms avg latency.    (total:          181)
   2s:       83,114 messages/s.     1,479.015 ms avg latency.    (total:       83,298)
   3s:      101,300 messages/s.     1,073.953 ms avg latency.    (total:      184,748)
   4s:      108,856 messages/s.       940.549 ms avg latency.    (total:      293,575)
   5s:      102,275 messages/s.       924.469 ms avg latency.    (total:      396,049)
   6s:      106,333 messages/s.       952.024 ms avg latency.    (total:      502,353)
   7s:      113,751 messages/s.       905.039 ms avg latency.    (total:      615,845)
   8s:      110,656 messages/s.       887.875 ms avg latency.    (total:      726,924)
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
* Implement a consumer
* Implement retries on auth failure
