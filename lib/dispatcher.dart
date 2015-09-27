library dispatcher;

import 'package:gcloud/db.dart';
import 'package:gcloud/storage.dart';
import 'package:gcloud/pubsub.dart';
import 'package:gcloud/src/datastore_impl.dart' as datastore_impl;

import 'dart:async';

var PROJECT = 'sintr-internal';
var CRED_PATH = '/Users/lukechurch/Communications/CryptoTokens/$PROJECT.json';

// Get an HTTP authenticated client using the service account credentials.
var scopes = []
  ..addAll(datastore_impl.DatastoreImpl.SCOPES)
  ..addAll(Storage.SCOPES)
  ..addAll(PubSub.SCOPES);

var client;
var storage;
PubSub pubsub;
var db;

main() async {
  print('Starting');

  print('Client setup');

// Instantiate objects to access Cloud Datastore, Cloud Storage
// and Cloud Pub/Sub APIs.
  db = new DatastoreDB(new datastore_impl.DatastoreImpl(client, PROJECT));
  storage = new Storage(client, PROJECT);
  pubsub = new PubSub(client, PROJECT);

  print('Setup done');
  Topic topic = await pubsub.lookupTopic('my-topic');
//  var topic = await pubsub.createTopic('my-topic');

  print('Topic created');

//  await setupReciever();

  for (int i = 0; i < 100; i++) {
    topic.publishString("Hello there $i");
  }

  print("main done");
}

setupReciever() async {
  Subscription subscription =
      await pubsub.createSubscription('my-subscription', 'my-topic');

  Future<PullEvent> pullEvent = subscription.pull();
  pullEvent.then((pe) async {
    print("${pe.message.asString}");
    await pe.acknowledge();
  });
}
