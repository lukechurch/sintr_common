library dispatcher;

import 'package:googleapis_auth/auth_io.dart' as auth;
import 'package:gcloud/db.dart';
import 'package:gcloud/storage.dart';
import 'package:gcloud/pubsub.dart';
import 'package:gcloud/src/datastore_impl.dart' as datastore_impl;

import 'dart:async';
import 'dart:io';

var PROJECT = 'dart-mind';
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

  var jsonCredentials = new File(CRED_PATH).readAsStringSync();
  var credentials =
      new auth.ServiceAccountCredentials.fromJson(jsonCredentials);

  client = await auth.clientViaServiceAccount(credentials, scopes);

  print('Client setup');

// Instantiate objects to access Cloud Datastore, Cloud Storage
// and Cloud Pub/Sub APIs.
  db = new DatastoreDB(new datastore_impl.DatastoreImpl(client, PROJECT));
  storage = new Storage(client, PROJECT);
  pubsub = new PubSub(client, PROJECT);

  print('Setup done');
//  var topic = await pubsub.createTopic('my-topic');

//  print ('Topic created');

  while (true) {
    await setupReciever();
  }

//  topic.publishString("Hello there");

  print("main done");
}

int i = 0;

setupReciever() async {

  //await pubsub.createSubscription('my-subscription', 'my-topic');

  Subscription subscription =
      await pubsub.lookupSubscription("my-subscription");

  Future<PullEvent> pullEvent = subscription.pull();
  var pe = await pullEvent;
  print("${new DateTime.now()}: ${i++}: ${pe.message.asString}");
  sleep(new Duration(seconds: 1));
  await pe.acknowledge();
}
