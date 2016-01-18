import 'package:gcloud/db.dart' as db;
import 'package:gcloud/service_scope.dart' as ss;
import 'package:gcloud/src/datastore_impl.dart' as datastore_impl;
import 'package:sintr_common/auth.dart';
import "package:sintr_common/configuration.dart" as config;
import 'package:sintr_common/logging_utils.dart' as log;
import 'package:sintr_common/tasks.dart' as tasks;
import "package:sintr_common/gae_utils.dart" as gae_utils;

main() async {
  log.setupLogging();

  String projectId = "liftoff-dev";

  config.configuration = new config.Configuration(projectId,
      cryptoTokensLocation:
          "${config.userHomePath}/Communications/CryptoTokens");

  var client = await getAuthedClient();

  var datastore = new datastore_impl.DatastoreImpl(client, 's~$projectId');
  var datastoreDB = new db.DatastoreDB(datastore);

  log.info("Setup done");

  ss.fork(() async {
    db.registerDbService(datastoreDB);

    tasks.TaskController taskController =
        new tasks.TaskController("example_task");

    await taskController.deleteAllTasks();
    log.info("Tasks deleted");

    var taskList = [];

    for (int i = 0; i < 1000; i++) {
      taskList.add(new gae_utils.CloudStorageLocation(
          "liftoff-dev-datasources", "t8.shakespeare.txt"));
      taskList.add(new gae_utils.CloudStorageLocation(
          "liftoff-dev-datasources", "2.shakespeare.txt"));
    }

    await taskController.createTasks(
        // Input locations
        taskList,
        // Source locations
        new gae_utils.CloudStorageLocation(
            "liftoff-dev-source", "test_worker.json"),

        // results
        "liftoff-dev-results");

    log.info("Tasks created");
  });
}
