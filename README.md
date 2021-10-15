# Types usage counter

A set of scala code to process information obtained from TASTy files within Spark. All using Scala 3.

Run it with `scala-cli . --main-class localCluster` to process information in standalone Spark,
`scala-cli . --main-class typesFrom -- <org> <name> <version>` to process single library outside of Spark.

We are using [Scala CLI](scala-cli.virtuslab.org) to manage the code used in this project.