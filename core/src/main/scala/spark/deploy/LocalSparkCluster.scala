package spark.deploy

import scala.collection.mutable.ArrayBuffer

import akka.actor.{ActorRef, Props, Actor, ActorSystem, Terminated}

import spark.SparkException
import spark.deploy.worker.Worker
import spark.deploy.master.Master
import spark.util.AkkaUtils
import spark.{Logging, Utils}

private[spark]
class LocalSparkCluster(numSlaves: Int, coresPerSlave: Int, memoryPerSlave: Int) extends Logging {

  val localIpAddress = Utils.localIpAddress

  var masterActor : ActorRef = _
  var masterActorSystem : ActorSystem = _
  var masterPort : Int = _
  var masterUrl : String = _

  val slaveActorSystems = ArrayBuffer[ActorSystem]()
  val slaveActors = ArrayBuffer[ActorRef]()

  def start() : String = {
    logInfo("Starting a local Spark cluster with " + numSlaves + " slaves.")

    /* Start the Master */
    val (actorSystem, masterPort) = AkkaUtils.createActorSystem("sparkMaster", localIpAddress, 0)
    masterActorSystem = actorSystem
    masterUrl = "spark://" + localIpAddress + ":" + masterPort
    val actor = masterActorSystem.actorOf(
      Props(new Master(localIpAddress, masterPort, 0)), name = "Master")
    masterActor = actor

    // By default, we assume 127.100.*.* are local hostnames that Spark can bind to. However,
    // in some systems (e.g. some configuration of Mac OS X), 127.100.*.* is not usable names
    // for localhost. In those cases, we allow the developer to specify a list of usable
    // local hostnames in spark-env.sh using SPARK_TEST_LOCAL_HOSTS environmental variable.
    // The list of hostnames should be separated by comma. Developers can edit /etc/hosts
    // to generate a list (at least 2) of usable hostnames for Spark test suites. For example,
    //
    // export SPARK_TEST_LOCAL_HOSTS="localhost1, localhost2"
    val localHosts: Array[String] =
      if (System.getenv("SPARK_TEST_LOCAL_HOSTS") == null) {
        // We can pretend to test distributed stuff by giving the slaves distinct hostnames.
        // All of 127/8 should be a loopback, we use 127.100.*.* in hopes that it is
        // sufficiently distinctive.
        Array.tabulate[String](numSlaves) { slaveNum => "127.100.0." + (slaveNum + 1) }
      } else {
        val hosts = System.getenv("SPARK_TEST_LOCAL_HOSTS").split(".\\s+").take(numSlaves)
        if (hosts.size < numSlaves) {
          throw new SparkException(
            "SPARK_TEST_LOCAL_HOSTS needs to specify at least " + numSlaves + " host names.")
        }
        hosts
      }

    /* Start the Slaves */
    localHosts.zipWithIndex.foreach { case(slaveIpAddress, slaveNum) =>
      val (actorSystem, boundPort) =
        AkkaUtils.createActorSystem("sparkWorker" + (slaveNum + 1), slaveIpAddress, 0)
      slaveActorSystems += actorSystem
      val actor = actorSystem.actorOf(
        Props(new Worker(slaveIpAddress, boundPort, 0, coresPerSlave, memoryPerSlave, masterUrl)),
        name = "Worker")
      slaveActors += actor
    }

    return masterUrl
  }

  def stop() {
    logInfo("Shutting down local Spark cluster.")
    // Stop the slaves before the master so they don't get upset that it disconnected
    slaveActorSystems.foreach(_.shutdown())
    slaveActorSystems.foreach(_.awaitTermination())
    masterActorSystem.shutdown()
    masterActorSystem.awaitTermination()
  }
}
