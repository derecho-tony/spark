package spark.storage

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import spark.serializer.Serializer


private[spark]
class ShuffleWriterGroup(val id: Int, val writers: Array[BlockObjectWriter])


private[spark]
class ShuffleBlockManager(blockManager: BlockManager) {

  val shuffles = new ConcurrentHashMap[Int, Shuffle]

  def forShuffle(shuffleId: Int, numBuckets: Int, serializer: Serializer): Shuffle = {
    new Shuffle(shuffleId, numBuckets, serializer)
  }

  class Shuffle(shuffleId: Int, numBuckets: Int, serializer: Serializer) {

    // Acquire a group of writers for a map task. This function first attempts to get a
    // group from the list of free groups. If it can't find one, it creates a new group
    // of writers and return that instead. At the end of a map task, the caller should
    // call the release function to return the group back to the free list.
    def acquireWriters(): ShuffleWriterGroup = {
      var group = freeGroups.poll()
      if (group == null) {
        val groupId = nextGroupID.getAndIncrement()
        val writers = Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockManager.blockId(shuffleId, bucketId, groupId)
          blockManager.getDiskBlockWriter(blockId, serializer)
        }
        group = new ShuffleWriterGroup(groupId, writers)
        allGroups += group
      }
      group
    }

    def releaseWriters(group: ShuffleWriterGroup) = {
      freeGroups.add(group)
    }

    // Keep track of all groups that have been generated. We can use this array buffer to
    // get the complete list of groups (and writers) for a particular shuffle. Default the
    // size to 32 since normally the size of the thread pool is 32 or less (32 cores).
    private val allGroups = new ArrayBuffer[ShuffleWriterGroup](32)

    // Keep track of groups that are not currently in use (i.e. no threads are using them)
    private val freeGroups = new ConcurrentLinkedQueue[ShuffleWriterGroup]

    // Used to generate the next group id.
    private val nextGroupID = new AtomicInteger(0)
  }
}


private[spark]
object ShuffleBlockManager {

  // Returns the block id for a given shuffle block.
  def blockId(shuffleId: Int, bucketId: Int, groupId: Int): String = {
    "shuffle_" + shuffleId + "_" + bucketId + "_" + groupId
  }

  // Returns true if the block is a shuffle block.
  def isShuffle(blockId: String): Boolean = blockId.startsWith("shuffle_")
}
