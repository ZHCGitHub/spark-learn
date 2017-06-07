package kafka.stores

import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.common.TopicAndPartition
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

class ZooKeeperOffsetsStore(zkHosts: String, zkPath: String) extends OffsetsStore with LazyLogging {

  private val zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer)
  //private val zkClient = new ZkClient(zkHosts)
  //使用第二种方式创建zkClient时，在更新offset的时候，前面会出现一段乱码
  //使用第一种创建方式可以解决此问题

  // Read the previously saved offsets from Zookeeper
  override def readOffsets(topic: String): Option[Map[TopicAndPartition, Long]] = {

    logger.info("Reading offsets from ZooKeeper")

    val children = zkClient.countChildren(zkPath)
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）

    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    if (children > 0) {
      //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，
      // 不然会报 OutOfRange 的错误
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${zkPath}/${i}")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        logger.info("fromOffsets==============================" + fromOffsets)
        logger.info("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
      }
      Some(fromOffsets)
    } else {
      //      logger.info("No offsets found in ZooKeeper.")
      //      val tp = TopicAndPartition(topic, 0)
      //      fromOffsets += (tp -> 0)
      //      Some(fromOffsets)
      None
    }


    //    val stopwatch = new Stopwatch()
    //    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)

    //    offsetsRangesStrOpt match {
    //      case Some(offsetsRangesStr) =>
    //        logger.debug(s"Read offset ranges: ${offsetsRangesStr}")
    //
    //        //        val offsets = offsetsRangesStr.split(",")
    //        //          .map(s => s.split(":"))
    //        //          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
    //        //          .toMap
    //        val offsets = offsetsRangesStr.map { case offsetStr => (TopicAndPartition(topic, 0) -> offsetStr.toLong) }.toMap
    //
    //        logger.info("Done reading offsets from ZooKeeper. Took " + stopwatch)
    //
    //        Some(offsets)
    //      case None =>
    //        logger.info("No offsets found in ZooKeeper. Took " + stopwatch)
    //        None
    //    }
  }

  // Save the offsets back to ZooKeeper
  //
  // IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
  // because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
  // logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
  // workaround.
  override def saveOffsets(topic: String, rdd: RDD[_]): Unit = {
    //    logger.info("Saving offsets to ZooKeeper")
    //    val stopwatch = new Stopwatch()
    //
    //    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //    offsetsRanges.foreach(offsetRange => logger.debug(s"Using ${offsetRange}"))
    //
    //    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
    //      .mkString(",")
    //    logger.info(s"向zookeeper中topic:${topic}的分区写入offset:${offsetsRangesStr}")
    //    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
    //
    //    logger.info("Done updating offsets in ZooKeeper. Took " + stopwatch)


    logger.info("Saving offsets to ZooKeeper")
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    logger.info(s"offsetsRanges的长度======================${offsetsRanges.length}")

    offsetsRanges.foreach(offsetRange => {
      val path = s"${zkPath}/${offsetRange.partition}"
      val offset = offsetRange.fromOffset.toString
      // 将该 partition 的 offset 保存到 zookeeper
      logger.warn(s"向zookeeper中topic:${offsetRange.topic}的${offsetRange.partition}分区写入offset:${offset}")
      ZkUtils.updatePersistentPath(zkClient, path, offset)
    }
    )
  }
}
