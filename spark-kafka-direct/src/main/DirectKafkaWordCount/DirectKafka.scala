import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.stores.ZooKeeperOffsetsStore
import kafka.utils.ZKGroupTopicDirs
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * ━━━━━━神兽出没━━━━━━
  * 　　　┏┓　　　┏┓
  * 　　┏┛┻━━━┛┻┓
  * 　　┃　　　　　　　┃
  * 　　┃　　　━　　　┃
  * 　　┃　┳┛　┗┳　┃
  * 　　┃　　　　　　　┃
  * 　　┃　　　┻　　　┃
  * 　　┃　　　　　　　┃
  * 　　┗━┓　　　┏━┛
  * 　　　　┃　　　┃神兽保佑, 永无BUG!
  * 　　　　 ┃　　　┃Code is far away from bug with the animal protecting
  * 　　　　┃　　　┗━━━┓
  * 　　　　┃　　　　　　　┣┓
  * 　　　　┃　　　　　　　┏┛
  * 　　　　┗┓┓┏━┳┓┏┛
  * 　　　　　┃┫┫　┃┫┫
  * 　　　　　┗┻┛　┗┻┛
  * ━━━━━━感觉萌萌哒━━━━━━
  * Created by Intellij IDEA
  * User: Created by 宋增旭
  * DateTime: 2017/5/26 14:40
  * 功能：通过Direct Approach (No Receivers) 方式的 createDirectStream 方法获取kafka数据，
  * 并将kafka 的 offset 手动更新到zookeeper
  * 参考网站：http://www.aboutyun.com/thread-20427-1-1.html
  */
object DirectKafka {
  def main(args: Array[String]): Unit = {
    val topics = args(0)
    val time = args(1).toInt

    val topicDirs = new ZKGroupTopicDirs("direct", topics)
    //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    //获取 zookeeper 中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name


    val offsetsStore = new ZooKeeperOffsetsStore("192.168.12.12:2181", zkTopicPath)


    val brokers = "192.168.12.10:6667,192.168.12.11:6667,192.168.12.12:6667" //注意是broker的端口，不是zookeeper的端口

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("yarn-client")
    val ssc = new StreamingContext(sparkConf, Seconds(time))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)


    val storedOffsets = offsetsStore.readOffsets(topics)

    println("Offsets=================" + storedOffsets)

    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicsSet)
      case Some(fromOffsets) =>
        // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder
          , (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    // Get the lines, split them into words, count the words and print
    val lines = kafkaStream.map(_._2)
    val words = lines.flatMap(_.split("\",\""))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // save the offsets
    kafkaStream.foreachRDD(rdd => offsetsStore.saveOffsets(topics, rdd))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
