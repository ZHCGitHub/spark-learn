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
  * DateTime: 2017/5/27 11:23
  * 功能：
  * 参考网站：
  */
object KafkaStreamingWordCount {
  //每批次的wordcount

  def main(args: Array[String]): Unit = {
    /*
    对kafka来讲,groupid的作用是:
    我们想多个作业同时消费同一个topic时,
    1每个作业拿到完整数据,计算互不干扰;
    2每个作业拿到一部分数据,相当于实现负载均衡
    当多个作业groupid相同时,属于2
    否则属于情况1
     */
    //    val topics = "g01_attack_log"
    val topics = args(0)
    val time = args(1).toInt

    //setmaster的核数至少给2,如果给1,资源不够则无法计算,至少需要一个核进行维护,一个计算
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("yarn-client")
    val ssc = new StreamingContext(sparkConf, Seconds(time)) //两秒一个批次


    val zkQuorum = "192.168.12.12:2181"
    val group = "g01"

    val numThreads = 6

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //得出写到kafka里面每一行每一行的数据
    //每个时间段批次
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)


    val words = lines.flatMap(_.split("\",\""))

    //得到每个批次的wordcount
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)


    //打印三行
    wordCounts.print()
    //    wordCounts.foreachRDD(rdd => {
    //      rdd.foreachPartition(p => {
    //        p.foreach(println)
    //      })
    //    })

    //根据value值排序，取前十行


    ssc.start()
    ssc.awaitTermination()
    //如果要统计一天的,或者10小时的,我们要设置检查点,看历史情况

  }

}
