import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
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
  * DateTime: 2017/5/27 11:24
  * 功能：
  * 参考网站：
  */
object WordCountOnline {
  def main(args: Array[String]): Unit = {
    /**
      * 第一步，接收数据源
      */
    val conf = new SparkConf().setAppName("WordCount").setMaster("yarn-client")
    val ssc = new StreamingContext(conf, Seconds(10))

    /*
    设置数据来源，比如socket
    设置端口9999，数据存储级别MEMORY_AND_DISK
      */
    val receiverInputStream = ssc.socketTextStream("192.168.12.12", 9999, StorageLevel.MEMORY_AND_DISK)

    /*
    也可以来源于file
     */
    val directory = ssc.textFileStream("D:/")
    /*
    还可以来自于db，hdfs等数据源，甚至可以自定义任何数据源，给开发者带来了巨大
    的想象空间。
     */

    /**
      * 第二步，flatMap操作
      */

    /*
    接下来，我们对接收进来的数据进行操作
    第一步，通过","将每一行的单词分割开来，得到一个一个单词
     */

    val words = receiverInputStream.flatMap(_.split(","))

    /**
      * 第三步，map操作
      */

    /*
    将每个单词计数为1
     */
    val pairs = words.map(word => (word, 1))

    /**
      * 第四步，reduce操作
      */

    /*
    reduceByKey通过将局部样本相同单词的个数相加，从而得到总体的单词个数
     */

    val wordCount = pairs.reduceByKey(_ + _)

    /**
      * 第五步,print()等操作
      */

    /*
    与java编程一样，除了print()方法将处理后的数据输出外，
    还有其它的方法也很重要，在开发中需要重点掌握
    比如SaveAsTextFile,SaveAsHadoopFile等，
    最为重要的是foreachRDD方法，这个方法可以将数据写入Redis，DB，DashBoard等，
    甚至可以随意的定义数据存放在哪里，功能非常强大
     */
    wordCount.print()

    /**
      * 第六步，awaitTermination操作
      */

    /*
    此处的print方法并不会触发job执行，因为目前代码还处于Spark Streaming框架的控制下，具体是否触发job
    是取决于设置的Duration时间的间隔
     */
    ssc.start()

    /*
    等待程序结束
     */

    ssc.awaitTermination()


  }

}
