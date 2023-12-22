package com.bigdata.realtime.app

import com.alibaba.fastjson.JSON
import com.bigdata.realtime.bean.PageLog
import com.bigdata.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.{lang, util}
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 日活宽表
 * 1.准备实时环境
 * 2.从redis读取偏移量
 * 3.从kafka中消费数据
 * 4.提取偏移量结束点
 * 5.处理数据
 *  5.1 转换数据结构
 *  5.2 去重
 *  5.3 维度关联
 * 6.写入ES
 * 7.提交offsets
 */
object dwd_DauApp {
  def main(args: Array[String]): Unit = {
    //TODO 1 准备环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_DauApp").setMaster("local[3]")
    val ssc:StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //TODO 2 从redis读取偏移量
    val topicName : String = "DWD_PAGE_LOG_TOPIC"
    val groupId : String = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //TODO 3 从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    //TODO 4 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //TODO  5 处理数据
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    //pageLogDStream.print(100)
    pageLogDStream.cache()
    pageLogDStream.foreachRDD(rdd => println("处理前: " + rdd.count()))
    //TODO 5.1 去重
    //自我审查：将页面访问数据中Last_pege_id不为空的数据过滤掉
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    filterDStream.foreachRDD(
      rdd => {
        println("自我审查后: " + rdd.count())
        println("---------------------------------")
      }
    )

    //TODO 5.2 第三方审查
    //三方审查：通过redis将当日活跃的mid维护起来，自我审查后的每条数据需要到redis中进行对比去重
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        val pageLoglist: List[PageLog] = pageLogIter.toList
        println("三方审查前: " + pageLoglist.size)

        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLoglist) {
          //提取每条数据中的mid
          val mid: String = pageLog.mid
          //提取当前时间
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"
          //          val mids: util.List[String] = jedis.lrange(redisDauKey, 0, -1)
          //          if(!mids.contains(mid)){
          //            jedis.lpush(redisDauKey, mid)
          //            pageLogs.append(pageLog)
          //          }
          //
          //          val setMids: util.Set[String] = jedis.smembers(redisDauKey)
          //          if(!setMids.contains(mid)){
          //            jedis.sadd(redisDauKey,mid)
          //            pageLogs.append(pageLog)
          //          }
          val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        println("三方审查后: " + pageLogs.size)
        pageLogs.iterator
      }
    )
    redisFilterDStream.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
