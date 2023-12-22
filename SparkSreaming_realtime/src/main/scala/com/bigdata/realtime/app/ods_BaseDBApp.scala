package com.bigdata.realtime.app

import java.util
import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * 业务数据消费分流
 * 1.准备实时环境
 * 2.从redis中读取偏移量
 * 3.从kafka中消费数据
 * 4.提取偏移量结束点
 * 5.数据处理：转换数据结构，数据分流
 *          事实数据 ->kafka
 *          维度数据 ->Redis
 * 6.flush kafka的缓冲区
 * 7.提交offset
 */

object ods_BaseDBApp {
  def main(args: Array[String]): Unit = {
    // 1.准备实时环境
    val sparkConf:SparkConf = new SparkConf().setAppName("ods_BaseDBApp").setMaster("local[3]")
    val ssc:StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    val topicName : String = "ODS_BASE_DB"
    val groupId : String = "ODS_BASE_DB_GROUP"
    // 2.从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    // 3.从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
        kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // 4.提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.数据处理：转换数据结构，数据分流
    val jsonObjectDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val dataJson: String = consumerRecord.value()
        val jsonObject: JSONObject = JSON.parseObject(dataJson)
        jsonObject
      }
    )
    // 数据分流
    jsonObjectDStream.foreachRDD(
      rdd => {
        val redisFactKeys :String = "FACT:TABLES"
        val redisDimKeys :String = "DIM:TABLES"
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        // 获取事实表清单
        val factTables: util.Set[String] = jedis.smembers(redisFactKeys)
        println("factTables: "  + factTables)
        //广播变量
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)

        val dimTables: util.Set[String] = jedis.smembers(redisDimKeys)
        println("dimTables: "  + dimTables)
        //广播变量
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)

        jedis.close()

        rdd.foreachPartition(
          jsonObjIter => {
            //开启redis连接
            val jedis: Jedis = MyRedisUtils.getJedisFromPool()
            for (jsonObj <- jsonObjIter) {
              // 提取操作类型
              val operaType: String = jsonObj.getString("type")
              val operaValue:String = operaType match {
                case "bootstrap-insert" => "insert"
                case "insert" => "insert"
                case "update" => "update"
                case "delete" => "delete"
                case _ => "null"
              }
              //判断操作类：1.明确什么操作  2.过滤无用数据
              if(operaValue != "null"){
                //提取表名
                val tableName: String = jsonObj.getString("table")

                if(factTablesBC.value.contains(tableName)){
                  //事实数据
                  val data: String  = jsonObj.getString("data")
                  //DWD_ORDER_INFO_INSERT,DWD_ORDER_INFO_UPDATE,DWD_ORDER_INFO_DELETE
                  val dwdTopicName :String = s"DWD_${tableName.toUpperCase}_${operaValue.toUpperCase}"
                  MyKafkaUtils.send(dwdTopicName,data)
                }
                if(dimTablesBC.value.contains(tableName)){
                  //维度数据
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  val redisKey: String = s"DIM:${tableName.toUpperCase}:$id"
                  jedis.set(redisKey,dataObj.toJSONString)
                }
              }
            }
            //关闭redis连接
            jedis.close()
            //刷新kafka缓冲区
            MyKafkaUtils.flush()
          }
        )
        //提交偏移量
        MyOffsetsUtils.saveOffset(topicName,groupId,offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
