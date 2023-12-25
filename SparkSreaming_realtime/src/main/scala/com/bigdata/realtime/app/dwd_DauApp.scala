package com.bigdata.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.realtime.bean.{DauInfo, PageLog}
import com.bigdata.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
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
//    redisFilterDStream.print()

    // TODO 5.3 维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {

        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()

        for (pageLog <- pageLogIter) {
          val dauInfo = new DauInfo()
          //1 将pagelog中的以后字段拷贝到duainfo
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          //2 补充维度
          //2.1 用户信息维度
          val uid: String = pageLog.user_id
          val redisUidkey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidkey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday: String = userInfoJsonObj.getString("birthday") // 1976-03-22
          //换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          //补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          //地区维度
          //2.2  地区信息维度
          // redis中: DIM:BASE_PROVINCE:1
          val provinceID: String = dauInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          //补充到对象中
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode

          //2.3  日期字段处理
          val date: Date = new Date(pageLog.ts)
          val dtHr: String = sdf.format(date)
          val dtHrArr: Array[String] = dtHr.split(" ")
          val dt: String = dtHrArr(0)
          val hr: String = dtHrArr(1).split(":")(0)
          //补充到对象中
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)

        }
        jedis.close()
        dauInfos.iterator
      }
    )
//    dauInfoDStream.print(100)

    //写入到OLAP
    //按照天分割索引，通过索引模版控制mapping，settings，aliases等
    //准备ES工具类
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs:List[(String ,DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid,dauInfo)).toList
            if(docs.size >0){
              val head: (String,DauInfo) = docs.head
              val ts: Long = head._2.ts
              val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr:String = sdf.format(new Date(ts))
              val indexName :String = s"gmall_dau_info_$dateStr"
              MyEsUtils.bulkSave(indexName, docs)
            }
          }
        )
        //提交偏移量
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
  //状态还原
  def revertState(): Unit ={
    //从ES中查询到所有的mid
    val date: LocalDate = LocalDate.now()
    val indexName : String = s"gmall_dau_info_$date"
    val fieldName : String = "mid"
    val mids: List[ String ] = MyEsUtils.searchField(indexName , fieldName)
    //删除redis中记录的状态（所有的mid）
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisDauKey : String = s"DAU:$date"
    jedis.del(redisDauKey)
    //将从ES中查询到的mid覆盖到Redis中
    if(mids != null && mids.size > 0 ){
      /*for (mid <- mids) {
        jedis.sadd(redisDauKey , mid )
      }*/
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey , mid )  //不会直接到redis执行
      }

      pipeline.sync()  // 到redis执行
    }

    jedis.close()
  }
}
