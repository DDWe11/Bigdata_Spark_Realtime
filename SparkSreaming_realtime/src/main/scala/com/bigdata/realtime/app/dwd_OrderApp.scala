package com.bigdata.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.bigdata.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import java.util
import java.time.{LocalDate, Period}
import scala.collection.mutable.ListBuffer

/**
 * @descrption:订单宽表任务
 * 1.准备实时环境
 * 2.从redis中读取offset{order_info,order_detail}
 * 3.从kafka消费数据{order_info,order_detail}
 * 4.提取offset{order_info,order_detail}
 * 5.数据处理
 *  5.1维度关联
 *  5.2双流join
 * 6.写入ES
 * 7.提交offset{order_info,order_detail}
 */
object dwd_OrderApp{
  def main(args: Array[String]): Unit = {
    // TODO 1 准备环境
    val sparkconf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[3]")
    val ssc: StreamingContext = new StreamingContext(sparkconf, Seconds(5))

    // TODO 2 从redis中读取offset{order_info,order_detail}
    val orderInfoTopicName : String = "DWD_ORDER_INFO_INSERT"
    val orderInfoGroup : String = "DWD_ORDER_INFO_GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderInfoTopicName, orderInfoGroup)

    val orderDetailTopicName : String = "DWD_ORDER_DETAIL_INSERT"
    val orderDetailGroup : String = "DWD_ORDER_DETAIL_GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderDetailTopicName, orderDetailGroup)

    // TODO 3 从kafka消费数据{order_info,order_detail}
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderInfoOffsets != null && orderInfoOffsets.nonEmpty){
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup,orderInfoOffsets)
    }else{
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup)
    }
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderDetailOffsets != null && orderDetailOffsets.nonEmpty){
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup,orderDetailOffsets)
    }else{
      orderDetailKafkaDStream =  MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup)
    }

    // TODO 4 提取offset{order_info,order_detail}
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // TODO 5 数据处理
    // TODO 5.1 转换数据结构
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
    //test
//    orderInfoDStream.print(100)
    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )
    //test
//    orderDetailDSream.print(100)

    // TODO 5.2 维度关联
    val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      orderInfoIter => {
//        val orderInfoes: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
        val orderInfos: List[OrderInfo] = orderInfoIter.toList
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (orderInfo <- orderInfos) {
          //关联用户维度
          val uid: Long = orderInfo.user_id
          val redisUserKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUserKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          //换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          //补充到对象中
          orderInfo.user_gender = gender
          orderInfo.user_age = age

          //关联地区维度
          val provinceID: Long = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")

          //补充到对象中
          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_iso_code = provinceIsoCode

          //处理日期字段
          val createTime: String = orderInfo.create_time
          val createDtHr: Array[String] = createTime.split(" ")
          val createDate: String = createDtHr(0)
          val createHr: String = createDtHr(1).split(":")(0)
          //补充到对象中
          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr
//          orderInfoes.append(orderInfo)
        }
        jedis.close()
        orderInfos.iterator
      }
    )
//    orderInfoDimDStream.print(100)

    // TODO 5.3 双流Join
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] =
      orderInfoDimDStream.map( orderInfo => (orderInfo.id , orderInfo))

    val orderDetailKVDStream: DStream[(Long, OrderDetail)] =
      orderDetailDStream.map(orderDetail => (orderDetail.order_id , orderDetail))

    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] =
      orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)

    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.mapPartitions(
      orderJoinIter => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          //orderInfo有， orderDetail有
          if (orderInfoOp.isDefined) {
            //取出orderInfo
            val orderInfo: OrderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              //取出orderDetail
              val orderDetail: OrderDetail = orderDetailOp.get
              //组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              //放入到结果集中
              orderWides.append(orderWide)
            }
            //orderInfo有，orderDetail没有

            //orderInfo写缓存
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))

            //orderInfo读缓存
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                //组装成orderWide
                val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
                //加入到结果集中
                orderWides.append(orderWide)
              }
            }
          } else {
            //orderInfo没有， orderDetail有
            val orderDetail: OrderDetail = orderDetailOp.get
            //读缓存
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson: String = jedis.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.size > 0) {
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              //组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              //加入到结果集中
              orderWides.append(orderWide)
            } else {
              //写缓存
              // 类型:   set
              // key :   ORDERJOIN:ORDER_DETAIL:ORDER_ID
              // value :  json, json ....
              // 写入API: sadd
              // 读取API: smembers
              // 是否过期: 24小时
              val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        orderWides.iterator
      }
    )
    orderWideDStream.print(1000)

    //写入ES
    //1. 索引分割， 通过索引模板控制mapping setting aliases
    //2. 使用工具类将数据写入ES
//    orderWideDStream.foreachRDD(
//      rdd => {
//        rdd.foreachPartition(
//          orderWideIter => {
//            val orderWides: List[(String, OrderWide)] =
//              orderWideIter.map( orderWide => (orderWide.detail_id.toString , orderWide)).toList
//            if(orderWides.size > 0 ){
//              val head: (String, OrderWide) = orderWides.head
//              val date: String = head._2.create_date
//              //索引名
//              val indexName : String = s"gmall_order_wide_1018_$date"
//              //写入到ES
//              MyEsUtils.bulkSave(indexName , orderWides)
//            }
//          }
//        )
//        //提交offsets
//        MyOffsetsUtils.saveOffset(orderInfoTopicName , orderInfoGroup , orderInfoOffsetRanges)
//        MyOffsetsUtils.saveOffset(orderDetailTopicName , orderDetailGroup ,orderDetailOffsetRanges)
//      }
//    )

    ssc.start()
    ssc.awaitTermination()
  }
}
