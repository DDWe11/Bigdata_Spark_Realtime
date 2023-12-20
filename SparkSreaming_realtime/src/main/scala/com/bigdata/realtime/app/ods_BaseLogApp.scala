package com.bigdata.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bigdata.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.bigdata.realtime.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang

/*
 *  @descrption:日志数据的消费分流
 * 1.准备实时处理环境StreamingContext
 * 2.从kafka消费数据
 * 3.处理数据
 *  3.1 转换数据结构 json ->
 *          专用结构  Bean
 *          通用结构  Map  JsonObject
 *  3.2 分流操作
 * 4.写出到DWD层
 */

object ods_BaseLogApp {
    def main(args: Array[String]): Unit = {
        //1.准备实时环境
        val sparkConf: SparkConf = new SparkConf().setAppName("ods_BaseLogApp").setMaster("local[3]")
        //5s一个周期
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

        //2.从kafka消费数据
        val topicName: String = "ODS_BASE_LOG"
        val groupId: String = "ODS_BASE_LOG_GROUP"
        val KafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
        //    KafkaDStream.print(10)
        // 3.处理数据
        // 3.1 转换数据结构
        val jsonObjDStream: DStream[JSONObject] = KafkaDStream.map(
            consumerRecord => {
                //获取consumerRecord中的value，value就是日志数据
                val log: String = consumerRecord.value()
                val jsonObj: JSONObject = JSON.parseObject(log)
                jsonObj
            }
        )
        //    jsonObjDStream.print(10)

        // 3.2 分流操作
        // 日志数据：页面访问数据{公共字段，页面数据，曝光数据，事件数据，错误数据}；启动数据{公共字段，启动数据}
        val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC" //页面访问数据
        val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光数据
        val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC" //页面事件数据
        val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC" //启动数据
        val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC" //错误数据

        //分流规则：
        //错误数据  不做任何拆分，只要包含错误字段，直接整条数据发送到相应的topic中
        //页面数据  拆分成，页面访问，曝光，事件，分别发送到相应的topic中
        //启动数据  发送到对应的topic中

        jsonObjDStream.foreachRDD(
            rdd => {
                rdd.foreach(
                    jsonObj => {
                        //获取topic
                        //分流错误数据
                        val errObj: JSONObject = jsonObj.getJSONObject("err")
                        if (errObj != null) {
                            //将错误数据发送到kafka
                            MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
                        } else {
                            // 提取公共字段
                            val commonObj: JSONObject = jsonObj.getJSONObject("common")
                            val ar: String = commonObj.getString("ar")
                            val uid: String = commonObj.getString("uid")
                            val os: String = commonObj.getString("os")
                            val ch: String = commonObj.getString("ch")
                            val isNew: String = commonObj.getString("is_new")
                            val md: String = commonObj.getString("md")
                            val mid: String = commonObj.getString("mid")
                            val vc: String = commonObj.getString("vc")
                            val ba: String = commonObj.getString("ba")

                            // 提出时间戳
                            val ts: Long = jsonObj.getLong("ts")
                            // 页面数据
                            val pageObj: JSONObject = jsonObj.getJSONObject("page")
                            if (pageObj != null) {
                                //将页面数据发送到kafka
                                val pageID: String = pageObj.getString("page_id")
                                val pageItem: String = pageObj.getString("item")
                                val pageItemType: String = pageObj.getString("item_type")
                                val duringTime: Long = pageObj.getLong("during_time")
                                val lastPageID: String = pageObj.getString("last_page_id")
                                val sourceType: String = pageObj.getString("source_type")
                                //封装成PageLog
                                val pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba,
                                    pageID, lastPageID, pageItem, pageItemType, duringTime, sourceType, ts)
                                //发送到DWD_PAGE_LOG_TOPIC
                                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                                //提出曝光数据
                                val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                                if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
                                    for(i <- 0 until displaysJsonArr.size()){
                                        val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                                        //提取曝光字段
                                        val posID: String = displayObj.getString("pos_id")
                                        val displayItem: String = displayObj.getString("item")
                                        val displayItemType: String = displayObj.getString("item_type")
                                        val displayType: String = displayObj.getString("display_type")
                                        val order: String = displayObj.getString("order")
                                        //每拿到一条数据就写入到kafka中
                                        //封装成PageDisplayLog
                                        val pageDisplayLog =
                                            PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba,pageID,lastPageID,
                                                pageItem, pageItemType, duringTime, sourceType, displayItemType,
                                                displayItem,displayItemType, order,posID,ts)
                                        MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                                    }
                                }
                                //提取事件数据
                                val actionsJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                                if(displaysJsonArr != null && displaysJsonArr.size() > 0){
                                    for(i <- 0 until actionsJsonArr.size()){
                                        val actionObj: JSONObject = actionsJsonArr.getJSONObject(i)
                                        //提取事件字段
                                        val actionID: String = actionObj.getString("action_id")
                                        val actionItemType: String = actionObj.getString("item_type")
                                        val actionItem: String = actionObj.getString("item")
                                        val actionTS: Long = actionObj.getLong("ts")

                                        //封装成PageDisplayLog
                                        val pageActionLog =
                                            PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba,pageID,lastPageID,
                                                pageItem, pageItemType, duringTime, sourceType, actionID, actionItem,
                                                actionItemType, actionTS, ts)
                                        MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                                    }
                                }
                            }
                            // 启动数据
                            val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
                            if (startJsonObj != null) {
                                val entry: String = startJsonObj.getString("entry")
                                val loadingtime: Long = startJsonObj.getLong("loading_time")
                                val openAdId: String = startJsonObj.getString("open_ad_id")
                                val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                                val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")

                                //封装成StartLog
                                val startLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba,
                                    entry,openAdId,loadingtime,openAdMs,openAdSkipMs,ts)

                                //写到DWD_START_LOG_TOPIC
                                MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))

                            }
                        }

                    }
                )

            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
