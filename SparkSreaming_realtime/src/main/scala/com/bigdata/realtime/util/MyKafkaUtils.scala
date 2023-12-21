package com.bigdata.realtime.util

import java.util

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/*
 * kafka工具类，用于生产和消费数据
 */
object MyKafkaUtils {
  /*
   * 生产者对象
   */
  val producer : KafkaProducer[String,String] = createProducer()

  /*
   * 创建生产者对象
   */
  def createProducer():KafkaProducer[String,String] ={
    val producerConfigs:util.HashMap[String,AnyRef] = new util.HashMap[String,AnyRef]
    //生产者配置类ProducerConfig
    //kafka集群位置
//    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop1:9092,hadoop2:9092,hadoop3:9092")
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))

    // kv序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    // acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG,"all")
    // batch.size
    producerConfigs.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384")
    // linger.ms
    producerConfigs.put(ProducerConfig.LINGER_MS_CONFIG,"1")
    // 幂等配置
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  /*
   * 生产数据(按照默认的黏性分区策略）
   */
  def send(topic:String,msg:String): Unit ={
    producer.send(new ProducerRecord[String,String](topic,msg))
  }
  /*
 * 生产数据(按照key进行分区）
 */
  def send(topic:String,key:String,msg:String): Unit ={
    producer.send(new ProducerRecord[String,String](topic,key,msg))
  }
  /*
   * 关闭生产者对象
   */
  def closeProducer(): Unit ={
    if(producer != null) producer.close()
  }
  /*
   * 刷写 ，将缓冲区的数据刷写到磁盘
   */
  def flush(): Unit ={
    producer.flush()
  }
  /*
   * 消费者配置
   */
  private val consumerConfigs: mutable.Map[String,Object] = mutable.Map[String, Object](
    //kafka集群位置
//    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop1:9092,hadoop2:9092,hadoop3:9092",
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),

    //kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //groupID

    //offset提交 ”true”,“false”
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",

    //offset重置 “earliest”,“latest”,“none”
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

  /*
   * 基于SparkStreaming消费数据,获得到KafkaDStream,使用默认offset
   */
  def getKafkaDStream(ssc : StreamingContext ,topic: String , groupID:String ) ={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    val kafkaDStream:InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }
  /*
 * 基于SparkStreaming消费数据,获得到KafkaDStream,使用指定的offset
 */
  def getKafkaDStream(ssc : StreamingContext ,topic: String , groupID:String ,offsets: Map[TopicPartition, Long]) ={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    val kafkaDStream:InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets))
    kafkaDStream
  }
}
