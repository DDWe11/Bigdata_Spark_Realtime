package com.bigdata.realtime.util

import java.util.ResourceBundle
/*
 * @descrption:配置文件解析类
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
  def apply(propskey:String):String = {
    bundle.getString(propskey)
  }

  def main(args: Array[String]): Unit = {
    println(MyPropsUtils.apply("kafka.bootstrap-servers"))
  }
}
