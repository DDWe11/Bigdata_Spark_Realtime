package com.bigdata.realtime.util


import com.bigdata.realtime.bean.{DauInfo, PageLog}

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * @descrption:实现对象属性拷贝
 */
object MyBeanUtils {
  def main(args: Array[String]): Unit = {
    val pageLog: PageLog =
      PageLog("mid1001" , "uid101" , "prov101" , null ,null ,null ,null ,
        null ,null ,null ,null ,null ,null ,0L ,null ,123456)

    val dauInfo: DauInfo = new DauInfo()
    println("拷贝前: " + dauInfo)

    copyProperties(pageLog,dauInfo)

    println("拷贝后: " + dauInfo)

  }
  /**
   * @param srcObj
   * @param destObj
   */
  def copyProperties(srcObj:AnyRef,destObj:AnyRef):Unit={
    if(srcObj == null || destObj == null){
      return
    }
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    //处理每个属性的拷贝
    for(srcField <- srcFields){
      Breaks.breakable{
        var getMethodName : String = srcField.getName
        var setMethodName : String = srcField.getName+"_$eq"
        val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
        val setMethod: Method =
          try{
            destObj.getClass.getDeclaredMethod(setMethodName,srcField.getType)
          }catch {
            case ex : Exception => Breaks.break()
          }
          //忽略val属性
          val destField: Field = destObj.getClass.getDeclaredField(srcField.getName)
          if(destField.getModifiers.equals(Modifier.FINAL)){
            Breaks.break()
          }
          //调用get方法获取到srcObj属性的值， 再调用set方法将获取到的属性值赋值给destObj的属性
          setMethod.invoke(destObj, getMethod.invoke(srcObj))

      }
      }

  }
}
