package cn.cstor.face.hbase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cls on 17-1-18.
  */
object MainTest {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apche.spark").setLevel(Level.WARN)
        Logger.getLogger("org.apche.spark").setLevel(Level.OFF)

        val sparkConf = new SparkConf()
            .setAppName("")
            .setMaster("spark://demo151:7077")
        val sc = new SparkContext(sparkConf)

//        ****************************************************

//        向hbase中插入重点人员信息表,民族对照表

//        ****************************************************

        //  解析文件内容，获取主表部分基本信息
        val path = "filePath"
        val rawRdd = sc.textFile(path)
        val line = rawRdd.flatMap(_.split("\t"))
        val result = line.map{x=>
            val str = x.split("#")
            val code = str(0)   //获取特征吗
            val id = str(1).split("_")(-2)  //获取身份证信息
            val name = "aaa"    //获取人名
            val area = id.substring(0,5)    //获取区域编号
            val birthday = id.substring(6,13) //获取出生年月信息
            //获取人员性别信息
            val num = id.substring(14,16).toInt
            if (num%2==0){
                val sex = "女"
            } else {
                val sex = "男"
            }

            val imgAddr = ""
            val message = ""
            val nation = ""

        }

        // 查询名组对照表以及重点人员信息表，补充主表内容



        // 关闭spark应用
        sc.stop()
    }
}
