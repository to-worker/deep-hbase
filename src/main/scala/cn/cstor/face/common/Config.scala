package cn.cstor.face.common

import java.io.File
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created on 2017/1/6
  * get the configuration
  *
  * @author feng.wei
  */
object Config {


    val props = {
        val properties = new Properties()
        properties.load(Source.fromFile(new File("conf/deep.properties"), Constants.utf8).reader())
        properties
    }

    def getHadoopConfig(): Configuration = {
        val configuration: Configuration = new Configuration
        configuration.set("fs.default.name", props.getProperty("fs.default.name", ""))
        configuration
    }

    def getSparkConfig(): SparkContext = {
        val conf = new SparkConf().setMaster("spark://178.68.0.34:7077")
        val sparkContext = new SparkContext(conf)
        sparkContext
    }

    def getHBaseConfig(): Configuration = {

        val conf: Configuration = HBaseConfiguration.create(getHadoopConfig())
        conf.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"))
        conf.set("hbase.zookeeper.property.clientPort", props.getProperty("hbase.zookeeper.property.clientPort"))
        conf.set("zookeeper.znode.parent", props.getProperty("zookeeper.znode.parent"))
        conf
    }

}
