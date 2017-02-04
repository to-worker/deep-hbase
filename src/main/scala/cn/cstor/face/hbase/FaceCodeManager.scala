package cn.cstor.face.hbase

import java.io.{BufferedReader, File, InputStreamReader}
import java.util
import java.util.Properties

import cn.cstor.face.common.{Config, Constants}
import cn.cstor.face.meta.DataTables
import cn.cstor.face.source.DataSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.{Get, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Data Entry in HBase.
  * Created on 2017/1/6
  *
  * @author feng.wei
  */
object FaceCodeManager {

    val dataTables = DataTables("face")

    def main(args: Array[String]) {

//        val props = {
//            val properties = new Properties()
//            properties.load(Source.fromFile(new File("conf/deep.properties"), Constants.utf8).reader())
//            properties
//        }

        val nationFile = Config.props.getProperty("nationFile")
        val perNationFile = Config.props.getProperty("perNationFile")
        val keyPersonInfoFile = Config.props.getProperty("keyPersonInfoFile")
        // face:id_code
        val tableFace = dataTables.FaceInfo
        val hTableFace: HTable = new HTable(Config.getHBaseConfig(), tableFace.name)
        // face:key_person_info
        val tableKey = dataTables.KeyPerson
        val hTableKey: HTable = new HTable(Config.getHBaseConfig(), tableKey.name)
        // face:nation
        val tableNation = dataTables.Nation
        val hTableNation: HTable = new HTable(Config.getHBaseConfig(), tableNation.name)
//         face:person_nation
        val tablePerNation = dataTables.PerNation
        val hTablePerNation: HTable = new HTable(Config.getHBaseConfig(),tablePerNation.name)

        DataSource.readXlsx(hTableNation,Config.getHadoopConfig(),nationFile,"nation")
        DataSource.readXlsx(hTableKey,Config.getHadoopConfig(), keyPersonInfoFile,"key")
        DataSource.Insert("face:key_person_info","face:id_code","key")
        DataSource.readXlsx(hTablePerNation,Config.getHBaseConfig(), perNationFile,"perNation")
        DataSource.Insert("face:person_nation","face:id_code","perNation")

                                                
        hTableFace.close()
        hTableKey.close()
        hTableNation.close()
    }
}
