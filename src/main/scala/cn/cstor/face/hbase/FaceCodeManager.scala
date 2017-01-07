package cn.cstor.face.hbase

import java.io.{InputStreamReader, BufferedReader}
import java.util

import cn.cstor.face.common.Config
import cn.cstor.face.meta.DataTables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, Path, FileSystem}
import org.apache.hadoop.hbase.client.{Get, Put, HTable}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Data Entry in HBase.
  * Created on 2017/1/6
  *
  * @author feng.wei
  */
object FaceCodeManager {

    val dataTables = DataTables("face")

    def main(args: Array[String]) {
        // face:id_code
        val table = dataTables.FaceInfo
        val hTable: HTable = new HTable(Config.getHBaseConfig(), table.name)
        // println(getByRow(hTable, "row1", "f", "name"))
        putDataFromTextFile(hTable, Config.getHadoopConfig(), "/user/hadoop/face/features_0103.txt")
        hTable.close()
    }

    /**
      * Get the value according to the rowkey, family and qualifier.
      *
      * @param hTable
      * @param rowkey
      * @param family
      * @param qualifier
      * @return
      */
    def getByRow(hTable: HTable, rowkey: String, family: String, qualifier: String): String = {

        val result = hTable.get(new Get(Bytes.toBytes(rowkey)))
        if (!result.isEmpty) {
            Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)))
        } else "is null"
    }

    /**
      * Construnct the put qualifier and value.
      *
      * @param rowkey
      * @param family
      * @param qualifier
      * @param value
      * @return
      */
    def put(rowkey: String, family: String, qualifier: String, value: String): Put = {
        new Put(Bytes.toBytes(rowkey))
                .add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
    }

    /**
      * Construct the put contained multiple-twin wth qualifier and value.
      *
      * @param rowkey
      * @param family
      * @param map : contains qualifier and value.
      * @return
      */
    def put(rowkey: String, family: String, map: util.Map[String, String]): Put = {
        val put = new Put(Bytes.toBytes(rowkey))
        val iter = map.entrySet().iterator()
        while (iter.hasNext) {
            val entry = iter.next()
            put.add(Bytes.toBytes(family), Bytes.toBytes(entry.getKey), Bytes.toBytes(entry.getValue))
        }
        put
    }

    /**
      * put the datas that comes from the file.
      *
      * @param hTable
      * @param configuration
      * @param file :  the path to the file.
      */
    def putDataFromTextFile(hTable: HTable, configuration: Configuration, file: String): Unit = {
        val fileSystem: FileSystem = FileSystem.get(configuration)
        val path: Path = new Path(file)
        val fileStatus: FileStatus = fileSystem.getFileStatus(path)
        val inputStream: FSDataInputStream = fileSystem.open(path)
        val reader: BufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        var line: String = ""
        val count: Int = 0
        val list: util.List[Put] = new util.ArrayList[Put]
        while ((({
            line = reader.readLine;
            line
        })) != null) {

            val strs: Array[String] = line.split("#")
            val code = strs(0)
            val img_addr = strs(1)
            val id_name: String = strs(1).substring(strs(1).lastIndexOf("/") + 1, strs(1).lastIndexOf("."))
            val id: String = id_name.substring(id_name.indexOf("_") + 1)
            val name: String = id_name.substring(0, id_name.indexOf("_"))
            val map = new util.HashMap[String, String]()
            map.put("id", id)
            map.put("name", name)
            map.put("code", code)
            map.put("sex", "男")
            map.put("nation", "汉族")
            map.put("img_addr", img_addr)
            map.put("mark", "details")
            map.put("birthday", "2016-12-12")

            list.add(put(id, "f", map))

        }
        hTable.put(list)
        hTable.close
    }

}
