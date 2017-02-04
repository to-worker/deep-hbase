package cn.cstor.face.source

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util

import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{HTable, HTablePool, _}
import org.apache.hadoop.hbase.util.Bytes;

/**
  * Created on 2017/1/7
  *
  * @author feng.wei
  */
object DataSource {

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
        while (({
            line = reader.readLine;
            line
        }) != null) {
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
//            map.put("sex", "男")
//            map.put("nation", "汉族")
//            map.put("img_addr", img_addr)
//            map.put("mark", "details")
//            map.put("birthday", "2016-12-12")
            list.add(put(id, "f", map))
        }
        hTable.put(list)
        hTable.close
    }

    /**
      * HSSFWorkbook: implementation of the Excel '97(-2007) file format.
      *
      * @param path
      */
    def readXls(path: String): Unit = {
        val hSSFWorkbook = new HSSFWorkbook(new FileInputStream(path))

        val sheet = hSSFWorkbook.getSheet("year")
        for (rowNum <- 0 until sheet.getLastRowNum) {
            println(rowNum)
        }

    }

    /**
      * XSSF is the POI Project's pure Java implementation of the Excel 2007 OOXML (.xlsx) file format.
      *
      * @param filePath
      */
    def readXlsx(hTable: HTable,configuration: Configuration,filePath: String,choice : String): Unit = {

        val workbook = new XSSFWorkbook(new FileInputStream(filePath))

        val list: util.List[Put] = new util.ArrayList[Put]
        val sheet = workbook.getSheet("sheet1")

        for (i <- sheet.getFirstRowNum.toInt until (sheet.getLastRowNum+1)) {
            val row = sheet.getRow(i)
            val map = new util.HashMap[String, String]()
            val rank1 = row.getCell(0).getStringCellValue
            val rank2 = row.getCell(1).getStringCellValue
            choice match {
                case "key" =>{
                    map.put("id", rank1)
                    map.put("message",rank2)
                    list.add(put(rank1, "message", map))
                }
                case "nation" =>{
                    map.put("nation",rank1)
                    map.put("num",rank2)
                    list.add(put(rank1, "num", map))
                }
                case "perNation" =>{
                    map.put("id",rank1)
                    map.put("num",rank2)
                    list.add(put(rank1,"num",map))
                }
            }
//            if (row != null) {
//                for (j <- row.getFirstCellNum until (row.getLastCellNum)) {
//                    val cell = row.getCell(j)
//                    println(cell.getRawValue)
//                    println(cell.getStringCellValue)
//                }
//            }

        }
        hTable.put(list)
        hTable.close
    }

    def Insert(fromTableName:String,toTableName:String,choice : String): Unit ={
        val conf = new Configuration()
        val list: util.List[Put] = new util.ArrayList[Put]

        val fTable = new HTable(conf, fromTableName)
        val tTable = new HTable(conf, toTableName)
        val nTable = new HTable(conf,"face:nation")
        val scan=new Scan()
        scan.setBatch(1000)
        val rs = fTable.getScanner(scan);

        val results:util.Iterator[Result] = rs.iterator()
        while (results.hasNext){
            val r = results.next()
            val id = Bytes.toString(r.getRow())

            val get = new Get(Bytes.toBytes(id))
            val last =tTable.get (get)
            if(last.value()!=null){
                choice match {
                    case "key" =>{
                        val message = getByRow(fTable,id,"message","message")
                        list.add(put(id,"f","mark",message))
                    }
                    case "perNation" =>{
                        val num = getByRow(fTable,id,"num","num")
                        val nation = getByRow(nTable,num,"num","num")
                        list.add(put(id,"f","nation",nation))
                    }
                }

            } else {
                println("Not Found")
            }
        }
        tTable.put(list)
        rs.close();
        tTable.close()

    }
}
