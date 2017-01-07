package cn.cstor.face.source

import java.io.FileInputStream

import org.apache.poi.hssf.usermodel.HSSFWorkbook

/**
  * Created on 2017/1/7
  *
  * @author feng.wei
  */
object DataSource {

    def main(args: Array[String]) {
        readXls("E:\\cstor\\deep_compare\\data\\test.xlsx")
    }

    def readXls(path: String): Unit = {
        val hSSFWorkbook = new HSSFWorkbook(
            new FileInputStream(path))

        val sheet = hSSFWorkbook.getSheet("year")
        for (rowNum <- 0 until sheet.getLastRowNum) {
            println(rowNum)
        }

    }

}
