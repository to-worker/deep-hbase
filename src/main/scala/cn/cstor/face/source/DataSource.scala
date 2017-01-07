package cn.cstor.face.source

import java.io.FileInputStream

import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook

/**
  * Created on 2017/1/7
  *
  * @author feng.wei
  */
object DataSource {

    def main(args: Array[String]) {
        readXlsx("E:\\cstor\\deep_compare\\data\\test.xlsx")
    }

    /**
      * HSSFWorkbook: implementation of the Excel '97(-2007) file format.
      *
      * @param path
      */
    def readXls(path: String): Unit = {
        val hSSFWorkbook = new HSSFWorkbook(
            new FileInputStream(path))

        val sheet = hSSFWorkbook.getSheet("year")
        for (rowNum <- 0 until sheet.getLastRowNum) {
            println(rowNum)
        }

    }

    /**
      * XSSF is the POI Project's pure Java implementation of the Excel 2007 OOXML (.xlsx) file format.
      *
      * @param path
      */
    def readXlsx(path: String): Unit = {
        val workbook = new XSSFWorkbook(
            new FileInputStream(path))

        val sheet = workbook.getSheet("year")
        for (i <- sheet.getFirstRowNum until (sheet.getLastRowNum)) {
            val row = sheet.getRow(i)

            if (row != null) {
                for (j <- row.getFirstCellNum.toInt until (row.getLastCellNum)) {
                    val cell = row.getCell(j)

                    if (cell != null) {
                        println(cell.getRawValue)
                    }
                }
            }
        }
    }

}
