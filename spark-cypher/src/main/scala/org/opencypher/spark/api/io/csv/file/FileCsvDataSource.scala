package org.opencypher.spark.api.io.csv.file

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.io.{FileBasedGraphDataSource, FileSystemAdapter}

import scala.collection.JavaConverters._

case class FileCsvDataSource(rootPath: String)(implicit val session: CAPSSession) extends FileBasedGraphDataSource {
  self =>

  override val fs: FileSystemAdapter = new FileSystemAdapter {
    override val rootPath: String = self.rootPath

    override protected def listDirectories(path: String): Set[String] = {
      Files
        .list(Paths.get(path))
        .iterator
        .asScala
        .filter(p => Files.isDirectory(p))
        .map(p => p.getFileName.toString)
        .toSet
    }

    override protected def deleteDirectory(path: String): Unit = {
      FileUtils.deleteDirectory(Paths.get(path).toFile)
    }

    override protected def readFile(path: String): String = {
      println(s"reading file from $path")
      new String(Files.readAllBytes(Paths.get(path)))
    }

    override protected def writeFile(path: String, content: String): Unit = {
      println(s"writing file to $path")
      val file = new File(path.toString)
      file.getParentFile.mkdirs
      if (!file.exists) file.createNewFile
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(content)
      bw.close()
    }

    override protected def readTable(path: String, schema: StructType): DataFrame = {
      val df = session.sparkSession.read.schema(schema).csv(path)
      df.show()
      df.schema.printTreeString()
      df
    }

    override protected def writeTable(path: String, table: DataFrame): Unit = {
      table.show()
      table.write.csv(path) //.option("header", "true")
    }

  }
}
