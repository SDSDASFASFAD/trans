package beijing

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import org.apache.spark.sql.{SaveMode, SparkSession}

object export_Hive_Data {

  def main(args: Array[String]): Unit = {

    val sourceFile = args(0)
    val targetFile = args(1)

    val ses = SparkSession
      .builder()
      .appName("")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    val file = new File(sourceFile)

    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))

    val buffer = new StringBuffer()

    var line : String = null ;

    while ((line=reader.readLine())!=null){

      buffer.append(line+"/n");
    }

    val string: String = buffer.toString

    println(string)

    ses.sql(string)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite).format("csv")
      .save(targetFile)



  }

}
