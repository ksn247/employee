package com.state

import java.util.Calendar
import org.apache.spark.sql.functions.udf

/* import org.apache.spark._*/

object employee {

  def main(args: Array[String]): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("Spark CSV Reader")
      .getOrCreate

    val emp_df = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load("hdfs://master:9000/data/10000_Records_formated.csv")

    val getAge = udf( (db:String) => {
      val d = db.split("/")
      val dob = Calendar.getInstance()
      dob.set(d(2).toInt, d(0).toInt, d(1).toInt)
      val cur_dt = Calendar.getInstance()
      var age:Int = cur_dt.get(Calendar.YEAR) - dob.get(Calendar.YEAR)

      if (((dob.get(Calendar.MONTH) + 1) > (cur_dt.get(Calendar.MONTH) + 1)) ||
        (dob.get(Calendar.MONTH) == cur_dt.get(Calendar.MONTH) && dob.get(Calendar.DAY_OF_MONTH) > cur_dt.get(Calendar.DAY_OF_MONTH)) )
      {
        age = age - 1
      }
      age
    }
    )

    val emp_df1 = emp_df.withColumn("age", getAge(emp_df("dob")))
    emp_df1.write.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .save("hdfs://master:9000/data/empout")
  }
}
