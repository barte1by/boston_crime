package com.example.boston_crime

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object BostonCrimes extends App {

  val crimeFile = "./src/files/crime.csv"
  val offenseCodesFile = "./src/files/offense_codes.csv"
  val resultFile = "./src/files/result"

  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "boston_crimes")
    .master(master = "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  case class Crime(
                    INCIDENT_NUMBER: Option[String],
                    OFFENSE_CODE: Option[Int],
                    OFFENSE_CODE_GROUP: Option[String],
                    OFFENSE_DESCRIPTION: Option[String],
                    DISTRICT: Option[String],
                    REPORTING_AREA: Option[String],
                    SHOOTING: Option[String],
                    OCCURRED_ON_DATE: Option[String],
                    YEAR: Option[Int],
                    MONTH: Option[Int],
                    DAY_OF_WEEK: Option[String],
                    HOUR: Option[Int],
                    UCR_PART: Option[String],
                    STREET: Option[String],
                    Lat: Option[Double],
                    Long: Option[Double],
                    Location: Option[String]
                  )
  case class OffenseCode(
                          CODE: Option[Int],
                          NAME: Option[String],
                          CRIME_TYPE: Option[String]
                        )

  val crimes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(crimeFile)
    .as[Crime]

  val offense_codes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(offenseCodesFile)
    .withColumn("CRIME_TYPE", trim(substring_index($"NAME", "-", 1)))
    .as[OffenseCode]

  val offense_codes_br = spark.sparkContext.broadcast(offense_codes)

  val filteredCrimes = crimes
    .filter($"DISTRICT".isNotNull).cache()

  val crimesWithOffenceCodes = filteredCrimes
    .join(offense_codes_br.value, filteredCrimes("OFFENSE_CODE") === offense_codes_br.value("CODE"))
    .select("INCIDENT_NUMBER", "DISTRICT", "MONTH", "Lat", "Long", "CRIME_TYPE").cache()

  val crimesDistrictAnalytics = filteredCrimes
    .groupBy($"DISTRICT")
    .agg(expr("COUNT(INCIDENT_NUMBER) as crimes_total"),
      expr("AVG(Lat) as lat"),
      expr("AVG(Long) as lng")
    )

  val crimesByDistrictByMonth = filteredCrimes
    .groupBy($"DISTRICT", $"MONTH")
    .agg(expr("count(INCIDENT_NUMBER) as CRIMES_CNT")).createOrReplaceTempView("crimesByDistrictByMonth")

  val crimesDistrictMedian = spark.sql(
    "select " +
      " DISTRICT" +
      " ,percentile(CRIMES_CNT, 0.5) as crimes_monthly " +
      " from crimesByDistrictByMonth" +
      " group by DISTRICT")

  val crimesByDistrictByCrimeTypes = crimesWithOffenceCodes
    .groupBy($"DISTRICT", $"CRIME_TYPE")
    .agg(expr("count(INCIDENT_NUMBER) as CRIMES_CNT"))
    .selectExpr("*", "row_number() over(partition by DISTRICT order by CRIMES_CNT desc) as rn")
    .filter($"rn" <= 3)
    .drop($"rn")
    .drop($"CRIMES_CNT")
    .groupBy($"DISTRICT")
    .agg(concat_ws(", ", collect_list($"CRIME_TYPE")).alias("frequent_crime_types"))

  val finalResult =
    crimesDistrictAnalytics
      .join(crimesDistrictMedian, "DISTRICT")
      .join(crimesByDistrictByCrimeTypes, "DISTRICT")
      .select($"DISTRICT", $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")


  //finalResult.repartition(1).write.mode("OVERWRITE").parquet(resultFile)
  finalResult.repartition(1).write.mode("OVERWRITE").csv(resultFile)
  finalResult.show()
}