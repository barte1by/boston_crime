package com.example.boston_crime

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{expr, _}

object BostonCrimesMap extends App {

//  val crimeFile = "./src/files/crime.csv"
//  val offenseCodesFile = "./src/files/offense_codes.csv"
//  val resultFile = "./src/files/result"
  val crimeFile = args(0)
  val offenseCodesFile = args(1)
  val resultFile = args(2)


  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "BostonCrimesMap")
    .master(master = "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  //case class for file crime.csv
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
  //case class for file offensecodes.csv
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
    .withColumn( colName = "CRIME_TYPE", trim(substring_index(str= $"NAME", delim ="-", count = 1)))
    .as[OffenseCode]

  //add val broadcast
  val broadcast_offense_codes = sc.broadcast(offense_codes)


  //val countDistrict = crimes.groupBy("DISTRICT").count().show()
  val filterCrimes = crimes
    .filter($"DISTRICT".isNotNull)

  //Jois Codes to Crime
  val crimesPlusOffenceCodes = filterCrimes
    .join(broadcast_offense_codes.value, filterCrimes("OFFENSE_CODE") === broadcast_offense_codes.value("CODE"))
    .select( col = "INCIDENT_NUMBER", cols = "DISTRICT", "MONTH", "Lat", "Long", "CRIME_TYPE")
    .cache()

  val crimesDistrictTotal = filterCrimes
    .groupBy(cols =$"DISTRICT")
    .agg(expr(expr = "count(INCIDENT_NUMBER) as crimes_total"))

  //val countDistrict = crimes.filter($"Lat".isNull).groupBy($"Lat").count().show()
  val crimesDistrictCord = filterCrimes
    .groupBy($"DISTRICT")
    .agg(expr("AVG(Lat) as lat"), expr("AVG(Long) as lng"))
//  crimesDistrictCord.show()

  val crimesDistrictPlusTypes = crimesPlusOffenceCodes
    .groupBy($"DISTRICT", $"CRIME_TYPE")
    .agg(expr("count(INCIDENT_NUMBER) as CRIMES_MON"))
    .selectExpr("*", "row_number() over(partition by DISTRICT order by CRIMES_MON desc) as CR_TY")
    .filter($"CR_TY" <= 3)
    .drop($"CR_TY")
    .drop($"CRIMES_MON")
    .groupBy($"DISTRICT")
    .agg(concat_ws(", ", collect_list($"CRIME_TYPE")).alias("frequent_crime_types"))


  val crimesDistrictMonth = filterCrimes
    .groupBy(cols =$"DISTRICT", $"MONTH")
    .agg(expr(expr = "count(INCIDENT_NUMBER) as CRIMES_MON")).createOrReplaceTempView("crimesDistrictMonth")

  val crimesDistrictMedian = spark.sql(
      "select " +
        " DISTRICT" +
        " ,percentile(CRIMES_MON, 0.5) as crimes_monthly " +
        " from crimesDistrictMonth" +
        " group by DISTRICT")

  val Final =
    crimesDistrictCord
      .join(crimesDistrictTotal, "DISTRICT")
      .join(crimesDistrictMedian, "DISTRICT")
      .join(crimesDistrictPlusTypes, "DISTRICT")
      .select($"DISTRICT", $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")

  Final.repartition(1).write.mode("OVERWRITE").parquet(resultFile)
  //Final.repartition(1).write.mode("OVERWRITE").csv(resultFile)
  //Final.show()
}