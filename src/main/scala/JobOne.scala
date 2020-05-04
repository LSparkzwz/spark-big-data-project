import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object Helpers  {
  def getPercentVariationInterval(
                                   prevStartClose: Double,
                                   prevEndClose: Double,
                                   prevStartDate: Date,
                                   prevEndDate: Date,
                                   newClose: Double,
                                   newDate: Date): (Double, Double, Date, Date) =
  {
    if (newDate.before(prevStartDate)) {
      (newClose, prevEndClose, newDate, prevEndDate)
    } else if (newDate.after(prevEndDate)) {
      (prevStartClose, newClose, prevStartDate, newDate)
    } else {
      (prevStartClose, prevEndClose, prevStartDate, prevEndDate)
    }
  }

  //acc = startClose, endClose, startDate, endDate, minPrice, maxPrice, avgVolume
  //values = close, volumeSum, volumeCounter, date
  //returns acc
  def seqOp(acc: (Double, Double, Date, Date, Double, Double, Long, Int), values: (Double, Long, Date))
  : (Double, Double, Date, Date, Double, Double, Long, Int) =
  {
    val percentVariation = getPercentVariationInterval(acc._1, acc._2, acc._3, acc._4, values._1, values._3)
    val minPrice = if (acc._5 < values._1 && acc._5 != -1.toDouble) acc._5 else values._1
    val maxPrice = if (acc._6 > values._1) acc._6 else values._1
    val averageVolumeSum = acc._7 + values._2
    val averageVolumeCounter = acc._8 + 1

    (percentVariation._1, percentVariation._2, percentVariation._3, percentVariation._4, minPrice, maxPrice, averageVolumeSum, averageVolumeCounter)
  }

  //acc1 and 2 = startClose, endClose, startDate, endDate, minPrice, maxPrice, avgVolume
  //returns acc
  def compOp(acc1: (Double, Double, Date, Date, Double, Double, Long, Int), acc2: (Double, Double, Date, Date, Double, Double, Long, Int))
  : (Double, Double, Date, Date, Double, Double, Long, Int) =
  {
    val percentVariationStart = getPercentVariationInterval(acc1._1, acc1._2, acc1._3, acc1._4, acc2._1, acc2._3)
    val percentVariationEnd = getPercentVariationInterval(
      percentVariationStart._1,
      percentVariationStart._2,
      percentVariationStart._3,
      percentVariationStart._4,
      acc2._2,
      acc2._4)

    val minPrice = if (acc1._5 < acc2._5) acc1._5 else acc2._5
    val maxPrice = if (acc1._6 > acc2._6) acc1._6 else acc2._6
    val averageVolumeSum = acc1._7 + acc2._7
    val averageVolumeCounter = acc1._8 + acc2._8

    (percentVariationEnd._1, percentVariationEnd._2, percentVariationEnd._3, percentVariationEnd._4, minPrice, maxPrice, averageVolumeSum, averageVolumeCounter)
  }

  def roundDouble(num: Double): Double ={
    Math.round(num * 1000) / 1000
  }

  def roundLong(num: Long): Double ={
    Math.round(num * 1000) / 1000
  }

}

object JobOne {
  val historicalStockPrices = "resources/historical_stock_prices.csv"
  val historicalStocks = "resources/historical_stocks.csv"
  val spark_events = "/tmp/spark-events"
  //val historicalStockPrices = "resources/testprices.csv"
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val minDate = dateFormat.parse("2007-12-31")
  val maxDate = dateFormat.parse("2019-01-01")

  val run = () => {
    val conf = new SparkConf()
      .setAppName("Big Data Project")
      .setMaster("local")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", spark_events)
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(historicalStockPrices)
    val parsed = rdd.map(c => c.split(","))
      .mapPartitionsWithIndex { (idx, row) =>
        if (idx == 0) row.drop(1)
        else row
      }
    //gets rows between 2008 and 2018
    val filtered = parsed.filter(row => (dateFormat.parse(row(7))).after(minDate) && (dateFormat.parse(row(7))).before(maxDate))
    //turns the dataset into a rdd with the structure: ticker, (close,volumeSum,volumeCounter,date)
    val mapped = filtered.map(row => {
      val date = dateFormat.parse(row(7))
      (
        row(0), //ticker
        (
          row(2).toDouble, //close
          row(6).toLong, //volumeSum
          date) //date
      )
    })
    // startClose, endClose, startDate, endDate, minPrice, maxPrice, avgVolumeSum, avgVolumeCounter
    val zeroVal = (0.toDouble, 0.toDouble, maxDate, minDate, -1.toDouble, 0.toDouble, 0.toLong, 0)

    //aggregates by ticker
    //returns rdd as ticker, (startClose, endClose, startDate, endDate, minPrice, maxPrice, avgVolumeSum, avgVolumeCounter)
    val agg = mapped.aggregateByKey(zeroVal)(Helpers.seqOp, Helpers.compOp)

    //returns result as ticker, percentVariation, minPrice, maxPrice, avgVolume sorted by percentVariatin in descending order
    val result = agg.map(row => {
      // volumeSum / volumeCounter
      val averageVolume = row._2._7 / row._2._8
      // (startClose - endClose) / endClose * 100
      val percentVariation = ((row._2._2 - row._2._1) / row._2._1) * 100
      (row._1, percentVariation, row._2._5, row._2._6, averageVolume)
    }).sortBy((row) => row._2, false)

    result.take(10).foreach(println)

  }

}