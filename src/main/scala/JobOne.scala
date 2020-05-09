import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}

case class StockInfo(close: Double, volume: Long, date: Date)

case class JobOneAccumulator(
                              startClose: Double,
                              endClose: Double,
                              startDate: Date,
                              endDate: Date,
                              minPrice: Double,
                              maxPrice: Double,
                              volumeSum: Long,
                              volumeCounter: Int)

//needed to calculate the Percent Variation at a later step
case class PercentVariationInterval(startClose: Double, endClose: Double, startDate: Date, endDate: Date)

object JobOneHelpers {
  def updatePercentVariationInterval(
                                      prevStartClose: Double,
                                      prevEndClose: Double,
                                      prevStartDate: Date,
                                      prevEndDate: Date,
                                      newClose: Double,
                                      newDate: Date): PercentVariationInterval = {
    if (newDate.before(prevStartDate)) {
      PercentVariationInterval(newClose, prevEndClose, newDate, prevEndDate)
    } else if (newDate.after(prevEndDate)) {
      PercentVariationInterval(prevStartClose, newClose, prevStartDate, newDate)
    } else {
      PercentVariationInterval(prevStartClose, prevEndClose, prevStartDate, prevEndDate)
    }
  }

  //acc = Accumulator
  //values = StockInfo
  //returns acc
  def seqOp(acc: JobOneAccumulator, value: StockInfo): JobOneAccumulator = {

    val percentVariationInterval = updatePercentVariationInterval(acc.startClose, acc.endClose, acc.startDate, acc.endDate, value.close, value.date)
    val minPrice = if (acc.minPrice < value.close && acc.minPrice != -1.toDouble) acc.minPrice else value.close
    val maxPrice = if (acc.maxPrice > value.close) acc.maxPrice else value.close
    val volumeSum = acc.volumeSum + value.volume
    val volumeCounter = acc.volumeCounter + 1

    JobOneAccumulator(
      percentVariationInterval.startClose,
      percentVariationInterval.endClose,
      percentVariationInterval.startDate,
      percentVariationInterval.endDate,
      minPrice,
      maxPrice,
      volumeSum,
      volumeCounter)
  }

  //combines the accumulators
  def compOp(acc1: JobOneAccumulator, acc2: JobOneAccumulator): JobOneAccumulator = {
    //first we see if the acc2.startClose can update acc1's value
    val percentVariationStart = updatePercentVariationInterval(acc1.startClose, acc1.endClose, acc1.startDate, acc1.endDate, acc2.startClose, acc2.startDate)
    //then we do the same for acc2.endClose
    val percentVariationEnd = updatePercentVariationInterval(
      percentVariationStart.startClose,
      percentVariationStart.endClose,
      percentVariationStart.startDate,
      percentVariationStart.endDate,
      acc2.endClose,
      acc2.endDate)

    val minPrice = Math.min(acc1.minPrice, acc2.minPrice)
    val maxPrice = Math.max(acc1.maxPrice, acc2.maxPrice)
    val volumeSum = acc1.volumeSum + acc2.volumeSum
    val volumeCounter = acc1.volumeCounter + acc1.volumeCounter

    JobOneAccumulator(
      percentVariationEnd.startClose,
      percentVariationEnd.endClose,
      percentVariationEnd.startDate,
      percentVariationEnd.endDate,
      minPrice,
      maxPrice,
      volumeSum,
      volumeCounter)
  }

  def roundDouble(num: Double): Double = {
    Math.round(num * 100) / 100
  }

  def roundLong(num: Long): Double = {
    Math.round(num * 100) / 100
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
      .setAppName("Job One")
      .setMaster("local")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", spark_events)
    val sc = new SparkContext(conf)

    //map returns (ticker, StockInfo)
    //filter filters by StockInfo.date between 2008 and 2018
    val rdd = sc.textFile(historicalStockPrices)
      .map(row => {
        var result = row.split(",")
        //skip header by creating a dummy row that will be filtered
        if (result(7) == "date") {
          //if header, we generate dummy data that will be filtered
          (result(0), StockInfo(0.toDouble, 0.toLong, minDate))
        } else {
          val date = dateFormat.parse(result(7))
          (result(0), StockInfo(result(2).toDouble, result(6).toLong, date))
        }
      })
      .filter(row => (row._2.date).after(minDate) && (row._2.date).before(maxDate))

    // startClose, endClose, startDate, endDate, minPrice, maxPrice, volumeSum, volumeCounter
    val zeroVal = JobOneAccumulator(0.toDouble, 0.toDouble, maxDate, minDate, -1.toDouble, 0.toDouble, 0.toLong, 0)


    //aggregate returns (ticker, JobOneAccumulator)
    //map calculates the percentVariation and averageVolume
    //sort sorts by percentVariation
    val result = rdd
      .aggregateByKey(zeroVal)(JobOneHelpers.seqOp, JobOneHelpers.compOp)
      .map(row => {
        val averageVolume = row._2.volumeSum / row._2.volumeCounter
        val percentVariation = ((row._2.endClose - row._2.startClose) / row._2.startClose) * 100
        (
          row._1,
          percentVariation,
          row._2.minPrice,
          row._2.maxPrice,
          averageVolume
        )
      })
      .sortBy((row) => row._2, false)


    result.take(10).foreach(println)

  }

}