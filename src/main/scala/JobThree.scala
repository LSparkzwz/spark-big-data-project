import java.text.SimpleDateFormat
import java.util.Date
import java.time.LocalDate
import java.time.ZoneId
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkContext}

case class Stock(ticker: String, close: Double, year: Date)

case class CompanyStock(stock: Stock, companyName: String)

case class YearlyTrend(
                        year: String,
                        var startClose: Double,
                        var yearStart: Date,
                        var endClose: Double,
                        var yearEnd: Date,
                      )

// tickers(i) = ticker, trends(i) = trends regarding ticker
case class TrendsByTicker(var tickers: Array[String], var trends: Array[Array[YearlyTrend]])


object TrendByCompany {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def seqOp(acc: TrendsByTicker, value: Stock): TrendsByTicker = {
    // Date is terrible but I'm too lazy to refactor everything to LocalDate
    // because LocalDate is incopatible with SimpleDateFormat
    // so this is a simple patch, I'm only doing this because this is just an educational project
    val valueYear = (value.year.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()).getYear()
    //if acc doesn't have the ticker of value yet we initialize the respective trends
    if (!acc.tickers.contains(value.ticker)) {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var initializedTrends = Array(
        YearlyTrend("2016", 0.toDouble, dateFormat.parse("2017-01-01"), 0.toDouble, dateFormat.parse("2015-12-31")),
        YearlyTrend("2017", 0.toDouble, dateFormat.parse("2018-01-01"), 0.toDouble, dateFormat.parse("2016-12-31")),
        YearlyTrend("2018", 0.toDouble, dateFormat.parse("2019-01-01"), 0.toDouble, dateFormat.parse("2017-12-31")))

      acc.tickers = acc.tickers :+ value.ticker
      acc.trends = acc.trends :+ initializedTrends
    }

    //we get the trends that correspond to the ticker of value and update them
    val index = acc.tickers.indexOf(value.ticker)

    acc.trends(index).foreach(yearlyTrend => {
      if (valueYear.toString == yearlyTrend.year) {
        if ((value.year).before(yearlyTrend.yearStart)) {
          yearlyTrend.yearStart = value.year
          yearlyTrend.startClose = value.close
        }
        if ((value.year).after(yearlyTrend.yearEnd)) {
          yearlyTrend.yearEnd = value.year
          yearlyTrend.endClose = value.close
        }
      }
    })
    acc
  }

  def compOp(acc1: TrendsByTicker, acc2: TrendsByTicker): TrendsByTicker = {
    //merge the trends by ticker
    acc2.tickers.foreach(ticker => {
      val acc2TrendsIndex = acc2.tickers.indexOf(ticker)
      val acc2Trends = acc2.trends(acc2TrendsIndex)

      if (!acc1.tickers.contains(ticker)) {
        acc1.tickers = acc1.tickers :+ ticker
        acc1.trends = acc1.trends :+ acc2Trends
      }
      else {
        val acc1TrendsIndex = acc1.tickers.indexOf(ticker)
        var trendsToUpdate = acc1.trends(acc1TrendsIndex)

        for (i <- 0 until trendsToUpdate.length) {
          if ((acc2Trends(i).yearStart).before(trendsToUpdate(i).yearStart)) {
            trendsToUpdate(i).yearStart = acc2Trends(i).yearStart
            trendsToUpdate(i).startClose = acc2Trends(i).startClose
          }
          if ((acc2Trends(i).yearEnd).after(trendsToUpdate(i).yearEnd)) {
            trendsToUpdate(i).yearEnd = acc2Trends(i).yearEnd
            trendsToUpdate(i).endClose = acc2Trends(i).endClose
          }
        }
        acc1.trends(acc1TrendsIndex) = trendsToUpdate
      }
    })
    acc2
  }
}


object JobThree {
  val historicalStockPrices = "/resources/historical_stock_prices.csv"
  val historicalStocks = "/resources/historical_stocks.csv"
  val spark_events = "/tmp/spark-events"
  //val historicalStockPrices = "resources/testprices.csv"
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val minDate = dateFormat.parse("2015-12-31")
  val maxDate = dateFormat.parse("2019-01-01")

  val run = () => {
    val conf = new SparkConf()
      .setAppName("Job Three")
      .setMaster("local")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", spark_events)
    val sc = new SparkContext(conf)

    val pricesRdd = sc.textFile(historicalStockPrices)
      .map(row => {
        var result = row.split(",")
        //skip header by creating a dummy row that will be filtered
        if (result(7) == "date") {
          (result(0), Stock(result(0), 0.toDouble, minDate))
        } else {
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
          val date = dateFormat.parse(result(7))
          (result(0), Stock(result(0), result(2).toDouble, date))
        }
      })
      .filter(row => {
        val date = row._2.year
        date.after(minDate) && date.before(maxDate)
      })

    val companiesRdd = sc.textFile(historicalStocks)
      .map(row => {
        val result = row.split(",")
        val wordsWithCommasCounter = row.count(_ == '"')/2;
        val numberOfCommasBeforeFirstWordWithCommas = row.split('"')(0).count(_ == ',');
        //if company name and sector have commas
        if (wordsWithCommasCounter == 2) {
          val companyName = row.split('"')(1)
          (result(0), companyName)
        }
          //if only company name has commas
        else if(wordsWithCommasCounter == 1 && numberOfCommasBeforeFirstWordWithCommas < 4){
          (result(0), row.split('"')(1))
        }
        //either only sector has commas or none has commas
        else{
          (result(0), result(2))
        }
 /*         //if sector doesn't have commas
        else {
          var companyName = result(2)
          if (result.length >= 5) { //if the company name has a "," in its name we need to avoid a wrong split
            for (i <- 3 until result.length - (wordsWithCommasCounter + 1)) {
              companyName += "," + result(i)
            }
          }
          (result(0), companyName)*/
      })

    var zeroVal = TrendsByTicker(Array[String](), Array())


    //the join returns (ticker, (Stock, companyName))
    //the first map returns (companyName, Stock)
    //the aggregate returns (companyName, TrendsByTicker)
    //so it gets the triennal trend of every ticker the company owns
    //the second map returns ((trend 2016, trend 2017, trend 2018), companyName)
    //the reduce returns ((trend 2016, trend 2017, trend 2018), companiesFollowingTheSameTrend)

    val result = pricesRdd
      .join(companiesRdd)
      .map(row => (row._2._2, row._2._1))
      .aggregateByKey(zeroVal)(TrendByCompany.seqOp, TrendByCompany.compOp)
      .map(row => {
        val trends = row._2.trends
        //keeps the sums of variances and a counter needed for the average
        var percentVarianceSums = Array(0.0, 0.0, 0.0)
        val percentVarianceCounter = trends.length
        //we have an array like this [[2016 trend, 2017 trend, 2018 trend], [2016..,2017, 2018] ...]
        // for every ticker the company has, we have to merge them
        for (i <- 0 until trends.length) {
          val trend = trends(i)
          for (j <- 0 until trend.length) {
            val percentVariance = ((trend(j).endClose - trend(j).startClose) / trend(j).startClose) * 100
            percentVarianceSums(j) += percentVariance
          }
        }
        var trendResultsBuffer = ArrayBuffer[String]()
        for (k <- 0 until percentVarianceSums.length) {
          val percentVarianceAverage = Math.round(percentVarianceSums(k) / percentVarianceCounter)
          var sign = ": "
          if (percentVarianceAverage.toInt > 0) sign += "+"
          trendResultsBuffer += "201" + (6 + k).toString + sign + percentVarianceAverage.toString + "%"
        }
        val trendResults = trendResultsBuffer.toArray
        ((trendResults(0), trendResults(1), trendResults(2)), row._1)
      })
      .reduceByKey((accumulator, value) => {
        accumulator + "; " + value
      })

      .filter(row => row._2.contains(";"))
    //.sortBy((row) => row._2) //maybe sort result by alphabetical order?

    result.take(10).foreach(println)


  }
}
