package JobTwo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

// Used the webpage https://spark.apache.org/docs/latest/rdd-programming-guide.html as reference

//calculation of the means: https://math.stackexchange.com/questions/115091/is-the-average-of-the-averages-equal-to-the-average-of-all-the-numbers-originall

public class SparkEsDue {

    //classi create nell' ottica di utilizzarlo nel metodo ReduceByKey,
    //dato che prende coppie di valori con stessa chiave e le aggrega
    //ho creato un oggetto per aggregare per rendere più ordinate le cose

    private static class YearlyTrend implements Serializable {
        public Integer year;
        public Double firstClosePrice;
        public Double lastClosePrice;
        public String firstDate;
        public String lastDate;
        public Double pricesSum;//and mean, later
        public Double volumeSum;//and mean, later
        public Integer count;

        public YearlyTrend(Integer year, Double firstClosePrice, Double lastClosePrice, String firstDate, String lastDate, Double pricesSum, Double volumeSum, Integer count) {
            this.year = year;
            this.firstClosePrice = firstClosePrice;
            this.lastClosePrice = lastClosePrice;
            this.firstDate = firstDate;
            this.lastDate = lastDate;
            this.pricesSum = pricesSum;
            this.volumeSum = volumeSum;
            this.count = count; //come fosse un wordcount, metto 1 e poi sommo
        }
    }

    private static class Trend implements Serializable {
        public Double pricesMean;
        public Double volumeMean;
        public Double percentVarMean;
        public Integer countForPriceAndVolume;
        public Integer countForPercentVar; //come un wordcount metto 1 e poi sommo

        public Trend(Double pricesMean, Double volumeMean, Double percentVarMean, Integer countForPriceAndVolume, Integer countForPercentVar) {
            this.pricesMean = pricesMean;
            this.volumeMean = volumeMean;
            this.percentVarMean = percentVarMean;
            this.countForPriceAndVolume = countForPriceAndVolume;
            this.countForPercentVar = countForPercentVar;
        }
    }

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkEsDue");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //prendo il file di testo da input
        JavaRDD<String> historicalStockPrices = sc.textFile(args[0]);
        JavaRDD<String> historicalStocks = sc.textFile(args[1]);

        String referenceStartDate = "2008-01-01";
        SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd");
        Date referenceDate = null;
        try {
            referenceDate = format.parse(referenceStartDate);
        } catch (ParseException e) {
            System.out.println("errore parsing data");
            e.printStackTrace();
        }
        //regex found at https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
        JavaRDD<String[]> splitStocks = historicalStocks.map(row -> row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
        //per il join devo fare un mapToPair
        JavaPairRDD<String, String> tickerAndCompanySectorTuple = splitStocks.filter(line -> {return !(line[0].equals("ticker"));}).
                mapToPair(line -> new Tuple2<>(line[0], line[3]));

        JavaRDD<String[]> splitStockPrices = historicalStockPrices.map(row -> row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
        Date finalReferenceDate = referenceDate; //suggested by Intellij as "variables used in Lambda expression should be final"
        JavaPairRDD<String, String[]> stockPricesTuple = splitStockPrices.filter(line -> {
            if(line[0].equals("ticker")) { return false;}
            Date actualDate = null;
            try {
                actualDate = format.parse(line[7]);
            } catch (ParseException e) {
                System.out.println("errore parsing data");
                e.printStackTrace();
            }
            return actualDate.compareTo(finalReferenceDate) > 0;
        }).mapToPair(line -> new Tuple2<>(line[0], new String[] {line[2], line[6], line[7]}));//later called pair._2().2().[...]

        //join
        JavaPairRDD<String, Tuple2<String, String[]>> joinDataset = tickerAndCompanySectorTuple.join(stockPricesTuple);

        //questo è il dataset di join, con chiave una tupla contenente ticker, settore e anno, e valore un oggetto Trend
        JavaPairRDD<Tuple3<String, String, String>, YearlyTrend> joinDatasetMRLike = joinDataset.mapToPair(
                line -> {
                    Integer year = Integer.parseInt(line._2._2[2].substring(0,4));
                    Double closePrice = Double.parseDouble(line._2()._2()[0]);
                    YearlyTrend yt = new YearlyTrend(year, closePrice, closePrice, line._2()._2()[2], line._2()._2()[2], closePrice, Double.parseDouble(line._2()._2()[1]), 1);
                    //Trend t = new Trend(Double.parseDouble(line._2()._2()[0]), Double.parseDouble(line._2()._2()[1]), 0.0,1, yt);
                        //chiavi della prima tupla sono ticker, settore, anno
                    return new Tuple2<>(new Tuple3<>(line._1(), line._2()._1(), line._2()._2()[2].substring(0,4)), yt);
                    });

        //ho scelto la strada del calcolo della media dei valori richiesti per ticker
        //quindi calcolo per prima cosa i valori per anno, per settore e per ticker (somma prezzi, somma volumi, variazioni percentuali)
        //dopodichè andrò a calcolare i valori richiesti per tutti i ticker del settore

        JavaPairRDD<Tuple3<String, String, String>, YearlyTrend> reducedDatasetByTickerYearAndSector = joinDatasetMRLike.reduceByKey(
                (firstTrend, secondTrend) -> { //dalla documentazione "the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V"
                Double aggregatedPrice = firstTrend.pricesSum + secondTrend.pricesSum;
                Double aggregatedVolume = firstTrend.volumeSum + secondTrend.volumeSum;
                int aggregatedCount = firstTrend.count + secondTrend.count;
                Date firstInitialDate = null, secondInitialDate = null;
                Date firstFinalDate = null, secondFinalDate = null;
                try {
                    firstInitialDate = format.parse(firstTrend.firstDate);
                    secondInitialDate = format.parse(secondTrend.firstDate);
                    firstFinalDate = format.parse(firstTrend.lastDate);
                    secondFinalDate = format.parse(secondTrend.lastDate);
                } catch (ParseException e) {
                        System.out.println("errore parsing data");
                        e.printStackTrace();
                }
                //intanto li inizializzo, poi controllo
                String chosenFirstDate;
                String chosenLastDate;
                Double chosenFirstPrice;
                Double chosenLastPrice;

                if(firstInitialDate.compareTo(secondInitialDate) > 0) { //la prima è più in là della seconda
                    chosenFirstDate = secondTrend.firstDate;
                    chosenFirstPrice = secondTrend.firstClosePrice;
                } else {  //la seconda è più in là della prima, o sono uguali
                    chosenFirstDate = firstTrend.firstDate;
                    chosenFirstPrice = firstTrend.firstClosePrice;
                }

                if(firstFinalDate.compareTo(secondFinalDate) > 0) { //la prima è più in là della seconda
                    chosenLastDate = firstTrend.lastDate;
                    chosenLastPrice = firstTrend.lastClosePrice;
                } else {
                    chosenLastDate = secondTrend.lastDate;
                    chosenLastPrice = secondTrend.lastClosePrice;
                }

                Integer yearCheck = firstTrend.year;
                if(!firstTrend.year.equals(secondTrend.year)) {
                    yearCheck = 0; //questo è un check per vedere se i valori sono coerenti o se sbaglio qualcosa
                }

                return new YearlyTrend(yearCheck, chosenFirstPrice, chosenLastPrice, chosenFirstDate, chosenLastDate, aggregatedPrice, aggregatedVolume, aggregatedCount);

                //return new Trend(aggregatedPrice,  aggregatedVolume, 0.0, aggregatedCount, finalyt);
                });

        //Ho provato con un foreach ma ho scoperto nel peggiore dei modi che le Tuple sono immutabili (T_T)
        /* reducedDatasetByTickerYearAndSector.foreach(line -> {
            line._2().setVolumeMean(volumeMean);
            line._2().setPricesMean(priceMean);
            line._2().setPercentVar(percentVariation);
        }); */

        //ora ottengo i valori che voglio divisi per settore e anno, tralasciando i ticker,
        //ma prima devo trasformare il tutto perchè il reduceByKey mi lega ad un determinato valore di ritorno

        JavaPairRDD<Tuple2<String, String>, Trend> newComputationByYearAndSector = reducedDatasetByTickerYearAndSector.mapToPair(
                line -> {
                    Double lastClosePrice = line._2().lastClosePrice;
                    Double firstClosePrice = line._2().firstClosePrice;
                    Double volumeSum = line._2().volumeSum;
                    Double pricesSum = line._2().pricesSum;
                    Integer count = line._2().count;

                    /*Double volumeMean = volumeSum/count;
                    Double priceMean = pricesSum/count; */
                    Double percentVariation = ((lastClosePrice - firstClosePrice)/firstClosePrice)*100;

                    return new Tuple2<>(new Tuple2<>(line._1()._2(), line._1()._3()), new Trend(pricesSum, volumeSum, percentVariation, count, 1));
                });

        JavaPairRDD<Tuple2<String, String>, Trend> finalReducedByYearAndSector = newComputationByYearAndSector.reduceByKey(
                (firstTrend, secondTrend) -> {
                    Double aggregatedVolumeForMeans = firstTrend.volumeMean + secondTrend.volumeMean;
                    Double aggregatedPriceForMeans = firstTrend.pricesMean + secondTrend.pricesMean;
                    Double aggregatedPercentVariation = firstTrend.percentVarMean + secondTrend.percentVarMean;
                    Integer countTotalForPriceAndVolume = firstTrend.countForPriceAndVolume + secondTrend.countForPriceAndVolume;
                    Integer countTotalForPercentVar = firstTrend.countForPercentVar + secondTrend.countForPercentVar;

                    return new Trend(aggregatedPriceForMeans, aggregatedVolumeForMeans, aggregatedPercentVariation, countTotalForPriceAndVolume, countTotalForPercentVar);
                });

        //calcoli della media finale e stampa dei risultati

        JavaRDD<String> finalResultAsString = finalReducedByYearAndSector.map(
                line -> {
                    Trend trendToElaborate = line._2();
                    Double finalVolumeMeans = trendToElaborate.volumeMean / trendToElaborate.countForPriceAndVolume;
                    Double finalPricesMeans = trendToElaborate.pricesMean / trendToElaborate.countForPriceAndVolume;
                    Double finalPercentVariation = trendToElaborate.percentVarMean / trendToElaborate.countForPercentVar;

                    String vol = String.format("Volume annuale medio: %.2f ", finalVolumeMeans).replace(",", ".");
                    String pv = String.format("Percentuale annuale media: %.2f", finalPercentVariation).replace(",", ".");
                    String sp = String.format("Quotazione giornaliera media: %.2f", finalPricesMeans).replace(",", ".");

                    return String.format("Risultati per il settore %s per l' anno %s : %s, %s, %s", line._1()._1(), line._1()._2(), vol, pv, sp);
                });

        finalResultAsString.saveAsTextFile(args[2]);

        sc.stop();

    }

}
