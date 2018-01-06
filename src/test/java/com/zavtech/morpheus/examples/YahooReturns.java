/**
 * Copyright (C) 2014-2017 Xavier Witdouck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zavtech.morpheus.examples;

import java.awt.*;
import java.io.File;
import java.time.LocalDate;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.StatType;
import static com.zavtech.morpheus.stats.StatType.*;

import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooOptionSource;
import com.zavtech.morpheus.yahoo.YahooQuoteHistorySource;
import com.zavtech.morpheus.yahoo.YahooQuoteLiveSource;
import com.zavtech.morpheus.yahoo.YahooReturnSource;
import com.zavtech.morpheus.yahoo.YahooStatsSource;

/**
 * Yahoo Finance examples involving asset returns
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooReturns {

    /**
     * Static initializer
     */
    static {
        DataFrameSource.register(new YahooOptionSource());
        DataFrameSource.register(new YahooQuoteHistorySource());
        DataFrameSource.register(new YahooQuoteLiveSource());
        DataFrameSource.register(new YahooReturnSource());
        DataFrameSource.register(new YahooStatsSource());
    }


    @Test()
    public void assetReturns() throws Exception {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final DataFrame<LocalDate,String> returns = source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(10));
            options.withEndDate(LocalDate.now());
            options.withTickers("VWO", "VNQ", "VEA", "DBC", "VTI", "BND");
            options.cumulative();
        }).cols().mapKeys(column -> {
            switch (column.key()) {
                case "VWO": return "EM Equities";
                case "VNQ": return "Real-estate";
                case "VEA": return "Foreign Equity";
                case "DBC": return "Commodities";
                case "VTI": return "Large Blend";
                case "BND": return "Fixed Income";
                default:    return column.key();
            }
        }).applyDoubles(v -> {
            return v.getDouble() * 100d;
        });

        Chart.create().asSwing().withLinePlot(returns, chart -> {
            chart.title().withText("Major Asset Class Cumulative Returns Last 10 Years (ETF Proxies)");
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.##'%';-0.##'%'");
            chart.legend().on().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/asset_returns_1.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void smoothedReturns() throws Exception {
        String ticker = "SPY";
        YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        Array<Integer> halfLives = Array.of(0, 5, 10, 30, 60);
        DataFrame<LocalDate,String> frame = DataFrame.combineFirst(halfLives.map(halfLife -> {
            return source.read(options -> {
                options.withStartDate(LocalDate.now().minusYears(5));
                options.withEndDate(LocalDate.now());
                options.withTickers(ticker);
                options.withEmaHalfLife(halfLife.getInt());
                options.cumulative();
            }).cols().replaceKey(ticker, String.format("%s(%s)", ticker, halfLife.getInt()));
        }));

        Chart.create().withLinePlot(frame.applyDoubles(v -> v.getDouble() * 100d), chart -> {
            chart.title().withText(String.format("%s EWMA Smoothed Returns With Various Half-Lives", ticker));
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.##'%';-0.##'%'");
            chart.legend().on().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/spy_returns_1.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void normal() throws Exception {
        final double mean = 4.5;
        final double stdDev = 22.3d;
        final int obsCount = 100000;
        final double binCount = 250;
        final NormalDistribution dist = new NormalDistribution(mean, stdDev);
        final DataFrame<Integer,String> frame = DataFrame.of(Range.of(0, obsCount), String.class, columns -> {
            columns.add("Data", Double.class, v -> dist.sample());
        });

        double min = frame.stats().min();
        double max = frame.stats().max();
        double mean_ = frame.stats().mean();
        double stdDev_ = frame.stats().stdDev();
        double scale = ((max - min) / binCount) * frame.stats().count();
        DataFrame<Double,String> normDist = normal("NDIST", min, max, 200, mean_, stdDev_, scale);

        Chart.create().withHistPlot(frame, (int)binCount, chart -> {
            chart.title().withText("Gausian Fit");
            chart.plot().<String>data().add(normDist);
            chart.plot().style("NDIST").withColor(Color.BLACK).withLineWidth(2f);
            chart.plot().render(1).withLines(false, false);
            chart.plot().axes().domain().label().withText("Return");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Frequency");
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void norm() {
        String ticker = "SPY";
        YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        DataFrame<LocalDate,String> returns = source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(10));
            options.withEndDate(LocalDate.now());
            options.withTickers(ticker);
        });

        double sigmaCount = -6d;
        double mean = returns.col("SPY").stats().mean();
        double stdDev = returns.col("SPY").stats().stdDev();
        NormalDistribution dist = new NormalDistribution(mean, stdDev);
        double probability = dist.cumulativeProbability(stdDev * sigmaCount);

        IO.println("Mean = " + mean);
        IO.println("StdDev = " + stdDev);
        IO.println("Sigma event" + (stdDev * sigmaCount));
        IO.println("Probability of event = " + probability * 100d);
        IO.println("Days to occur: " + (1d / probability));
        IO.println("Years to occur: " + (1d / probability) / 252d);


        double event = stdDev * sigmaCount;
        DataFrame<Double,String> hist = returns.cols().hist(250, ticker);
        double upperKey = hist.rows().higherKey(event).get();
        double count = hist.data().getDouble(upperKey, ticker);
        IO.println(String.format("Count = %s, probability = " + count / returns.rowCount(), count));
    }


    @Test()
    public void returnsFile() {
        String ticker = "SPY";
        YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(10));
            options.withEndDate(LocalDate.now());
            options.withTickers(ticker);
        }).write().csv(options -> {
            options.setFile("/Users/witdxav/Dropbox/data/sp-daily-returns.csv");
        });
    }



    @Test()
    public void dailyReturnDist1() throws Exception {
        String ticker = "SPY";
        YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        DataFrame<LocalDate,String> returns = source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(20));
            options.withEndDate(LocalDate.now());
            options.withTickers(ticker);
        }).applyDoubles(v -> {
            return v.getDouble() * 100d;
        });

        double binCount = 200;
        double min = returns.stats().min();
        double max = returns.stats().max();
        double mean = returns.stats().mean();
        double stdDev = returns.stats().stdDev();
        double bound = Math.max(Math.abs(min), Math.abs(max));
        double scale = ((max - min) / binCount) * returns.stats().count();
        DataFrame<Double,String> normDist = normal("NDIST", -bound, bound, (int)binCount, mean, stdDev, scale);

        IO.println("Mean = " + mean);
        IO.println("StdDev = " + stdDev);
        IO.println("Bound = " + bound);

        Chart.create().withHistPlot(returns, (int)binCount, chart -> {
            chart.title().withText(ticker + ": Daily Return Frequency Distribution (20 Years)");
            chart.subtitle().withText("Extreme Events Occur More Frequently Than Normal Distribution");
            chart.plot().<String>data().add(normDist);
            chart.plot().style("NDIST").withColor(Color.BLACK).withLineWidth(2f);
            chart.plot().render(1).withLines(false, false);
            chart.plot().axes().domain().label().withText("Return");
            chart.plot().axes().domain().format().withPattern("0.000'%';-0.000'%'");
            chart.plot().axes().range(0).label().withText("Frequency");
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/return_dist_3.png"), 700, 400, true);
            chart.show(700, 400);
        });

        Thread.currentThread().join();
    }


    @Test()
    public void dailyReturnDistSmoothed() throws Exception {
        String ticker = "SPY";
        int halfLife = 20;
        YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        DataFrame<LocalDate,String> returns = source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(20));
            options.withEndDate(LocalDate.now());
            options.withTickers(ticker);
            options.withEmaHalfLife(halfLife);
        }).applyDoubles(v -> {
            return v.getDouble() * 100d;
        });

        double binCount = 200;
        double min = returns.stats().min();
        double max = returns.stats().max();
        double mean = returns.stats().mean();
        double stdDev = returns.stats().stdDev();
        double bound = Math.max(Math.abs(min), Math.abs(max));
        double scale = ((max - min) / binCount) * returns.stats().count();
        DataFrame<Double,String> normDist = normal("NDIST", -bound, bound, (int)binCount, mean, stdDev, scale);

        Chart.create().withHistPlot(returns, (int)binCount, chart -> {
            chart.title().withText(String.format("%s: Smoothed Daily Return Distribution (HL: %s)", ticker, halfLife));
            chart.plot().<String>data().add(normDist);
            chart.plot().style("NDIST").withColor(Color.BLACK).withLineWidth(2f);
            chart.plot().render(1).withLines(false, false);
            chart.plot().axes().domain().label().withText("Return");
            chart.plot().axes().domain().format().withPattern("0.000'%';-0.000'%'");
            chart.plot().axes().range(0).label().withText("Frequency");
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/return_dist_4.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }



    @Test()
    public void monthlyReturnDist() throws Exception {
        String ticker = "AAPL";
        YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        DataFrame<LocalDate,String> returns = source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(10));
            options.withEndDate(LocalDate.now());
            options.withTickers(ticker);
            options.withEmaHalfLife(20);
        }).applyDoubles(v -> {
            return v.getDouble() * 100d;
        });

        final double binCount = 250;
        double min = returns.stats().min();
        double max = returns.stats().max();
        double mean = returns.stats().mean();
        double stdDev = returns.stats().stdDev();
        double scale = ((max - min) / binCount) * returns.stats().count();
        DataFrame<Double,String> normDist = normal("NDIST", min, max, (int)binCount, mean, stdDev, scale);

        Chart.create().withHistPlot(returns, (int)binCount, chart -> {
            chart.title().withText(ticker + ": Monthly Return Frequency Distribution");
            chart.plot().<String>data().add(normDist);
            chart.plot().style("NDIST").withColor(Color.BLACK).withLineWidth(2f);
            chart.plot().render(1).withLines(false, false);
            chart.plot().axes().domain().label().withText("Return");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Frequency");
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/return_dist_2.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void returnDist2() throws Exception {
        String ticker = "AAPL";
        YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        DataFrame<LocalDate,String> dailyReturns = source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(10));
            options.withEndDate(LocalDate.now());
            options.withTickers(ticker);
            options.withEmaHalfLife(20);
        }).applyDoubles(v -> v.getDouble() * 100d);

        Chart.create().withHistPlot(dailyReturns, 250, chart -> {
            chart.title().withText(ticker + ": Daily Return Frequency Distribution (Smoothed 60 HalfLife)");
            chart.plot().axes().domain().label().withText("Return");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Frequency");
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/return_dist_2.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void returnDist3() throws Exception {
        String ticker = "AAPL";
        YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        DataFrame<LocalDate,String> dailyReturns = source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(10));
            options.withEndDate(LocalDate.now());
            options.withTickers(ticker);
            options.withEmaHalfLife(20);
            options.monthly();
        }).applyDoubles(v -> v.getDouble() * 100d);
        Chart.create().withHistPlot(dailyReturns, 250, chart -> {
            chart.title().withText(ticker + ": Monthly Return Frequency Distribution (Smoothed 60 HalfLife)");
            chart.plot().axes().domain().label().withText("Return");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Frequency");
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/return_dist_3.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    /**
     * Returns a single column DataFrame containing values generated from a normal distribution probability density function
     * @param label     the column key for the PDF series
     * @param lower     the lower bound for PDF
     * @param upper     the upper bound for PDF
     * @param count     the number of values to include
     * @param mean      the mean for the distribution
     * @param sigma     the standard deviation for the distribution
     * @return          the DataFrame of Normal PDF values
     */
    @SuppressWarnings("unchecked")
    private <C> DataFrame<Double,C> normal(C label, double lower, double upper, int count, double mean, double sigma, double scale) {
        final double stepSize = (upper - lower) / (double)count;
        final Range<Double> xValues = Range.of(lower, upper, stepSize);
        return DataFrame.of(xValues, (Class<C>)label.getClass(), columns -> {
            columns.add(label, xValues.map(x -> {
                final double part1 = 1d / (sigma * Math.sqrt(2d * Math.PI));
                final double part2 = Math.exp(-Math.pow(x - mean, 2d) / (2d * Math.pow(sigma, 2d)));
                return part1 * part2;
            }));
        }).applyDoubles(v -> {
            return v.getDouble() * scale;
        });
    }


}
