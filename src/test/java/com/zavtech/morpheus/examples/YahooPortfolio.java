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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooFinance;
import com.zavtech.morpheus.yahoo.YahooOptionSource;
import com.zavtech.morpheus.yahoo.YahooQuoteHistorySource;
import com.zavtech.morpheus.yahoo.YahooQuoteLiveSource;
import com.zavtech.morpheus.yahoo.YahooReturnSource;
import com.zavtech.morpheus.yahoo.YahooStatsSource;
import com.zavtech.morpheus.frame.DataFramePCA.Field;

/**
 *
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooPortfolio {

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


    /**
     * A function that generates N long only random portfolios with weights the sum to 1
     * @param count     the number of portfolios / rows in the DataFrame
     * @param tickers   the security tickers to include
     * @return          the frame of N random portfolios, 1 per row
     */
    DataFrame<Integer,String> randomPortfolios(int count, Iterable<String> tickers) {
        DataFrame<Integer,String> weights = DataFrame.ofDoubles(Range.of(0, count), tickers);
        weights.applyDoubles(v -> Math.random());
        weights.rows().forEach(row -> {
            final double sum = row.stats().sum();
            row.applyDoubles(v -> {
                double weight = v.getDouble();
                return weight / sum;
            });
        });
        return weights;
    }


    @Test
    public void cumReturns() throws Exception {
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        Array<String> tickers = Array.of("AAPL", "AMZN");

        YahooFinance yahoo = new YahooFinance();
        DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);
        cumReturns.applyDoubles(v -> v.getDouble() * 100d);

        Chart.create().withLinePlot(cumReturns, chart -> {
            chart.title().withText("Cumulative Asset Returns");
            chart.subtitle().withText("Range: (" + start + " to " + end + ")");
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().on().right();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_0.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();

    }


    @Test()
    public void riskReturnTradeOff() throws Exception {
        //Define portfolio count, investment horizon and universe
        int count = 10000;
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        Array<String> tickers = Array.of("AAPL", "AMZN");

        //Grab daily returns and cumulative returns from Yahoo Finance
        YahooFinance yahoo = new YahooFinance();
        DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(start, end, tickers);
        DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);

        //Compute asset covariance matrix from daily returns and annualize
        DataFrame<String,String> sigma = dayReturns.cols().stats().covariance().applyDoubles(v -> v.getDouble() * 252);
        DataFrame<LocalDate,String> assetReturns = cumReturns.rows().last().map(DataFrameRow::toDataFrame).get();

        //Generate random portfolios and compute risk & return for each
        DataFrame<Integer,String> portfolios = randomPortfolios(count, tickers);
        DataFrame<Integer,String>  results = DataFrame.ofDoubles(Range.of(0, count), Array.of("Risk", "Return"));
        portfolios.rows().forEach(row -> {
            DataFrame<Integer,String> weights = row.toDataFrame();
            double portReturn = weights.dot(assetReturns.transpose()).data().getDouble(0, 0);
            double portVariance = weights.dot(sigma).dot(weights.transpose()).data().getDouble(0, 0);
            results.data().setDouble(row.key(), "Return", portReturn * 100d);
            results.data().setDouble(row.key(), "Risk", Math.sqrt(portVariance) * 100d);
        });

        //Plot the results using a scatter plot
        Chart.create().withScatterPlot(results, false, "Risk", chart -> {
            chart.title().withText("Risk / Return Profiles For AAPL+AMZN Portfolios");
            chart.subtitle().withText(count + " Portfolio Combinations Simulated");
            chart.plot().axes().domain().label().withText("Portfolio Risk");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_1.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }



    @Test()
    public void comparison1() throws Exception {

        //Define portfolio count, investment horizon
        int count = 10000;
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        YahooFinance yahoo = new YahooFinance();

        Array<DataFrame<Integer,String>> results = Array.of(
            Array.of("VTI", "BND"),
            Array.of("AAPL", "AMZN"),
            Array.of("GOOGL", "BND"),
            Array.of("ORCL", "KO"),
            Array.of("VWO", "VNQ")
        ).map(v -> {
            //Access tickers
            Array<String> tickers = v.getValue();
            //Grab daily returns and cumulative returns from Yahoo Finance
            DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(start, end, tickers);
            DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);
            //Compute asset covariance matrix from daily returns and annualize
            DataFrame<String,String> sigma = dayReturns.cols().stats().covariance().applyDoubles(x -> x.getDouble() * 252);
            DataFrame<LocalDate,String> assetReturns = cumReturns.rows().last().map(DataFrameRow::toDataFrame).get();
            //Generate random portfolios and compute risk & return for each
            String label = String.format("%s+%s", tickers.getValue(0), tickers.getValue(1));
            DataFrame<Integer,String> portfolios = randomPortfolios(count, tickers);
            DataFrame<Integer,String>  riskReturn = DataFrame.ofDoubles(Range.of(0, count), Array.of("Risk", label));
            portfolios.rows().forEach(row -> {
                DataFrame<Integer,String> weights = row.toDataFrame();
                double portReturn = weights.dot(assetReturns.transpose()).data().getDouble(0, 0);
                double portVariance = weights.dot(sigma).dot(weights.transpose()).data().getDouble(0, 0);
                riskReturn.data().setDouble(row.key(), label, portReturn * 100d);
                riskReturn.data().setDouble(row.key(), "Risk", Math.sqrt(portVariance) * 100d);
            });

            return riskReturn;
        });

        DataFrame<Integer,String> first = results.getValue(0);
        Chart.create().<Integer,String>withScatterPlot(first, false, "Risk", chart -> {
            for (int i=1; i<results.length(); ++i) {
                chart.plot().<String>data().add(results.getValue(i), "Risk");
                chart.plot().render(i).withDots();
            }
            chart.plot().axes().domain().label().withText("Portfolio Risk");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.title().withText("Risk / Return Profiles of Various Two Asset Portfolios");
            chart.subtitle().withText(count + " Portfolio Combinations Simulated");
            chart.legend().on().right();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_2.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();

    }


    /**
     *  VTI: 35%
     *  VEA: 21%
     *  VWO: 16%
     *  VTEB: 15%
     *  VIG: 9%
     *  XLE: 5%
     *
     *
     * @throws Exception
     */
    @Test()
    public void comparison2() throws Exception {
        //Define portfolio count, investment horizon
        int count = 10000;
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        YahooFinance yahoo = new YahooFinance();

        Array<DataFrame<Integer,String>> results = Array.of(
            Array.of("VWO", "VNQ"),
            Array.of("VWO", "VNQ", "VEA"),
            Array.of("VWO", "VNQ", "VEA", "DBC"),
            Array.of("VWO", "VNQ", "VEA", "DBC", "VTI"),
            Array.of("VWO", "VNQ", "VEA", "DBC", "VTI", "BND")
        ).map(v -> {
            //Access tickers
            Array<String> tickers = v.getValue();
            //Grab daily returns and cumulative returns from Yahoo Finance
            DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(start, end, tickers);
            DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);
            //Compute asset covariance matrix from daily returns and annualize
            DataFrame<String,String> sigma = dayReturns.cols().stats().covariance().applyDoubles(x -> x.getDouble() * 252);
            DataFrame<LocalDate,String> assetReturns = cumReturns.rows().last().map(DataFrameRow::toDataFrame).get();
            //Generate random portfolios and compute risk & return for each
            String label = String.format("%s Assets", tickers.length());
            DataFrame<Integer,String> portfolios = randomPortfolios(count, tickers);
            DataFrame<Integer,String>  riskReturn = DataFrame.ofDoubles(Range.of(0, count), Array.of("Risk", label));
            portfolios.rows().forEach(row -> {
                DataFrame<Integer,String> weights = row.toDataFrame();
                double portReturn = weights.dot(assetReturns.transpose()).data().getDouble(0, 0);
                double portVariance = weights.dot(sigma).dot(weights.transpose()).data().getDouble(0, 0);
                riskReturn.data().setDouble(row.key(), 1, portReturn * 100d);
                riskReturn.data().setDouble(row.key(), 0, Math.sqrt(portVariance) * 100d);
            });
            return riskReturn;
        });

        DataFrame<Integer,String> first = results.getValue(0);
        Chart.create().<Integer,String>withScatterPlot(first, false, "Risk", chart -> {
            for (int i=1; i<results.length(); ++i) {
                chart.plot().<String>data().add(results.getValue(i), "Risk");
                chart.plot().render(i).withDots();
            }
            chart.plot().axes().domain().label().withText("Portfolio Risk");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.title().withText("Risk / Return Profiles of Portfolios With Increasing Assets");
            chart.subtitle().withText(count + " Portfolio Combinations Simulated");
            chart.legend().on().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_3.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();

    }


    /**
     *  VTI: 35%
     *  VEA: 21%
     *  VWO: 16%
     *  VTEB: 15%
     *  VIG: 9%
     *  XLE: 5%
     * @throws Exception
     */
    @Test()
    public void wealthFront1() throws Exception {
        //Defines investment horizon & universe
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        //Define investment universe and weights
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        //Define DataFrame of position weights suggested by Wealthfront
        DataFrame<String,String> portfolio = DataFrame.of(tickers, String.class, columns -> {
            columns.add("Weights", Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d));
        });
        //Grab daily returns and cumulative returns from Yahoo Finance
        YahooFinance yahoo = new YahooFinance();
        DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(start, end, tickers);
        DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);
        //Compute asset covariance matrix from daily returns and annualize
        DataFrame<String,String> sigma = dayReturns.cols().stats().covariance().applyDoubles(x -> x.getDouble() * 252);
        DataFrame<LocalDate,String> assetReturns = cumReturns.rows().last().map(DataFrameRow::toDataFrame).get();
        //Generate DataFrame of portfolio weights
        double portReturn = portfolio.transpose().dot(assetReturns.transpose()).data().getDouble(0, 0);
        double portVariance = portfolio.transpose().dot(sigma).dot(portfolio).data().getDouble(0, 0);
        IO.println(String.format("Portfolio Return: %s", portReturn));
        IO.println(String.format("Portfolio Risk: %s", Math.sqrt(portVariance)));
    }


    @Test()
    public void wealthfront2() throws Exception {
        //Define portfolio count, investment horizon and universe
        int count = 100000;
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");

        //Grab daily returns and cumulative returns from Yahoo Finance
        YahooFinance yahoo = new YahooFinance();
        DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(start, end, tickers);
        DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);

        //Compute asset covariance matrix from daily returns and annualize
        DataFrame<String,String> sigma = dayReturns.cols().stats().covariance().applyDoubles(v -> v.getDouble() * 252);
        DataFrame<LocalDate,String> assetReturns = cumReturns.rows().last().map(DataFrameRow::toDataFrame).get();

        //Generate random portfolios and compute risk & return for each
        DataFrame<Integer,String> portfolios = randomPortfolios(count, tickers);
        DataFrame<Integer,String>  results = DataFrame.ofDoubles(Range.of(0, count), Array.of("Risk", "Random"));
        portfolios.rows().forEach(row -> {
            DataFrame<Integer,String> weights = row.toDataFrame();
            double portReturn = weights.dot(assetReturns.transpose()).data().getDouble(0, 0);
            double portVariance = weights.dot(sigma).dot(weights.transpose()).data().getDouble(0, 0);
            results.data().setDouble(row.key(), "Random", portReturn * 100d);
            results.data().setDouble(row.key(), "Risk", Math.sqrt(portVariance) * 100d);
        });

        //Create DataFrame with risk / return of proposed Wealthfront portfolio
        DataFrame<Integer,String> proposed = DataFrame.of(Range.of(0, 1), String.class, cols -> {
            cols.add("Risk", Array.of(0.068950 * 100d));
            cols.add("Chosen", Array.of(0.1587613 * 100d));
        });

        //Plot the results using a scatter plot
        Chart.create().withScatterPlot(results, false, "Risk", chart -> {
            chart.title().withText("Risk / Return Profile For Wealthfront Portfolio");
            chart.subtitle().withText(count + " Portfolio Combinations Simulated");
            chart.plot().<String>data().add(proposed, "Risk");
            chart.plot().render(1).withDots();
            chart.plot().axes().domain().label().withText("Portfolio Risk");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().on().right();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_5.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();

    }


    @Test()
    public void pca_1() throws Exception {

        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        YahooFinance yahoo = new YahooFinance();

        Array<DataFrame<String,String>> explainedVariance = Array.of(
            Array.of("VTI", "BND", "VWO", "VTEB", "VIG", "XLE"),
            Array.of("FB", "AAPL", "AMZN", "NFLX", "GOOGL", "MSFT")
        ).map(v -> {
            Array<String> tickers = v.getValue();
            DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(start, end, tickers);
            DataFrame<String,String> correl = dayReturns.cols().stats().covariance();
            return correl.pca().apply(false, model -> {
                DataFrame<Integer,Field> eigenFrame = model.getEigenValues();
                DataFrame<Integer,Field> varPercent = eigenFrame.cols().select(Field.VAR_PERCENT);
                return Optional.of(varPercent.rows()
                    .mapKeys(row -> row.key().toString()).cols()
                    .mapKeys(col -> "Portfolio-" + v.index())
                    .applyDoubles(x -> x.getDouble() * 100d));
            }).get();
        });

        //Combined frames and plot
        DataFrame<String,String> combined = DataFrame.combineFirst(explainedVariance);
        Chart.create().withBarPlot(combined, false, chart -> {
            chart.title().withText("Explained Variance");
            chart.legend().on().right();
            chart.show();
        });

        Thread.currentThread().join();

    }




    @Test()
    public void portRet1() throws Exception {

        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        DataFrame<LocalDate,String> result = getEvolvedWeights(start, end, Collect.asOrderedMap(map -> {
            map.put("VTI", 0.35d);
            map.put("VEA", 0.21d);
            map.put("VWO", 0.16d);
            map.put("VTEB", 0.15d);
            map.put("VIG", 0.08d);
            map.put("XLE", 0.05d);
        }));

        result.tail(10).out().print();

    }



    @Test()
    public void portRet2() throws Exception {

        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);
        DataFrame<LocalDate,String> cumReturns = getPortfolioCumReturns(start, end, Collect.asOrderedMap(map -> {
            map.put("VTI", 0.35d);
            map.put("VEA", 0.21d);
            map.put("VWO", 0.16d);
            map.put("VTEB", 0.15d);
            map.put("VIG", 0.08d);
            map.put("XLE", 0.05d);
        }));

        cumReturns.cols().add("Total", Double.class, v -> v.row().stats().sum());
        cumReturns.applyDoubles(v -> v.getDouble() * 100d);

        Chart.create().withLinePlot(cumReturns, chart -> {
            chart.title().withText("Cumulative Portfolio Returns");
            chart.subtitle().withText("Robo-Visor Portfolio");
            chart.plot().style("Total").withColor(Color.BLACK).withLineWidth(1.5f);
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return (%)");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().on().right();
            chart.show();
        });

        Thread.currentThread().join();

    }


    @Test()
    public void riskWeighted() throws Exception {
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusYears(1);

        Map<String,Double> weights1 = Collect.asOrderedMap(map -> {
            map.put("VTI", 0.35d);
            map.put("VEA", 0.21d);
            map.put("VWO", 0.16d);
            map.put("VTEB", 0.15d);
            map.put("VIG", 0.08d);
            map.put("XLE", 0.05d);
        });

        final List<String> tickers = weights1.keySet().stream().collect(Collectors.toList());
        Map<String,Double> weights2 = getRiskBalancedWeights(tickers, start, end);
        DataFrame<LocalDate,String> cumReturns = getPortfolioCumReturns(start, end, weights2);

        cumReturns.cols().add("Total", Double.class, v -> v.row().stats().sum());
        cumReturns.applyDoubles(v -> v.getDouble() * 100d);

        Chart.create().withLinePlot(cumReturns, chart -> {
            chart.title().withText("Cumulative Portfolio Returns");
            chart.subtitle().withText("Robo-Visor Portfolio");
            chart.plot().style("Total").withColor(Color.BLACK).withLineWidth(1.5f);
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return (%)");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().on().right();
            chart.show();
        });

        Thread.currentThread().join();
    }


    /**
     * Returns portfolio weights that are scaled so as to be risk balanced
     * @param tickers   the asset tickers
     * @param start     the start date for calibration
     * @param end       the end date for calibration
     * @return          the map of risk balanced weights
     */
    private Map<String,Double> getRiskBalancedWeights(Iterable<String> tickers, LocalDate start, LocalDate end) {
        YahooFinance yahoo = new YahooFinance();
        DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(start, end, tickers);
        DataFrame<String,StatType> variance = dayReturns.cols().stats().variance();
        DataFrame<String,StatType> reciprocal = variance.applyDoubles(v -> 1d / v.getDouble());
        return Collect.asOrderedMap(map -> {
            reciprocal.forEachValue(v -> {
                final String ticker = v.rowKey();
                final double value = v.getDouble();
                final double sum = v.col().stats().sum();
                final double weight = value / sum;
                map.put(ticker, weight);
            });
        });
    }



    /**
     * Calculates portfolio position day returns over a date range given some starting weights
     * @param start     the start date to evolve weights from
     * @param end       the end date to evolve weights to
     * @param initial   the initial weights at start date
     * @return          the evolving portfolio weights
     */
    public DataFrame<LocalDate,String> getPortfolioDayReturns(LocalDate start, LocalDate end, Map<String,Double> initial) {
        final YahooFinance yahoo = new YahooFinance();
        final List<String> tickers = initial.keySet().stream().collect(Collectors.toList());
        final DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);
        return cumReturns.copy().applyDoubles(v -> {
            final int rowOrdinal = v.rowOrdinal();
            if (rowOrdinal == 0) {
                return 0d;
            } else {
                int colOrdinal = v.colOrdinal();
                double initWeight = initial.get(v.colKey());
                double priorRet = cumReturns.data().getDouble(rowOrdinal-1, colOrdinal);
                double currentRet = cumReturns.data().getDouble(rowOrdinal, colOrdinal);
                double priorWeight = initWeight * (1d + priorRet);
                double currentWeight = initWeight * (1d + currentRet);
                return currentWeight / priorWeight - 1d;
            }
        });
    }


    /**
     * Calculates portfolio position cumulative returns over a date range given some starting weights
     * @param start     the start date to evolve weights from
     * @param end       the end date to evolve weights to
     * @param initial   the initial weights at start date
     * @return          the evolving portfolio weights
     */
    public DataFrame<LocalDate,String> getPortfolioCumReturns(LocalDate start, LocalDate end, Map<String,Double> initial) {
        final YahooFinance yahoo = new YahooFinance();
        final List<String> tickers = initial.keySet().stream().collect(Collectors.toList());
        final DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);
        return cumReturns.copy().applyDoubles(v -> {
            final int rowOrdinal = v.rowOrdinal();
            if (rowOrdinal == 0) {
                return 0d;
            } else {
                int colOrdinal = v.colOrdinal();
                double initialWeight = initial.get(v.colKey());
                double cumReturn = cumReturns.data().getDouble(rowOrdinal, colOrdinal);
                return cumReturn * initialWeight;
            }
        });
    }


    /**
     * Returns evolving portfolio weights based on asset returns and given initial weights
     * @param start     the start date for portfolio evolution
     * @param end       the end date for portfolio evolution
     * @param initial   the initial weights of the portfolio
     * @return          the DataFrame of evolved portfolio weights
     */
    DataFrame<LocalDate,String> getEvolvedWeights(LocalDate start, LocalDate end, Map<String,Double> initial) {
        final YahooFinance yahoo = new YahooFinance();
        final List<String> tickers = initial.keySet().stream().collect(Collectors.toList());
        final DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);
        return cumReturns.copy().applyDoubles(v -> {
            final int rowOrdinal = v.rowOrdinal();
            final double initWeight = initial.get(v.colKey());
            if (rowOrdinal == 0) {
                return initWeight;
            } else {
                final int colOrdinal = v.colOrdinal();
                final double cumReturn = cumReturns.data().getDouble(rowOrdinal, colOrdinal);
                return initWeight * (1d + cumReturn);
            }
        }).rows().apply(row -> {
            final double sum = row.stats().sum();
            row.applyDoubles(v -> {
                return v.getDouble() / sum;
            });
        });
    }



}
