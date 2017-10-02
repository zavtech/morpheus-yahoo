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

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooFinance;

/**
 * @author Xavier Witdouck
 *
 * 1. Calculate Portfolio Beta
 * 2. Calculate Principal Portfolio / Explained Variance
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooRoboAdvisor {


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


    /**
     * Returns a DataFrame containing risk, return and sharpe ratio for portfolios over the date range
     * @param range         the date range for historical returns
     * @param portfolios    the DataFrame of portfolio weights, one row per portfolio configuration
     * @return              the DataFrame with risk, return and sharpe
     */
    DataFrame<Integer,String> calcRiskReturn(Range<LocalDate> range, DataFrame<Integer,String> portfolios) {
        YahooFinance yahoo = new YahooFinance();
        Array<String> tickers = portfolios.cols().keyArray();
        DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(range.start(), range.end(), tickers);
        DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(range.start(), range.end(), tickers);
        //Compute asset covariance matrix from daily returns and annualize
        DataFrame<String,String> sigma = dayReturns.cols().stats().covariance().applyDoubles(x -> x.getDouble() * 252);
        DataFrame<LocalDate,String> assetReturns = cumReturns.rows().last().map(DataFrameRow::toDataFrame).get();
        //Prepare 3 column DataFrame to capture results
        DataFrame<Integer,String>  riskReturn = DataFrame.ofDoubles(
            portfolios.rows().keyArray(),
            Array.of("Risk", "Return", "Sharpe")
        );
        portfolios.rows().forEach(row -> {
            DataFrame<Integer,String> weights = row.toDataFrame();
            double portReturn = weights.dot(assetReturns.transpose()).data().getDouble(0, 0);
            double portVariance = weights.dot(sigma).dot(weights.transpose()).data().getDouble(0, 0);
            riskReturn.data().setDouble(row.key(), "Return", portReturn * 100d);
            riskReturn.data().setDouble(row.key(), "Risk", Math.sqrt(portVariance) * 100d);
            riskReturn.data().setDouble(row.key(), "Sharpe", portReturn / Math.sqrt(portVariance));
        });
        return riskReturn;
    }


    /**
     * Calculates portfolio cumulative returns over a date range given a frame on initial weight configurations
     * @param range         the date range for historical returns
     * @param portfolios    MxN DataFrame of portfolio weights, M portfolios, N assets
     * @return              the cumulative returns for each portfolio, TxM, portfolios labelled P0, P1 etc...
     */
    DataFrame<LocalDate,String> getEquityCurves(Range<LocalDate> range, DataFrame<Integer,String> portfolios) {
        final YahooFinance yahoo = new YahooFinance();
        final Iterable<String> tickers = portfolios.cols().keyArray();
        final DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(range.start(), range.end(), tickers);
        final Range<String> colKeys = Range.of(0, portfolios.rowCount()).map(i -> "P" + i);
        return DataFrame.ofDoubles(cumReturns.rows().keyArray(), colKeys, v -> {
            double totalReturn = 0d;
            for (int i=0; i<portfolios.colCount(); ++i) {
                final double weight = portfolios.data().getDouble(v.colOrdinal(), i);
                final double assetReturn = cumReturns.data().getDouble(v.rowOrdinal(), i);
                totalReturn += (weight * assetReturn);
            }
            return totalReturn;
        });
    }



    @Test()
    public void equityCurves() throws Exception {
        int portfolioCount = 10000;
        Range<LocalDate> range = Range.of(LocalDate.now().minusYears(1), LocalDate.now());
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        DataFrame<Integer,String> portfolios = randomPortfolios(portfolioCount, tickers);
        DataFrame<LocalDate,String> performance = getEquityCurves(range, portfolios);
        Chart.create().withLinePlot(performance.applyDoubles(v -> v.getDouble() * 100d), chart -> {
            chart.title().withText(portfolioCount + " Equity Curves (Past 1 Year Returns)");
            chart.subtitle().withText("Robo-Advisor Universe: VTI, VEA, VWO, VTEB, VIG, XLE");
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_4.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void sharpes() throws Exception {
        int portfolioCount = 1000;
        Range<LocalDate> range = Range.of(LocalDate.now().minusYears(1), LocalDate.now());
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        Array<Double> proposed = Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d);
        DataFrame<Integer,String> portfolios = randomPortfolios(portfolioCount, tickers);
        portfolios.rowAt(0).applyDoubles(v -> proposed.getDouble(v.colOrdinal()));
        DataFrame<Integer,String> riskReturn = calcRiskReturn(range, portfolios).rows().sort(false, "Sharpe");

        riskReturn.out().print();

        DataFrame<Integer,String> sharpes = riskReturn.head(100).copy().rows().mapKeys(DataFrameRow::ordinal);
        Chart.create().withBarPlot(sharpes.cols().select("Sharpe"), false, chart -> {
            chart.title().withText("Top 100 Sharpe Ratios from " + portfolioCount + " Random Portfolios");
            chart.subtitle().withText("Robo-Advisor Universe: VTI, VEA, VWO, VTEB, VIG, XLE");
            chart.plot().data().at(0).withLowerDomainInterval(v -> v - 1);
            chart.plot().axes().domain().label().withText("Top N");
            chart.plot().axes().range(0).label().withText("Sharpe Ratio");
            chart.show();
        });

        /*
        Chart.create().withLinePlot(bestWorstChosen.applyDoubles(v -> v.getDouble() * 100d), chart -> {
            chart.title().withText("Best/Worst/Chosen Equity Curves (Past 1 Year Returns)");
            chart.subtitle().withText("Robo-Advisor Universe: VTI, BND, VWO, VTEB, VIG, XLE");
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.plot().style("Chosen").withColor(Color.BLACK).withLineWidth(1.5f);
            chart.legend().on();
            chart.show();
        });
        */

        Thread.currentThread().join();
    }


    @Test()
    public void bestWorstChosen() throws Exception {

        int portfolioCount = 1000;
        Range<LocalDate> range = Range.of(LocalDate.now().minusYears(1), LocalDate.now());
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        Array<Double> proposed = Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d);
        DataFrame<Integer,String> portfolios = randomPortfolios(portfolioCount, tickers);
        portfolios.rowAt(0).applyDoubles(v -> proposed.getDouble(v.colOrdinal()));
        //Compute risk / return / sharpe for random portfolios
        DataFrame<Integer,String> riskReturn = calcRiskReturn(range, portfolios).rows().sort(false, "Sharpe");
        //Capture portfolio keys for chosen, best and worst portfolio based on sharpe
        DataFrame<Integer,String> candidates = portfolios.rows().select(0,
            riskReturn.rows().first().get().key(),
            riskReturn.rows().last().get().key()
        );
        //Compute equity curves for chosen, best and worst
        DataFrame<LocalDate,String> equityCurves = getEquityCurves(range, candidates).cols().mapKeys(col -> {
            switch (col.ordinal()) {
                case 0: return "Chosen";
                case 1: return "Best";
                case 2: return "Worst";
                default: return col.key();
            }
        });
        //Capture returns of S&P 500
        YahooFinance yahoo = new YahooFinance();
        DataFrame<LocalDate,String> spy = yahoo.getCumReturns(range.start(), range.end(), "SPY");

        //Plot the equity curves
        Chart.create().withLinePlot(equityCurves.applyDoubles(v -> v.getDouble() * 100d), chart -> {
            chart.title().withText("Best/Worst/Chosen Equity Curves (Past 1 Year Returns) + SPY");
            chart.subtitle().withText("Robo-Advisor Universe: VTI, VEA, VWO, VTEB, VIG, XLE");
            chart.plot().<String>data().add(spy.times(100d));
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.plot().style("Chosen").withColor(Color.BLACK).withLineWidth(1.5f);
            chart.plot().style("Best").withColor(Color.GREEN.darker().darker()).withLineWidth(1.5f);
            chart.plot().style("Worst").withColor(Color.RED).withLineWidth(1.5f);
            chart.plot().style("SPY").withColor(Color.BLUE);
            chart.legend().on();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_6.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


}
