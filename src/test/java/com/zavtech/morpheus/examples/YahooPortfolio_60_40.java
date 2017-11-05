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
import java.time.Period;
import java.time.temporal.ChronoUnit;

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooFinance;

/**
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooPortfolio_60_40 extends YahooPortfolio {



    @Test()
    public void cumReturns() throws Exception {
        LocalDate end = LocalDate.of(2017, 1, 1);
        LocalDate start = end.minusYears(1);
        Array<String> tickers = Array.of("VTI", "BND");

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
            //chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_0.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void efficiency() throws Exception {

        int count = 10000;
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        //Define asset universe of low cost ETFs
        Array<String> tickers = Array.of("VTI", "BND");
        //Define the weights suggested by Robo-Advisor
        Array<Double> weights = Array.of(0.6d, 0.4d);
        //Generate long only random portfolios
        DataFrame<String,String> portfolios = randomPortfolios(count, tickers);
        //Apply proposed weights to first portfolio
        portfolios.rowAt("P0").applyDoubles(v -> weights.getDouble(v.colOrdinal()));
        //Compute portfolio risk, return & Sharpe ratio
        DataFrame<String,String> riskReturn = calcRiskReturn(portfolios, endDate, false);
        //Select row with risk / return of the proposed portfolio
        DataFrame<String,String> chosen = riskReturn.rows().select("P0").cols().replaceKey("Return", "60/40");

        //Plot the results using a scatter plot
        Chart.create().withScatterPlot(riskReturn, false, "Risk", chart -> {
            chart.title().withText("Risk / Return Profile For VTI + BND Portfolio");
            chart.title().withFont(new Font("Arial", Font.BOLD, 12));
            chart.subtitle().withText(count + " Portfolio Combinations Simulated");
            chart.subtitle().withFont(new Font("Arial", Font.PLAIN, 11));
            chart.plot().<String>data().add(chosen, "Risk");
            chart.plot().render(1).withShapes();
            chart.plot().style("60/40").withColor(Color.BLUE);
            chart.plot().axes().domain().label().withText("Portfolio Risk");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().on().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_7.png"), 400, 300, true);
            chart.show();
        });

        Thread.currentThread().join();

    }


    @Test()
    public void equityCurves() throws Exception {
        int portfolioCount = 10000;
        Array<String> tickers = Array.of("VTI", "BND");
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        Range<LocalDate> range = Range.of(endDate.minusYears(1), endDate);
        DataFrame<String,String> portfolios = randomPortfolios(portfolioCount, tickers);
        DataFrame<LocalDate,String> performance = getEquityCurves(range, portfolios);
        Chart.create().withLinePlot(performance.applyDoubles(v -> v.getDouble() * 100d), chart -> {
            chart.title().withText(portfolioCount + " Equity Curves (Past 1 Year Returns)");
            chart.title().withFont(new Font("Arial", Font.BOLD, 12));
            chart.subtitle().withFont(new Font("Arial", Font.PLAIN, 11));
            chart.subtitle().withText("Stock/Bond Universe: VTI, BND");
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_9.png"), 400, 280, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void bestWorstChosen() throws Exception {

        int portfolioCount = 100000;
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        Range<LocalDate> range = Range.of(endDate.minusYears(1), endDate);
        Array<String> tickers = Array.of("VTI", "BND");
        Array<Double> proposedWeights = Array.of(0.6d, 0.4d);
        DataFrame<String,String> portfolios = randomPortfolios(portfolioCount, tickers);
        portfolios.rowAt("P0").applyDoubles(v -> proposedWeights.getDouble(v.colOrdinal()));
        //Compute risk / return / sharpe for random portfolios
        DataFrame<String,String> riskReturn = calcRiskReturn(portfolios, endDate, true).rows().sort(false, "Sharpe");
        //Capture portfolio keys for chosen, best and worst portfolio based on sharpe
        DataFrame<String,String> candidates = portfolios.rows().select("P0",
            riskReturn.rows().first().get().key(),
            riskReturn.rows().last().get().key()
        );
        //Compute equity curves for chosen, best and worst
        DataFrame<LocalDate,String> equityCurves = getEquityCurves(range, candidates).cols().mapKeys(col -> {
            switch (col.ordinal()) {
                case 0: return "60/40";
                case 1: return "Best";
                case 2: return "Worst";
                default: return col.key();
            }
        });
        //Plot the equity curves
        Chart.create().withLinePlot(equityCurves.applyDoubles(v -> v.getDouble() * 100d), chart -> {
            chart.title().withText("Best, Worst, 60/40 Equity Curves (Past 1 Year Returns)");
            chart.title().withFont(new Font("Arial", Font.BOLD, 12));
            chart.subtitle().withText("Stock/Bond Universe: VTI, BND");
            chart.subtitle().withFont(new Font("Arial", Font.PLAIN, 11));
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.plot().style("60/40").withColor(Color.BLACK).withLineWidth(1.5f);
            chart.plot().style("Best").withColor(Color.GREEN.darker().darker());
            chart.plot().style("Worst").withColor(Color.RED).withLineWidth(1.5f);
            chart.legend().on().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_10.png"), 400, 300, true);
            chart.show();
        });

        riskReturn.rows().select("P0",
            riskReturn.rows().first().get().key(),
            riskReturn.rows().last().get().key()
        ).copy().rows().mapKeys(row -> {
            switch (row.ordinal()) {
                case 0: return "60/40";
                case 1: return "Best";
                case 2: return "Worst";
                default: return row.key();
            }
        }).out().print();

        Thread.currentThread().join();
    }


    @Test()
    public void sharpeDecay() throws Exception {
        int count = 10000;
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        Array<String> tickers = Array.of("VTI", "BND");
        Array<Double> proposed = Array.of(0.6, 0.4d);
        DataFrame<String,String> portfolios = randomPortfolios(count, tickers);
        portfolios.rowAt(0).applyDoubles(v -> proposed.getDouble(v.colOrdinal()));
        DataFrame<String,String> riskReturn = calcRiskReturn(portfolios, endDate, true);
        DataFrame<String,String> sharpes = riskReturn.rows().sort(false, "Sharpe").cols().select("Sharpe");
        int rankOfSixtyForty = sharpes.rows().ordinalOf("P0");
        DataFrame<Integer,String> _60_40 = sharpes.rows().select("P0").copy().rows().mapKeys(row -> rankOfSixtyForty);
        Chart.create().withLinePlot(sharpes.rows().mapKeys(DataFrameRow::ordinal), chart -> {
            chart.title().withText("Sharpe Ratio Decay for VTI + BND Random Portfolios");
            chart.title().withFont(new Font("Arial", Font.BOLD, 12));
            chart.subtitle().withText(count + " Portfolio Combinations Simulated");
            chart.subtitle().withFont(new Font("Arial", Font.PLAIN, 11));
            chart.plot().<String>data().add(_60_40.cols().replaceKey("Sharpe", "60/40"));
            chart.plot().render(1).withShapes();
            chart.plot().style("Sharpe").withColor(Color.RED).withLineWidth(1.5f);
            chart.plot().style("60/40").withColor(Color.BLUE);
            chart.plot().axes().domain().label().withText("Portfolio Rank");
            chart.plot().axes().range(0).label().withText("Sharpe Ratio");
            chart.plot().axes().range(0).format().withPattern("0.00;-0.00");
            chart.legend().on().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_8.png"), 400, 300, true);
            chart.show();
        });

        Thread.currentThread().join();
    }



    @Test
    public void years() throws Exception {

        //Define DataFrame of position weights per 60/40 Stock-Bond allocation
        Array<String> tickers = Array.of("VTI", "BND");
        DataFrame<String,String> portfolio = DataFrame.of(tickers, String.class, columns -> {
            columns.add("Weights", Array.of(0.6d, 0.4d));
        });
        //Generate 1-year equity curves starting in 2008 for 60/40 allocation
        Range<LocalDate> dateRange = Range.of(LocalDate.of(2009, 1, 1), LocalDate.of(2018, 1, 1), Period.ofYears(1));
        Range<DataFrame<Integer,String>> frames = dateRange.map(date -> {
            Range<LocalDate> range = Range.of(date.minusYears(1), date);
            DataFrame<LocalDate,String> equityCurve = getEquityCurves(range, portfolio.transpose());
            LocalDate start = equityCurve.rows().firstKey().get();
            return equityCurve
                .rows().mapKeys(DataFrameRow::ordinal)
                .cols().mapKeys(col -> String.valueOf(start.getYear()))
                .applyDoubles(v -> v.getDouble() * 100d);
        });
        //Combine equity curves into single frame and add mean of each curve
        DataFrame<Integer,String> combined = DataFrame.concatColumns(frames).rows().select(r -> !r.hasNulls());
        //Add a column with the average of all equity curves
        combined.cols().add("Mean", Double.class, v -> v.row().stats().mean());
        //Plot the equity curves
        Chart.create().withLinePlot(combined, chart -> {
            chart.title().withText("One-Year Equity Curves for 60/40 Stock/Bond Portfolio (Last 9 Years)");
            chart.subtitle().withText("Investment Universe: 60% VTI, 40% BND");
            chart.plot().axes().domain().label().withText("Trading Days Since Start Of Year");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.plot().style("Mean").withColor(Color.BLACK).withLineWidth(1.5f);
            chart.legend().on().right();
            //chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_12.png"), 800, 500, true);
            chart.show();
        });

        Thread.currentThread().join();

    }


    public void computeStats(DataFrame<?,String> combined) {
        double returnValue = combined.colAt("Mean").last(v -> true).get().getDouble() / 100d;
        Array<Double> values = Array.of(combined.colAt("Mean").toDoubleStream().map(v -> 100 * (1d + v / 100d)).toArray());
        Array<Double> returns = values.map(v -> {
            switch (v.index()) {
                case 0: return 0d;
                default: return v.getDouble() / values.getDouble(v.index()-1) -1d;
            }
        });
        double risk = Math.sqrt(returns.stats().variance().doubleValue() * 252d);
        IO.println("Return: " + returnValue);
        IO.println("Risk: " + risk);
        IO.println("Sharpe: " + returnValue / risk);
    }



    @Test()
    public void kissVsRobo() throws Exception {
        int count = 100000;
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        //Generate risk / return profile for Stock-Bond universe
        Array<String> tickers1 = Array.of("VTI", "BND");
        DataFrame<String,String> portfolios1 = randomPortfolios(count, tickers1);
        DataFrame<String,String> riskReturn1 = calcRiskReturn(portfolios1, endDate, false);
        //Generate risk / return profile for Robo-Advisor universe
        Array<String> tickers2 = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        DataFrame<String,String> portfolios2 = randomPortfolios(count, tickers2);
        DataFrame<String,String> riskReturn2 = calcRiskReturn(portfolios2, endDate, false);
        //Plot the results using a scatter plot
        Chart.create().withScatterPlot(riskReturn2.cols().replaceKey("Return", "Robo"), false, "Risk", chart -> {
            chart.title().withText("Risk / Return Profiles: 60/40 Stock-Bond Versus Robo-Advisor");
            chart.subtitle().withText(count + " Portfolio Combinations Simulated");
            chart.plot().<String>data().add(riskReturn1.cols().replaceKey("Return", "60/40"), "Risk");
            chart.plot().render(1).withDots();
            chart.plot().axes().domain().label().withText("Portfolio Risk");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().on().right();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_11.png"), 800, 500, true);
            chart.show();
        });

        Thread.currentThread().join();
    }

    @Test()
    public void kissVsRobo2() throws Exception {
        int count = 100000;
        LocalDate now = LocalDate.now();
        Range<LocalDate> endDates = Range.ofLocalDates("2009-01-01", "2019-01-01", Period.ofYears(1));
        endDates.forEach(endDate -> {
            //Generate risk / return profile for Stock-Bond universe
            LocalDate date = endDate.isAfter(now) ? now : endDate;
            Array<String> tickers1 = Array.of("VTI", "BND");
            DataFrame<String,String> portfolios1 = randomPortfolios(count, tickers1);
            DataFrame<String,String> riskReturn1 = calcRiskReturn(portfolios1, date, false);
            //Generate risk / return profile for Robo-Advisor universe
            Array<String> tickers2 = Array.of("VTI", "VEA", "VWO", "BND", "VIG", "XLE");
            DataFrame<String,String> portfolios2 = randomPortfolios(count, tickers2);
            DataFrame<String,String> riskReturn2 = calcRiskReturn(portfolios2, date, false);
            int year = endDate.minusYears(1).getYear();
            //Plot the results using a scatter plot
            Chart.create().withScatterPlot(riskReturn2.cols().replaceKey("Return", "Robo"), false, "Risk", chart -> {
                chart.title().withText("60/40 Stock-Bond Versus Robo-Advisor - Year " + year);
                chart.title().withFont(new Font("Arial", Font.BOLD, 12));
                chart.subtitle().withText(count + " Portfolio Combinations Simulated");
                chart.subtitle().withFont(new Font("Arial", Font.PLAIN, 11));
                chart.plot().<String>data().add(riskReturn1.cols().replaceKey("Return", "60/40"), "Risk");
                chart.plot().render(1).withDots();
                chart.plot().axes().domain().label().withText("Portfolio Risk");
                chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
                chart.plot().axes().range(0).label().withText("Portfolio Return");
                chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
                chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_kiss_" + year + ".png"), 400, 300, true);
            });
        });
    }


}
