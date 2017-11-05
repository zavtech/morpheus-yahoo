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
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.util.IO;
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
public class YahooRoboAdvisor extends YahooPortfolio {


    @Test()
    public void equityCurves() throws Exception {
        int portfolioCount = 10000;
        Range<LocalDate> range = Range.of(LocalDate.now().minusYears(1), LocalDate.now());
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        DataFrame<String,String> portfolios = randomPortfolios(portfolioCount, tickers);
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
        LocalDate endDate = LocalDate.now();
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        Array<Double> proposed = Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d);
        DataFrame<String,String> portfolios = randomPortfolios(portfolioCount, tickers);
        portfolios.rowAt(0).applyDoubles(v -> proposed.getDouble(v.colOrdinal()));
        DataFrame<String,String> riskReturn = calcRiskReturn(portfolios, endDate, true).rows().sort(false, "Sharpe");

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
    public void bestWorstProposed() throws Exception {

        int portfolioCount = 100000;
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        Range<LocalDate> range = Range.of(endDate.minusYears(1), endDate);
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        Array<Double> proposedWeights = Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d);
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
                case 0: return "Proposed";
                case 1: return "Best";
                case 2: return "Worst";
                default: return col.key();
            }
        });
        //Plot the equity curves
        Chart.create().withLinePlot(equityCurves.applyDoubles(v -> v.getDouble() * 100d), chart -> {
            chart.title().withText("Best/Worst/Proposed Equity Curves (Past 1 Year Returns)");
            chart.subtitle().withText("Robo-Advisor Universe: VTI, VEA, VWO, VTEB, VIG, XLE");
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.plot().style("Proposed").withColor(Color.BLACK).withLineWidth(1.5f);
            chart.plot().style("Best").withColor(Color.GREEN.darker().darker());
            chart.plot().style("Worst").withColor(Color.RED);
            chart.legend().on();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_6.png"), 700, 400, true);
            chart.show();
        });

        riskReturn.rows().select("P0",
            riskReturn.rows().first().get().key(),
            riskReturn.rows().last().get().key()
        ).copy().rows().mapKeys(row -> {
            switch (row.ordinal()) {
                case 0: return "Proposed";
                case 1: return "Best";
                case 2: return "Worst";
                default: return row.key();
            }
        }).out().print();

        Thread.currentThread().join();
    }



    /**
     * Returns portfolio asset weights where each position has equal contribution to portfolio risk
     * @param tickers       the asset tickers
     * @param endDate       the end date for 1-year window
     * @return              the array of portfolio weights
     */
    Array<Double> getRiskWeightedPortfolio(Iterable<String> tickers, LocalDate endDate) {
        YahooFinance yahoo = new YahooFinance();
        Range<LocalDate> range = Range.of(endDate.minusYears(1), endDate);
        DataFrame<LocalDate,String> returns = yahoo.getDailyReturns(range, tickers);
        DataFrame<String,StatType> variances = returns.cols().stats().variance().applyDoubles(v -> v.getDouble() * 252);
        DataFrame<String,StatType> reciprocal = variances.mapToDoubles(v -> 1d / v.getDouble());
        double total = reciprocal.stats().sum();
        return reciprocal.applyDoubles(v -> v.getDouble() / total).colAt(0).toArray();
    }


}
