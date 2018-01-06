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
import java.util.Map;
import java.util.stream.Stream;

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.util.text.printer.Printer;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooFinance;

/**
 * Class summary goes here...
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooParityPortfolios extends YahooPortfolio {


    @Test()
    public void efficiency() throws Exception {
        int count = 100000;
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        //Define asset universe of low cost ETFs
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        //Define the weights suggested by Robo-Advisor
        Array<Double> roboWeights = Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d);
        //Define equal capital weights
        Array<Double> equalWeights = Range.of(0, 6).map(i -> 1d / 6d).toArray();
        //Generate long only random portfolios
        DataFrame<String,String> portfolios = randomPortfolios(count, tickers);
        //Apply proposed robo & equal weights to first & second portfolio
        portfolios.row("P0").applyDoubles(v -> roboWeights.getDouble(v.colOrdinal()));
        portfolios.row("P1").applyDoubles(v -> equalWeights.getDouble(v.colOrdinal()));
        //Compute portfolio risk, return & Sharpe ratio
        DataFrame<String,String> riskReturn = calcRiskReturn(portfolios, endDate, false);
        //Select row with risk / return of test portfolios
        DataFrame<String,String> robo = riskReturn.rows().select("P0").cols().replaceKey("Return", "Robo-Weights");
        DataFrame<String,String> cap = riskReturn.rows().select("P1").cols().replaceKey("Return", "Cap-Weights");

        //Plot the results using a scatter plot
        Chart.create().withScatterPlot(riskReturn.cols().replaceKey("Return", "Random"), false, "Risk", chart -> {
            chart.title().withText("Risk / Return Profile For Robo-Advisor Portfolio");
            chart.subtitle().withText(count + " Portfolio Combinations Simulated");
            chart.plot().<String>data().add(robo, "Risk");
            chart.plot().<String>data().add(cap, "Risk");
            chart.plot().render(1).withDots();
            chart.plot().render(2).withDots();
            chart.plot().style("Robo-Weights").withColor(Color.GREEN);
            chart.plot().style("Cap-Weights").withColor(Color.BLUE);
            chart.plot().axes().domain().label().withText("Portfolio Risk");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().on().right();
            //chart.writerPng(new File("../morpheus-docs/docs/images/mpt/mpt_5.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    /**
     * Returns a DataFrame with Marginal Contribution to risk values for the input portfolios
     * @param portfolios    the DataFrame of portfolio weights, one row per portfolio configuration
     * @param range         the date range for asset returns
     * @return              the DataFrame of MC to risk values for asset (same shape as input frame)
     */
    DataFrame<String,String> calcMarginalContribution(DataFrame<String,String> portfolios, Range<LocalDate> range) {
        YahooFinance yahoo = new YahooFinance();
        Array<String> tickers = portfolios.cols().keyArray();
        DataFrame<LocalDate,String> dayReturns = yahoo.getDailyReturns(range, tickers);
        DataFrame<String,String> sigma = dayReturns.cols().stats().covariance().times(252d);
        DataFrame<String,String>  result = portfolios.copy().applyDoubles(v -> Double.NaN);
        result.cols().add("TotalRisk", Double.class);
        portfolios.rows().forEach(portfolio -> {
            DataFrame<String,String> weights = portfolio.toDataFrame();
            double variance = weights.dot(sigma).dot(weights.transpose()).data().getDouble(0,0);
            double volatility = Math.sqrt(variance);
            result.data().setDouble(portfolio.ordinal(), "TotalRisk", volatility);
            portfolio.forEachValue(asset -> {
                double mc = 0d;
                int rowOrdinal = asset.rowOrdinal();
                int colOrdinal = asset.colOrdinal();
                for (int j=0; j<portfolios.colCount(); ++j) {
                    double w_j = portfolio.getDouble(j);
                    double cov_i_j = sigma.data().getDouble(colOrdinal, j);
                    mc = mc + (w_j * cov_i_j);
                    if (asset.colKey().equals("BND")) {
                        IO.println(mc);
                    }
                }
                double w_i = asset.getDouble();
                double mctr = w_i * (mc / volatility);
                result.data().setDouble(rowOrdinal, colOrdinal, mctr);
            });
        });
        return result;
    }



    @Test()
    public void test_60_40() throws Exception {
        LocalDate endDate = LocalDate.of(2017, 1, 1);
        Array<String> tickers = Array.of("VTI", "BND");
        Array<Double> bondWeights = Range.of(0d, 101d, 5d).toArray();
        DataFrame<String,String> portfolio = DataFrame.of(tickers, String.class, columns -> {
            bondWeights.forEach(bondWeight -> {
                double equityWeight = 100d - bondWeight;
                String label = String.format("%s/%s", equityWeight, bondWeight);
                columns.add(label, Array.of(equityWeight / 100d, bondWeight / 100d));
            });
        });

        DataFrame<String,String> riskReturn = calcRiskReturn(portfolio.transpose(), endDate, false);
        riskReturn.out().print();
        riskReturn.cols().add("BondWeight", Double.class, v -> bondWeights.getDouble(v.rowOrdinal()));

        Chart.create().withLinePlot(riskReturn, "BondWeight", chart -> {
            chart.show();
        });

        Thread.currentThread().join();

        Range<LocalDate> range = Range.of(endDate.minusYears(5), endDate);
        DataFrame<String,String> mctr = calcMarginalContribution(portfolio.copy().transpose(), range);


        mctr.out().print();
    }


    /**
     * Returns naive risk parity weights for an investment universe
     * @param tickers       the security ticker symbols
     * @param dateRange     the date range for asset returns
     * @return              the DataFrame of risk parity weights
     */
    public DataFrame<String,String> getRiskParityWeights(Iterable<String> tickers, Range<LocalDate> dateRange) {
        YahooFinance yahoo = new YahooFinance();
        DataFrame<LocalDate,String> returns = yahoo.getDailyReturns(dateRange, tickers);
        Array<Double> variance = returns.cols().stats().variance().colAt(0).toArray();
        Array<Double> volatility = variance.mapToDoubles(v -> Math.sqrt(252d * v.getDouble()));
        double k = volatility.mapToDoubles(v -> 1d / v.getDouble()).stats().sum().doubleValue();
        Array<Double> riskParityWeights = volatility.mapToDoubles(v -> 1d / (v.getDouble() * k));
        return DataFrame.ofDoubles("Parity(Risk)", tickers).applyDoubles(v -> {
            return riskParityWeights.getDouble(v.colOrdinal());
        });
    }


    @Test()
    public void testMc() throws Exception {
        Range<LocalDate> range = Range.ofLocalDates("2010-01-01", "2017-09-29");
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "BND", "VIG", "XLE");
        DataFrame<String,String> weights = getRiskParityWeights(tickers, range);
        DataFrame<String,String> mctr = calcMarginalContribution(weights, range);
        weights.out().print();
        mctr.out().print();
    }


    @Test()
    public void cov() {
        YahooFinance yahoo = new YahooFinance();
        Range<LocalDate> range = Range.ofLocalDates("2010-01-01", "2017-09-29");
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "BND", "VIG", "XLE");
        DataFrame<String,String> portfolio = getRiskParityWeights(tickers, range);
        Map<String,Double> weights = Collect.asMap(map -> {
            portfolio.forEach(v -> {
                map.put(v.colKey(), v.getDouble());
            });
        });

        DataFrame<LocalDate,String> returns = getPortfolioCumReturns(range.start(), range.end(), weights);
        returns.out().print();

    }


    @Test()
    public void comparison() throws Exception {

        LocalDate endDate = LocalDate.of(2017, 9, 29);
        Range<LocalDate> parityRange = Range.of(endDate.minusYears(5), endDate);
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "BND", "VIG", "XLE");
        DataFrame<String,String> riskParityWeights = getRiskParityWeights(tickers, parityRange);
        DataFrame<String,String> portfolios = DataFrame.concatColumns(
            riskParityWeights.transpose(),
            DataFrame.of(tickers, String.class, columns -> {
                columns.add("Parity(Cap)", Range.of(0, 6).map(i -> 1d / 6d));
                columns.add("Robo(Proposed)", Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d));
            })
        );

        DataFrame<String,String> randomPortfolios = randomPortfolios(100000, tickers);
        DataFrame<String,String> randomRiskReturn = calcRiskReturn(randomPortfolios, endDate, false);
        DataFrame<String,String> caseStudies = calcRiskReturn(portfolios.transpose(), endDate, false);
        Array<DataFrame<String,String>> frames = portfolios.cols().keyArray().map(v -> {
            return caseStudies.rows().select(v.getValue()).cols().replaceKey("Return", v.getValue());
        });

        Chart.create().withScatterPlot(randomRiskReturn.cols().replaceKey("Return", "Random"), false, "Risk", chart -> {
            frames.forEachValue(v -> {
                chart.plot().<String>data().add(v.getValue(), "Risk");
                chart.plot().render(v.index() + 1).withDots(10);
            });
            String dates = String.format("(%s - %s)", endDate.minusYears(1), endDate);
            chart.title().withText("Parity Portfolios vs Robo Advisor Allocation " + dates);
            chart.subtitle().withText("Investment Universe: VTI, VEA, VWO, VTEB, VIG, XLE");
            chart.plot().axes().domain().label().withText("Portfolio Risk");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).label().withText("Portfolio Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.plot().style("Random").withColor(Color.LIGHT_GRAY);
            chart.legend().on().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/parity_1.png"), 700, 400, true);
            chart.show();
        });

        portfolios.transpose().out().print(formats -> {
            formats.setPrinter(Double.class, Printer.ofDouble("0.00'%';-0.00'%'", 100));
        });

        DataFrame<String,String> riskReturn = calcRiskReturn(portfolios.transpose(), endDate, true);
        riskReturn.out().print(formats -> {
            formats.setPrinter("Risk", Printer.ofDouble("0.00'%';-0.00'%'"));
            formats.setPrinter("Return", Printer.ofDouble("0.00'%';-0.00'%'"));
            formats.setPrinter("Sharpe", Printer.ofDouble("0.00;-0.00"));
        });

        Thread.currentThread().join();
    }


    @Test()
    public void multipleYears() throws Exception {

        LocalDate endDate = LocalDate.of(2017, 9, 29);
        Range<LocalDate> parityRange = Range.of(endDate.minusYears(5), endDate);
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "BND", "VIG", "XLE");
        DataFrame<String,String> riskParityWeights = getRiskParityWeights(tickers, parityRange);
        DataFrame<String,String> portfolios = DataFrame.concatColumns(
            riskParityWeights.transpose(),
            DataFrame.of(tickers, String.class, columns -> {
                columns.add("Parity(Cap)", Range.of(0, 6).map(i -> 1d / 6d));
                columns.add("Robo(Proposed)", Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d));
            })
        );

        Range<LocalDate> endDates = Range.ofLocalDates("2008-12-31", "2018-12-31", Period.ofYears(1));
        DataFrame<String,String> combined = DataFrame.concatColumns(endDates.map(end -> {
            LocalDate date = endDate.isAfter(end) ? end : endDate;
            DataFrame<String,String> riskReturn = calcRiskReturn(portfolios.transpose(), date, true);
            return riskReturn.cols().select("Sharpe").cols().replaceKey("Sharpe", String.valueOf(date.getYear()));
        }));

        Chart.create().withBarPlot(combined.transpose(), false, chart -> {
            chart.title().withText("Realized Sharpe Ratios of Parity Portfolios vs Robo-Advisor");
            chart.subtitle().withText("Investment Universe: VTI, VEA, VWO, BND, VIG, XLE");
            chart.plot().axes().domain().label().withText("Realized Sharpe Ratio");
            chart.plot().axes().range(0).label().withText("Year");
            chart.legend().on().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/mpt/parity_2.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void returnComp() throws Exception {
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        Range<LocalDate> range = Range.of(endDate.minusYears(1), endDate);
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        DataFrame<String,String> riskParityWeights = getRiskParityWeights(tickers, Range.of(endDate.minusYears(5), endDate));
        DataFrame<String,String> portfolios = DataFrame.concatColumns(
            riskParityWeights.transpose(),
            DataFrame.of(tickers, String.class, columns -> {
                columns.add("Parity(Cap)", Range.of(0, 6).map(i -> 1d / 6d));
                columns.add("Robo(Proposed)", Array.of(0.35d, 0.21d, 0.16d, 0.15d, 0.08d, 0.05d));
            })
        );

        portfolios.out().print();

        //DataFrame<LocalDate,String> returns = getPortfolioDayReturns(range, portfolios.transpose());
        DataFrame<LocalDate,String> returns = getEquityCurves(range, portfolios.transpose());
        Chart.create().withLinePlot(returns, chart -> {
            chart.title().withText("Cumulative Portfolio Returns");
            chart.subtitle().withText("Range: (" + range.start() + " to " + range.end() + ")");
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().on().right();
            chart.show();
        });

        returns.cols().stats().covariance().out().print();
        returns.cols().stats().correlation().out().print();

        Thread.currentThread().join();

    }


    @Test()
    public void correlations() throws Exception {
        YahooFinance yahoo = new YahooFinance();
        LocalDate endDate = LocalDate.of(2017, 9, 29);
        Range<LocalDate> range = Range.of(endDate.minusYears(1), endDate);
        Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VTEB", "VIG", "XLE");
        Array<Double> weights = Array.of(0.1241, 0.1069, 0.0839, 0.4716, 0.1368, 0.0767);
        DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(range, tickers);
        cumReturns.cols().add("Parity(Risk)", Double.class, v1 -> {
            double total = 0d;
            for (int i=0; i<tickers.length(); ++i) {
                double assetRet = v1.row().getDouble(i);
                double weight = weights.getDouble(i);
                total += weight * assetRet;
            }
            return total;
        });

        DataFrame<LocalDate,String> dayReturns = cumToDayReturns(cumReturns);
        dayReturns.cols().stats().covariance().out().print();
        dayReturns.cols().stats().correlation().out().print();

        Chart.create().withLinePlot(cumReturns, chart -> {
            chart.title().withText("Cumulative Returns");
            chart.subtitle().withText("Range: (" + range.start() + " to " + range.end() + ")");
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.plot().style("Parity(Risk)").withColor(Color.BLACK).withLineWidth(2f);
            chart.legend().on().right();
            chart.show();
        });

        Thread.currentThread().join();
    }


    /**
     * Converts a DataFrame of cumulative returns into daily returns
     * @param cumReturns        the frame of cumulative returns
     * @return                  the frame of daily returns
     */
    public DataFrame<LocalDate,String> cumToDayReturns(DataFrame<LocalDate,String> cumReturns) {
        DataFrame<LocalDate,String> result = cumReturns.copy().applyDoubles(v -> Double.NaN);
        return result.applyDoubles(v -> {
            final int rowOrdinal = v.rowOrdinal();
            if (rowOrdinal == 0) {
                return 0d;
            } else {
                final int colOrdinal = v.colOrdinal();
                final double ret0 = cumReturns.data().getDouble(rowOrdinal-1, colOrdinal);
                final double ret1 = cumReturns.data().getDouble(rowOrdinal, colOrdinal);
                return ((ret1 + 1d) / (ret0 + 1d)) - 1d;
            }
        });
    }

}
