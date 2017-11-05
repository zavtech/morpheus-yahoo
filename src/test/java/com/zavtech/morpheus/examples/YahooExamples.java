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

import java.io.File;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.Set;
import java.util.jar.Pack200;
import java.util.stream.Stream;

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooField;
import com.zavtech.morpheus.yahoo.YahooFinance;
import com.zavtech.morpheus.yahoo.YahooOptionSource;
import com.zavtech.morpheus.yahoo.YahooQuoteHistorySource;
import com.zavtech.morpheus.yahoo.YahooQuoteLiveSource;
import com.zavtech.morpheus.yahoo.YahooReturnSource;
import com.zavtech.morpheus.yahoo.YahooStatsSource;

/**
 * Various examples of how to use the Yahoo Adapter
 *
 * 1. Basic query for historical quotes, adjusted and unadjusted
 * 2. Basic plot of returns for multiple assets
 * 3. Basic query for equity level statistics
 * 4. Basic query for latest quotes
 * 5. Basic query for options data
 * 6. Calculate beta single example
 * 7. Calculate beta multiple example
 * 8. Plot option volatility smile
 * 9. Covariance and Correlation matrix (VTI, VEA, VWO, VIG, XLE, VTEB ... VNQ, LQD, EMB)
 * 10. Smoothing returns
 * 11. Efficient frontier example
 *
 * Wealthfront Portfolio
 * ---------------------
 * US Stocks: 35% -         VTI, ITOT, SCHB
 * Foreign Stocks: 21% -    VEA, IXUS, SCHF
 * EM Stocks: 16% -         VWO, IEMG, SCHE
 * Dividend Stocks:  8% -   VIG, DVY, SCHD
 * Natural Resources: 5% -  XLE, DJP, VDE
 * Municipal Bonds: 15% -   VTEB, TFI, MUB
 *
 * Real-Estate: 14% -       VNQ, IYR, SCHH
 * Corproate Bonds: 10% -   LQD, CORP, PFIG
 * EM Bonds: 7% -           EMB, PCY, EMLC
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooExamples {

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
    public void historicalQuotes1() throws Exception {
        String ticker = "BLK";
        YahooQuoteHistorySource source = DataFrameSource.lookup(YahooQuoteHistorySource.class);
        DataFrame<LocalDate,YahooField> closePrices = source.read(options -> {
            options.withStartDate(LocalDate.now().minusYears(10));
            options.withEndDate(LocalDate.now());
            options.withDividendAdjusted(true);
            options.withTicker(ticker);
        }).cols().select(YahooField.PX_CLOSE);
        Chart.create().asSwing().withAreaPlot(closePrices, false, chart -> {
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Close Price");
            chart.title().withText(ticker + ": Close Prices (Last 10 Years)");
            chart.legend().off();
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/blk_prices_1.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();

    }


    @Test()
    public void historicalQuotes2() throws Exception {
        String ticker = "BLK";
        YahooQuoteHistorySource source = DataFrameSource.lookup(YahooQuoteHistorySource.class);

        //Load both adjusted and unadjusted quotes
        DataFrame<LocalDate,String> frame = DataFrame.combineFirst(
            Stream.of("Adjusted", "Unadjusted").map(style -> source.read(options -> {
                options.withStartDate(LocalDate.now().minusYears(10));
                options.withEndDate(LocalDate.now());
                options.withDividendAdjusted(style.equals("Adjusted"));
                options.withTicker(ticker);
            }).cols().select(YahooField.PX_CLOSE).cols().mapKeys(column -> style))
        );

        //Plot close prices from both these series
        Chart.create().asSwing().withLinePlot(frame, chart -> {
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Close Price");
            chart.title().withText(ticker + ": Adjusted & Unadjusted Close Prices");
            chart.legend().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/blk_prices_2.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void historicalQuotes3() throws Exception {
        final String ticker = "NFLX";
        final LocalDate end = LocalDate.now();
        final LocalDate start = end.minusYears(10);
        final YahooFinance yahoo = new YahooFinance();

        Iterator<DataFrame<LocalDate,String>> iterator = Stream.of("Adjusted", "Unadjusted").map(style -> {
            final boolean adjusted = style.equals("Adjusted");
            final DataFrame<LocalDate,YahooField> frame = yahoo.getQuoteBars(ticker, start, end, adjusted);
            return frame.cols().select(YahooField.PX_CLOSE).cols().mapKeys(column -> style);
        }).iterator();

        Chart.create().asSwing().withLinePlot(iterator.next(), chart -> {
            chart.plot().<String>data().add(iterator.next());
            chart.plot().render(1).withLines(false, false);
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Close Price");
            chart.title().withText(ticker + ": Adjusted & Unadjusted Close Prices");
            chart.legend().bottom();
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/nflx_prices_1.png"), 700, 500, true);
            chart.show();
        });
    }


    @Test()
    public void latestQuotes1() {
        final YahooFinance yahoo = new YahooFinance();
        final Array<String> tickers = Array.of("AAPL", "BLK", "NFLX", "ORCL", "GS", "C", "GOOGL", "MSFT", "AMZN");
        final DataFrame<String, YahooField> quotes = yahoo.getLiveQuotes(tickers);
        quotes.out().print();

        String name = quotes.data().getValue("AAPL", YahooField.NAME);
        double closePrice = quotes.data().getDouble("AAPL", YahooField.PX_LAST);
        LocalDate date = quotes.data().getValue("AAPL", YahooField.PX_LAST_DATE);
    }


    @Test()
    public void latestQuotes2() {
        final YahooFinance yahoo = new YahooFinance();
        final Array<String> tickers = Array.of("AAPL", "BLK", "NFLX", "ORCL", "GS", "C", "GOOGL", "MSFT", "AMZN");
        final DataFrame<String, YahooField> quotes = yahoo.getLiveQuotes(tickers, Array.of(
            YahooField.PX_LAST,
            YahooField.PX_BID,
            YahooField.PX_ASK,
            YahooField.PX_VOLUME,
            YahooField.PX_CHANGE,
            YahooField.PX_LAST_DATE,
            YahooField.PX_LAST_TIME
        ));

        quotes.out().print();

    }



    @Test()
    public void correlation() {
        final LocalDate start = LocalDate.of(2016, 1, 1);
        final LocalDate end = LocalDate.of(2017, 9, 8);
        final YahooFinance yahoo = new YahooFinance();
        final Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VIG", "XLE", "VTEB", "VNQ", "LQD", "EMB");
        final DataFrame<LocalDate,String> returns = yahoo.getDailyReturns(start, end, tickers);
        final DataFrame<String,String> cov = returns.cols().stats().covariance();
        final DataFrame<String,String> correl = returns.cols().stats().correlation();
        cov.out().print();
        correl.out().print();
    }



    @Test()
    public static void portfolioReturns() throws Exception {
        final LocalDate end = LocalDate.now();
        final LocalDate start = end.minusYears(2);
        final YahooFinance yahoo = new YahooFinance();
        final Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VIG", "XLE", "VTEB");
        final DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);

        //Compute evolving market values based on asset return, must be sequential as path dependent
        final DataFrame<LocalDate,String> marketValues = cumReturns.copy().applyDoubles(v -> Double.NaN);
        marketValues.rows().forEach(row -> {
            final int ordinal = row.ordinal();
            if (ordinal == 0) {
                row.setDouble("VTI", 35d);
                row.setDouble("VEA", 21d);
                row.setDouble("VWO", 16d);
                row.setDouble("VIG", 8d);
                row.setDouble("XLE", 5d);
                row.setDouble("VTEB", 15d);
            } else {
                row.applyDoubles(v -> {
                    final int rowOrdinal = v.rowOrdinal();
                    final String ticker = v.colKey();
                    final double initValue = v.col().getDouble(0);
                    final double cumReturn = cumReturns.data().getDouble(rowOrdinal, ticker);
                    return initValue * (1d + cumReturn);
                });
            }
        });

        final DataFrame<LocalDate,String> pnlValues = marketValues.copy().applyDoubles(v -> {
            return v.getDouble() - marketValues.data().getDouble(0, v.colKey());
        });

        marketValues.out().print();
        pnlValues.out().print();

        Chart.create().asHtml().withAreaPlot(pnlValues,  true, chart -> {
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.plot().axes().range(0).format().withPattern("0.##'%';-0.##'%'");
            chart.title().withText("Wealthfront Model Portfolio Return Decomposition");
            chart.legend().on().right();
            chart.show();
        });
    }




}
