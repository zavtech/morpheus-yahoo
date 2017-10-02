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

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooOptionSource;
import com.zavtech.morpheus.yahoo.YahooQuoteHistorySource;
import com.zavtech.morpheus.yahoo.YahooQuoteLiveSource;
import com.zavtech.morpheus.yahoo.YahooReturnSource;
import com.zavtech.morpheus.yahoo.YahooStatsSource;

/**
 * Examples of how to use Yahoo Finance api to compute beta against certain risk factors
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooBeta {

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
    public void betaDaily() throws Exception {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final DataFrame<LocalDate,String> daily = source.read(options -> {
            options.withStartDate(LocalDate.of(2016, 1, 1));
            options.withEndDate(LocalDate.now());
            options.withTickers("AAPL", "SPY");
            options.daily();
        });

        Chart.create().withScatterPlot(daily.applyDoubles(v -> v.getDouble() * 100d), false, "SPY", chart -> {
            chart.title().withText("Regression of Daily AAPL Returns on SPY (HalfLife: 20d)");
            chart.plot().axes().domain().label().withText("SPY Returns");
            chart.plot().axes().range(0).label().withText("AAPL Returns");
            chart.plot().axes().domain().format().withPattern("0.00'%';-0.00'%'");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.plot().render(1).withDots();
            chart.plot().render(2).withDots();
            chart.plot().trend("AAPL");
            chart.legend().on().right();
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void betaMultiple() throws Exception {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final DataFrame<LocalDate,String> daily = source.read(options -> {
            options.withStartDate(LocalDate.of(2015, 1, 1));
            options.withEndDate(LocalDate.now());
            options.withTickers("AAPL", "SPY");
            options.daily();
        }).cols().replaceKey("AAPL", "Daily");
        final DataFrame<LocalDate,String> weekly = source.read(options -> {
            options.withStartDate(LocalDate.of(2015, 1, 1));
            options.withEndDate(LocalDate.now());
            options.withTickers("AAPL", "SPY");
            options.weekly();
        }).cols().replaceKey("AAPL", "Weekly");
        final DataFrame<LocalDate,String> monthly = source.read(options -> {
            options.withStartDate(LocalDate.of(2015, 1, 1));
            options.withEndDate(LocalDate.now());
            options.withTickers("AAPL", "SPY");
            options.monthly();
        }).cols().replaceKey("AAPL", "Monthly");

        Chart.create().asHtml().withScatterPlot(daily, false, "SPY", chart -> {
            chart.title().withText("Regression of Daily AAPL Returns on SPY");
            chart.plot().axes().domain().label().withText("SPY Returns");
            chart.plot().axes().range(0).label().withText("AAPL Returns");
            chart.plot().<String>data().add(weekly, "SPY");
            chart.plot().<String>data().add(monthly, "SPY");
            chart.plot().render(1).withDots();
            chart.plot().render(2).withDots();
            chart.plot().trend("Daily");
            chart.plot().trend("Weekly");
            chart.plot().trend("Monthly");
            chart.legend().on().right();
            chart.show();
        });

        Thread.currentThread().join();
    }


    @Test()
    public void dailyBeta() throws Exception {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final Array<String> tickers = Array.of("AAPL", "JNJ", "GS", "F");
        final Array<String> benchmarks = Array.of("SPY", "GLD", "USO", "BND");
        final DataFrame<LocalDate,String> returns = source.read(options -> {
            options.withStartDate(LocalDate.of(2015, 1, 1));
            options.withEndDate(LocalDate.now());
            options.withTickers(tickers.concat(benchmarks));
            options.daily();
        });

        tickers.forEach(ticker -> {
            final Iterator<String> regressor = benchmarks.iterator();

        });

    }


    @Test()
    public void weeklyBeta() throws Exception {

    }


    @Test()
    public void monthlyBeta() throws Exception {

    }

}
