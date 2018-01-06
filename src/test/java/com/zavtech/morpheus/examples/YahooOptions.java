/*
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
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameGrouping;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.util.Tuple;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooField;
import com.zavtech.morpheus.yahoo.YahooFinance;
import com.zavtech.morpheus.yahoo.YahooOptionSource;
import com.zavtech.morpheus.yahoo.YahooQuoteHistorySource;
import com.zavtech.morpheus.yahoo.YahooQuoteLiveSource;
import com.zavtech.morpheus.yahoo.YahooReturnSource;
import com.zavtech.morpheus.yahoo.YahooStatsSource;
import static com.zavtech.morpheus.yahoo.YahooField.*;

/**
 * Examples of how to use Yahoo Finance Options api
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooOptions {

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
    public static void optionExpiryDates() {
        final YahooFinance yahoo = new YahooFinance();
        final Set<LocalDate> expiryDates = yahoo.getOptionExpiryDates("SPY");
        expiryDates.forEach(IO::println);
    }


    @Test()
    public static void optionData() {
        final String ticker = "SPY";
        final YahooFinance yahoo = new YahooFinance();
        final Set<LocalDate> expiryDates = yahoo.getOptionExpiryDates(ticker);
        final LocalDate nextExpiry = expiryDates.iterator().next();
        final DataFrame<String,YahooField> optionQuotes = yahoo.getOptionQuotes(ticker, nextExpiry);

        final DataFrame<String,YahooField> calls = optionQuotes.rows().select(row -> {
            final String type = row.getValue(YahooField.OPTION_TYPE);
            return type.equalsIgnoreCase("Call");
        });

        final DataFrame<String,YahooField> puts = optionQuotes.rows().select(row -> {
            final String type = row.getValue(YahooField.OPTION_TYPE);
            return type.equalsIgnoreCase("Put");
        });

        calls.out().print(10);
        puts.out().print(10);
    }




    @Test()
    public void putCallRatio() throws Exception {

        String ticker = "SPY";
        YahooFinance yahoo = new YahooFinance();
        DataFrame<String,YahooField> quotes = yahoo.getOptionQuotes(ticker);
        Array<LocalDate> strikes = quotes.col(YahooField.EXPIRY_DATE).<LocalDate>distinct().sort(true);
        DataFrame<LocalDate,String> openInterest = DataFrame.ofDoubles(strikes, Array.of("CALL", "PUT"));

        quotes.rows().groupBy(YahooField.OPTION_TYPE, YahooField.EXPIRY_DATE).forEach(1, (key, group) -> {
            if (group.rowCount() > 0) {
                String optionType = key.item(0);
                LocalDate expiry = key.item(1);
                double openInt = group.col(YahooField.OPEN_INTEREST).stats().sum();
                openInterest.data().setDouble(expiry, optionType, openInt);
            }
        });

        openInterest.cols().add("P/C Ratio", Double.class, v -> {
            double putOpenInt = v.row().getDouble("PUT");
            double callOpenInt = v.row().getDouble("CALL");
            return callOpenInt > 0 ? putOpenInt / callOpenInt : Double.NaN;
        });

        openInterest.out().print(100);
        double mean = openInterest.col("P/C Ratio").stats().mean();
        openInterest.col("P/C Ratio").applyDoubles(v -> v.getDouble() - mean);

        DataFrame<String,String> data = openInterest.cols().select("P/C Ratio").rows().mapKeys(r -> r.key().toString());
        Chart.create().withBarPlot(data, false, chart -> {
            chart.plot().orient().horizontal();
            chart.show();
        });

        Thread.currentThread().join();

    }






    @Test()
    public void callOptionSmile() throws Exception {
        String ticker = "SPY";
        //Instantiate Yahoo convenience adapter
        YahooFinance yahoo = new YahooFinance();
        //Select last price for underlying
        double lastPrice = yahoo.getLiveQuotes(Array.of(ticker)).data().getDouble(0, YahooField.PX_LAST);
        //Select call options with strike price within 10% of current market price and non zero vol
        DataFrame<String,YahooField> options = yahoo.getOptionQuotes(ticker).rows().select(row -> {
            final String type = row.getValue(YahooField.OPTION_TYPE);
            if (!type.equalsIgnoreCase("CALL")) {
                return false;
            } else {
                final double strike = row.getDouble(YahooField.PX_STRIKE);
                final double impliedVol = row.getDouble(YahooField.IMPLIED_VOLATILITY);
                return impliedVol > 0 && strike > lastPrice * 0.9d && strike < lastPrice * 1.1d;
            }
        });

        //Select all distinct expiry dates for quotes
        Array<LocalDate> expiryDates = options.col(YahooField.EXPIRY_DATE).distinct();
        //Creates frames for each expiry including only strike price and implied-vol columns
        Array<DataFrame<Integer,String>> frames = expiryDates.map(v -> {
            final LocalDate expiry = v.getValue();
            final DataFrame<String,YahooField> calls = options.rows().select(row -> {
                final LocalDate date = row.getValue(YahooField.EXPIRY_DATE);
                return expiry.equals(date);
            });
            return DataFrame.of(Range.of(0, calls.rowCount()), String.class, columns -> {
                columns.add("Strike", calls.col(YahooField.PX_STRIKE).toArray());
                columns.add(expiry.toString(), calls.col(YahooField.IMPLIED_VOLATILITY).toArray().applyDoubles(x -> {
                    return x.getDouble() * 100d;
                }));
            });
        });

        //Create plot of N frames each for a different expiry
        Chart.create().asSwing().withLinePlot(frames.getValue(0), "Strike", chart -> {
            for (int i=1; i<frames.length(); ++i) {
                DataFrame<Integer,String> data = frames.getValue(i);
                chart.plot().<String>data().add(data, "Strike");
                chart.plot().render(i).withLines(true, false);
            }
            chart.plot().render(0).withLines(true, false);
            chart.plot().axes().domain().label().withText("Strike Price");
            chart.plot().axes().range(0).label().withText("Implied Volatility");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.title().withText("SPY CALL Option Implied Volatility Smiles");
            chart.legend().on().right();
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/option_call_smile.png"), 700, 500, true);
            chart.show();
        });

        Thread.currentThread().join();
    }



    @Test()
    public void putOptionSmile() throws Exception {
        String ticker = "SPY";
        //Instantiate Yahoo convenience adapter
        YahooFinance yahoo = new YahooFinance();
        //Select last price for underlying
        double lastPrice = yahoo.getLiveQuotes(Array.of(ticker)).data().getDouble(0, YahooField.PX_LAST);
        //Select call options with strike price within 10% of current market price and non zero vol
        DataFrame<String,YahooField> options = yahoo.getOptionQuotes(ticker).rows().select(row -> {
            final String type = row.getValue(YahooField.OPTION_TYPE);
            if (!type.equalsIgnoreCase("PUT")) {
                return false;
            } else {
                final double strike = row.getDouble(YahooField.PX_STRIKE);
                final double impliedVol = row.getDouble(YahooField.IMPLIED_VOLATILITY);
                return impliedVol > 0 && strike > lastPrice * 0.9d && strike < lastPrice * 1.1d;
            }
        });

        //Select all distinct expiry dates for quotes
        Array<LocalDate> expiryDates = options.col(YahooField.EXPIRY_DATE).distinct();
        //Creates frames for each expiry including only strike price and implied-vol columns
        Array<DataFrame<Integer,String>> frames = expiryDates.map(v -> {
            final LocalDate expiry = v.getValue();
            final DataFrame<String,YahooField> calls = options.rows().select(row -> {
                final LocalDate date = row.getValue(YahooField.EXPIRY_DATE);
                return expiry.equals(date);
            });
            return DataFrame.of(Range.of(0, calls.rowCount()), String.class, columns -> {
                columns.add("Strike", calls.col(YahooField.PX_STRIKE).toArray());
                columns.add(expiry.toString(), calls.col(YahooField.IMPLIED_VOLATILITY).toArray().applyDoubles(x -> {
                    return x.getDouble() * 100d;
                }));
            });
        });

        //Create plot of N frames each for a different expiry
        Chart.create().asSwing().withLinePlot(frames.getValue(0), "Strike", chart -> {
            for (int i=1; i<frames.length(); ++i) {
                DataFrame<Integer,String> data = frames.getValue(i);
                chart.plot().<String>data().add(data, "Strike");
                chart.plot().render(i).withLines(true, false);
            }
            chart.plot().render(0).withLines(true, false);
            chart.plot().axes().domain().label().withText("Strike Price");
            chart.plot().axes().range(0).label().withText("Implied Volatility");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.title().withText("SPY PUT Option Implied Volatility Smiles");
            chart.legend().on().right();
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/option_put_smile.png"), 700, 500, true);
            chart.show();
        });

        Thread.currentThread().join();
    }




}
