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
package com.zavtech.morpheus.yahoo;

import java.time.LocalDate;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;

/**
 * A unit test for the YahooReturnSource
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooReturnSourceTest {

    /**
     * Static initializer
     */
    static {
        DataFrameSource.register(new YahooReturnSource());
        DataFrameSource.register(new YahooQuoteHistorySource());
    }


    @Test()
    public void testDailyReturnSource() {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final DataFrame<LocalDate,String> returns = source.read(options -> {
            options.withStartDate(LocalDate.of(2014, 1, 1));
            options.withEndDate(LocalDate.of(2015, 2, 4));
            options.withTickers("AAPL", "SPY");
            options.daily();
        });
        returns.out().print();
        Asserts.assertEquals(returns.rowCount(), 274, "Has expected row count");
        Assert.assertEquals(returns.colCount(), 2, "Has expected column count");
        Assert.assertTrue(returns.cols().containsAll(Array.of("AAPL", "SPY")), "Has expected tickers");
        Assert.assertEquals(returns.rows().firstKey().get(), LocalDate.of(2014, 1, 2));
        Assert.assertEquals(returns.rows().lastKey().get(), LocalDate.of(2015, 2, 3));
        Assert.assertEquals(returns.data().getDouble(0, "AAPL"), 0d, "The first AAPL return is zero");
        Assert.assertEquals(returns.data().getDouble(0, "SPY"), 0d, "The first SPY return is zero");

        //returns.write().csv(o -> o.setFile("./src/test/resources/returns/daily-returns.csv"));

        DataFrameAsserts.assertEqualsByIndex(returns, DataFrame.read().<LocalDate>csv(options -> {
            options.setResource("/returns/daily-returns.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.setExcludeColumnIndexes(0);
        }));
    }


    @Test()
    public void testWeeklyReturns() {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final DataFrame<LocalDate,String> returns = source.read(options -> {
            options.weekly();
            options.withTickers("AAPL", "SPY");
            options.withStartDate(LocalDate.of(2014, 1, 1));
            options.withEndDate(LocalDate.of(2015, 2, 4));
        });
        returns.out().print();
        Asserts.assertEquals(returns.rowCount(), 274, "Has expected row count");
        Assert.assertEquals(returns.colCount(), 2, "Has expected column count");
        Assert.assertTrue(returns.cols().containsAll(Array.of("AAPL", "SPY")), "Has expected tickers");
        Assert.assertEquals(returns.rows().firstKey().get(), LocalDate.of(2014, 1, 2));
        Assert.assertEquals(returns.rows().lastKey().get(), LocalDate.of(2015, 2, 3));
        Assert.assertEquals(returns.data().getDouble(0, "AAPL"), 0d, "The first AAPL return is zero");
        Assert.assertEquals(returns.data().getDouble(0, "SPY"), 0d, "The first SPY return is zero");

        //returns.write().csv(o -> o.setFile("./src/test/resources/returns/weekly-returns.csv"));

        DataFrameAsserts.assertEqualsByIndex(returns, DataFrame.read().<LocalDate>csv(options -> {
            options.setResource("/returns/weekly-returns.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.setExcludeColumnIndexes(0);
        }));
    }


    @Test()
    public void testMonthlyReturns() {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final DataFrame<LocalDate,String> returns = source.read(options -> {
            options.monthly();
            options.withTickers("AAPL", "SPY");
            options.withStartDate(LocalDate.of(2014, 1, 1));
            options.withEndDate(LocalDate.of(2015, 2, 4));
        });
        returns.out().print();
        Asserts.assertEquals(returns.rowCount(), 274, "Has expected row count");
        Assert.assertEquals(returns.colCount(), 2, "Has expected column count");
        Assert.assertTrue(returns.cols().containsAll(Array.of("AAPL", "SPY")), "Has expected tickers");
        Assert.assertEquals(returns.rows().firstKey().get(), LocalDate.of(2014, 1, 2));
        Assert.assertEquals(returns.rows().lastKey().get(), LocalDate.of(2015, 2, 3));
        Assert.assertEquals(returns.data().getDouble(0, "AAPL"), 0d, "The first AAPL return is zero");
        Assert.assertEquals(returns.data().getDouble(0, "SPY"), 0d, "The first SPY return is zero");

        //returns.write().csv(o -> o.setFile("./src/test/resources/returns/monthly-returns.csv"));

        DataFrameAsserts.assertEqualsByIndex(returns, DataFrame.read().<LocalDate>csv(options -> {
            options.setResource("/returns/monthly-returns.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.setExcludeColumnIndexes(0);
        }));
    }



    @Test()
    public void testCumulativeReturns() {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final DataFrame<LocalDate,String> returns = source.read(options -> {
            options.cumulative();
            options.withTickers("AAPL", "SPY");
            options.withStartDate(LocalDate.of(2014, 1, 1));
            options.withEndDate(LocalDate.of(2015, 2, 4));
        });
        returns.out().print();
        Asserts.assertEquals(returns.rowCount(), 274, "Has expected row count");
        Assert.assertEquals(returns.colCount(), 2, "Has expected column count");
        Assert.assertTrue(returns.cols().containsAll(Array.of("AAPL", "SPY")), "Has expected tickers");
        Assert.assertEquals(returns.rows().firstKey().get(), LocalDate.of(2014, 1, 2));
        Assert.assertEquals(returns.rows().lastKey().get(), LocalDate.of(2015, 2, 3));
        Assert.assertEquals(returns.data().getDouble(0, "AAPL"), 0d, "The first AAPL return is zero");
        Assert.assertEquals(returns.data().getDouble(0, "SPY"), 0d, "The first SPY return is zero");

        //returns.write().csv(o -> o.setFile("./src/test/resources/returns/cumulative-returns.csv"));

        DataFrameAsserts.assertEqualsByIndex(returns, DataFrame.read().<LocalDate>csv(options -> {
            options.setResource("/returns/cumulative-returns.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.setExcludeColumnIndexes(0);
        }));
    }





}
