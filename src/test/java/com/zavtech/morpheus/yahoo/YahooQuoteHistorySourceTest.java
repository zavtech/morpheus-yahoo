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
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.util.text.printer.Printer;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A unit test for the Yahoo Quote history source
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooQuoteHistorySourceTest {

    private Array<YahooField> fields = Array.of(
        YahooField.PX_OPEN,
        YahooField.PX_HIGH,
        YahooField.PX_LOW,
        YahooField.PX_CLOSE,
        YahooField.PX_VOLUME,
        YahooField.PX_SPLIT_RATIO,
        YahooField.PX_CHANGE,
        YahooField.PX_CHANGE_PERCENT
    );


    @DataProvider(name="tickers")
    public Object[][] tickers() {
        return new Object[][] { {"AAPL"}, { "IBM" }, { "GE" }, { "MMM" }, { "BLK" }, { "SPY" } };
    }


    @Test(dataProvider = "tickers")
    public void testQuoteHistory(String ticker) {
        final LocalDate start = LocalDate.of(2014, 1, 1);
        final LocalDate end = LocalDate.of(2015, 2, 4);
        final YahooFinance yahoo = new YahooFinance();
        System.out.println("Loading quotes for " + ticker);
        final DataFrame<LocalDate,YahooField> quotes = yahoo.getQuoteBars(ticker, start, end, false);
        Assert.assertTrue(quotes.rowCount() > 0, "There are rows in the frame");
        Assert.assertTrue(quotes.colCount() > 0, "There are columns in the frame");
        fields.forEach(field -> Assert.assertTrue(quotes.cols().contains(field), "The DataFrame contains column for " + field.getName()));
        Assert.assertTrue(quotes.rows().firstKey().get().compareTo(start) >= 0);
        Assert.assertTrue(quotes.rows().lastKey().get().compareTo(end) <= 0);
        quotes.out().print();
        IntStream.range(1, quotes.rowCount()).forEach(rowIndex -> {
            final LocalDate previous = quotes.rows().key(rowIndex-1);
            final LocalDate current = quotes.rows().key(rowIndex);
            final double previousClose = quotes.data().getDouble(rowIndex-1, YahooField.PX_CLOSE);
            final double currentClose = quotes.data().getDouble(rowIndex, YahooField.PX_CLOSE);
            final double expectedChange = currentClose - previousClose;
            final double actualChange = quotes.data().getDouble(rowIndex, YahooField.PX_CHANGE);
            final double expectedChangePercent = currentClose / previousClose - 1d;
            Assert.assertTrue(previous.isBefore(current), "Dates are in ascending order");
            Assert.assertTrue(quotes.data().getDouble(rowIndex, YahooField.PX_HIGH) >= quotes.data().getDouble(rowIndex, YahooField.PX_LOW), "High >= Low");
            Assert.assertEquals(actualChange, expectedChange, 0.0001, "The price change is calculated correctly at " + quotes.rows().key(rowIndex));
            Assert.assertEquals(quotes.data().getDouble(rowIndex, YahooField.PX_CHANGE_PERCENT), expectedChangePercent, 0.00001);
        });

        /*
        quotes.write().csv(o -> {
            o.setFile(String.format("./src/test/resources/quotes/%s-quotes.csv", ticker.toLowerCase()));
            o.setFormats(formats -> {
                formats.setPrinter(YahooField.class, Printer.forObject(YahooField::getName));
            });
        });
        */

        Array<YahooField> compare = Array.of(
            YahooField.PX_OPEN,
            YahooField.PX_HIGH,
            YahooField.PX_LOW,
            YahooField.PX_CLOSE
        );

        DataFrameAsserts.assertEqualsByIndex(quotes.cols().select(compare), DataFrame.read().<LocalDate>csv(options -> {
            options.setResource(String.format("/quotes/%s-quotes.csv", ticker.toLowerCase()));
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.setExcludeColumnIndexes(0);
        }).cols().mapKeys(column -> {
            final String fieldName = column.key();
            return YahooField.getField(fieldName);
        }).cols().select(compare));
    }

}
