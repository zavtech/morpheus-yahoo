/**
 * Copyright (C) 2014-2016 Xavier Witdouck
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
package com.zavtech.finance.yahoo;

import java.time.LocalDate;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
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
public class YahooQuoteHistoryTest {

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
        return new Object[][] { {"AAPL"}, { "IBM" }, { "GE" }, { "MMM" } };
    }

    @Test(dataProvider = "tickers")
    public void testQuoteHistory(String ticker) {
        final LocalDate start = LocalDate.of(2006, 1, 1);
        final LocalDate end = LocalDate.of(2009, 1, 1);
        final YahooFinance yahoo = new YahooFinance();
        final DataFrame<LocalDate,YahooField> frame1 = yahoo.getEndOfDayQuotes(ticker, start, end, true);
        Assert.assertTrue(frame1.rowCount() > 0, "There are rows in the frame");
        Assert.assertTrue(frame1.colCount() > 0, "There are columns in the frame");
        fields.forEach(field -> Assert.assertTrue(frame1.cols().contains(field), "The DataFrame contains column for " + field.getName()));
        Assert.assertTrue(frame1.rows().firstKey().get().compareTo(start) >= 0);
        Assert.assertTrue(frame1.rows().lastKey().get().compareTo(end) <= 0);
        final DataFrame<LocalDate,YahooField> frame2 = yahoo.getEndOfDayQuotes(ticker, start.toString(), end.toString(), true);
        Assert.assertEquals(frame1, frame2, "The two frames are equal");
        IntStream.range(1, frame1.rowCount()).forEach(rowIndex -> {
            final LocalDate previous = frame1.rows().key(rowIndex-1);
            final LocalDate current = frame1.rows().key(rowIndex);
            final double previousClose = frame1.data().getDouble(rowIndex-1, YahooField.PX_CLOSE);
            final double currentClose = frame1.data().getDouble(rowIndex, YahooField.PX_CLOSE);
            final double expectedChange = currentClose - previousClose;
            final double actualChange = frame1.data().getDouble(rowIndex, YahooField.PX_CHANGE);
            final double expectedChangePercent = currentClose / previousClose - 1d;
            Assert.assertTrue(previous.isBefore(current), "Dates are in ascending order");
            Assert.assertTrue(frame1.data().getDouble(rowIndex, YahooField.PX_HIGH) >= frame1.data().getDouble(rowIndex, YahooField.PX_LOW), "High >= Low");
            Assert.assertEquals(actualChange, expectedChange, 0.0001, "The price change is calculated correctly at " + frame1.rows().key(rowIndex));
            Assert.assertEquals(frame1.data().getDouble(rowIndex, YahooField.PX_CHANGE_PERCENT), expectedChangePercent, 0.00001);
        });
    }

}
