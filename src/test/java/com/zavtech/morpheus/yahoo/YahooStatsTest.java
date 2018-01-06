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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A unit test for the Yahoo Finance equity statistics data source
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooStatsTest {

    @DataProvider(name = "tickers")
    public Object[][] tickers() {
        return new Object[][]{
            { Arrays.asList("AAPL", "MSFT", "GOOG", "ORCL", "FDX", "NFLX", "DELL", "SYMC", "XXX", "BLK") }
        };
    }

    @Test(dataProvider = "tickers")
    public void testStatsRequest(List<String> tickers) {
        final YahooFinance yahoo = new YahooFinance();
        final DataFrame<String,YahooField> frame = yahoo.getStatistics(new HashSet<>(tickers));
        final Array<YahooField> fields = new YahooStatsSource().getFields();
        Assert.assertEquals(frame.rowCount(), tickers.size(), "Frame has expected row count");
        Assert.assertEquals(frame.colCount(), fields.length(), "Frame has expected column count");
        tickers.forEach(ticker -> Assert.assertTrue(frame.rows().contains(ticker), "Frame contains row for " + ticker));
        fields.forEach(field -> Assert.assertTrue(frame.cols().contains(field), "Frame contains column for " + field));
        frame.out().print();
        frame.row("XXX").forEachValue(v -> {
            final Object value = v.getValue();
            if (value instanceof Number) {
                Assert.assertEquals(((Number)value).doubleValue(), Double.NaN);
            } else {
                Assert.assertTrue(value == null, "Value is null for incorrect ticker");
            }
        });
    }

}
