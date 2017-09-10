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
import java.util.Set;

import com.zavtech.morpheus.frame.DataFrame;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A unit test for the Yahoo live quote data source
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooQuoteLiveTest {

    private YahooFinance yahoo = new YahooFinance();

    @DataProvider(name = "tickers")
    public Object[][] tickers() {
        return new Object[][]{
            { Arrays.asList("AAPL", "MSFT", "GOOG", "ORCL", "FDX", "NFLX", "DELL", "SYMC", "XXX", "GE", "GS", "ML", "UBS", "BLK") }
        };
    }


    @Test(dataProvider = "tickers")
    public void testLiveQuotesWithAllFields(List<String> tickers) {
        final DataFrame<String, YahooField> frame = yahoo.getLiveQuotes(new HashSet<>(tickers));
        frame.out().print();
        final Set<YahooField> fields = new YahooQuoteLiveSource().getFieldSet();
        Assert.assertEquals(frame.rowCount(), tickers.size(), "There are rows in the frame");
        Assert.assertEquals(frame.colCount(), fields.size(), "There are columns in the frame");
        fields.forEach(field -> Assert.assertTrue(frame.cols().contains(field), "The DataFrame contains column for " + field.getName()));
        tickers.forEach(ticker -> Assert.assertTrue(frame.rows().contains(ticker), "The DataFrame contains a row for " + ticker));
    }


    @Test(dataProvider = "tickers")
    public void testLiveQuotesWithSubsetOfFields(List<String> tickers) {
        final YahooField[] fields = new YahooField[] { YahooField.PX_BID, YahooField.PX_BID_SIZE, YahooField.PX_ASK, YahooField.PX_ASK_SIZE };
        final DataFrame<String, YahooField> frame = yahoo.getLiveQuotes(new HashSet<>(tickers), fields);
        frame.out().print();
        Assert.assertEquals(frame.rowCount(), tickers.size(), "There are rows in the frame");
        Assert.assertEquals(frame.colCount(), fields.length, "There are columns in the frame");
        Arrays.asList(fields).forEach(field -> Assert.assertTrue(frame.cols().contains(field), "The DataFrame contains column for " + field.getName()));
        tickers.forEach(ticker -> Assert.assertTrue(frame.rows().contains(ticker), "The DataFrame contains a row for " + ticker));
    }

}
