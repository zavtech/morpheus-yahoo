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
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A unit test for the Yahoo option quote source
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooOptionQuoteTest {

    private Set<String> optionTypes = new HashSet<>(Arrays.asList("CALL", "PUT"));
    private Array<YahooField> fields = Array.of(
            YahooField.TICKER,
            YahooField.OPTION_TYPE,
            YahooField.EXPIRY_DATE,
            YahooField.PX_STRIKE,
            YahooField.PX_LAST,
            YahooField.PX_CHANGE,
            YahooField.PX_CHANGE_PERCENT,
            YahooField.PX_BID,
            YahooField.PX_ASK,
            YahooField.PX_VOLUME,
            YahooField.OPEN_INTEREST,
            YahooField.IMPLIED_VOLATILITY
    );


    @DataProvider(name="tickers")
    public Object[][] tickers() {
        return new Object[][] { {"AAPL"}, { "IBM" }, { "GE" }, { "MMM" } };
    }


    @Test(dataProvider = "tickers")
    public void testOptionExiryDates(String ticker) {
        final YahooFinance yahoo = new YahooFinance();
        final Set<LocalDate> expiryDates = yahoo.getOptionExpiryDates(ticker);
        Assert.assertTrue(expiryDates.size() > 0, "There are option expiries for " + ticker);
    }


    @Test(dataProvider = "tickers")
    public void testOptionQuotes(String ticker) {
        final YahooFinance yahoo = new YahooFinance();
        final Set<LocalDate> expiryDates = yahoo.getOptionExpiryDates(ticker);
        final LocalDate expiryDate = expiryDates.iterator().next();
        final DataFrame<String,YahooField> frame = yahoo.getOptionQuotes(ticker, expiryDate.toString());
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd");
        Assert.assertTrue(frame != null, "A DataFrame was produced for " + expiryDate);
        Assert.assertTrue(frame.rowCount() > 0, "The DataFrame contains rows for " + expiryDate);
        Assert.assertTrue(frame.colCount() > 0, "The DataFrame contains columns for " + expiryDate);
        Assert.assertEquals(frame.rows().select(row -> row.getValue(YahooField.EXPIRY_DATE).equals(expiryDate)).rowCount(), frame.rowCount());
        fields.forEach(field -> Assert.assertTrue(frame.cols().contains(field), "The DataFrame contains column for " + field.getName()));
        frame.out().print();
        frame.rows().forEach(row -> {
            Assert.assertTrue(row.key().contains(ticker), "The row key contains the ticker");
            Assert.assertEquals(row.getValue(YahooField.TICKER), ticker, "The tickers match");
            Assert.assertTrue(optionTypes.contains(row.<String>getValue(YahooField.OPTION_TYPE)));
            Assert.assertEquals(row.getValue(YahooField.EXPIRY_DATE).getClass(), LocalDate.class, "Expiry date is of LocalDate");
            Assert.assertEquals(row.getValue(YahooField.PX_STRIKE).getClass(), Double.class);
            final String token = formatter.format(expiryDate);
            Assert.assertTrue(row.key().contains(token), "The option ticker contains expiry string: " + token);
        });
     }
}
