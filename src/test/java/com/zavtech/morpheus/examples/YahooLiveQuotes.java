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

import java.time.LocalDate;

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.yahoo.YahooField;
import com.zavtech.morpheus.yahoo.YahooFinance;
import com.zavtech.morpheus.yahoo.YahooOptionSource;
import com.zavtech.morpheus.yahoo.YahooQuoteHistorySource;
import com.zavtech.morpheus.yahoo.YahooQuoteLiveSource;
import com.zavtech.morpheus.yahoo.YahooReturnSource;
import com.zavtech.morpheus.yahoo.YahooStatsSource;

/**
 * Examples of how to pull latest quotes
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooLiveQuotes {

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
    public void latestQuotes1() {
        YahooQuoteLiveSource source = DataFrameSource.lookup(YahooQuoteLiveSource.class);
        DataFrame<String, YahooField> quotes = source.read(options -> {
            options.withTickers("AAPL", "BLK", "NFLX", "ORCL", "GS", "C", "GOOGL", "MSFT", "AMZN");
        });

        quotes.out().print();

        String name = quotes.data().getValue("AAPL", YahooField.NAME);
        double closePrice = quotes.data().getDouble("AAPL", YahooField.PX_LAST);
        LocalDate date = quotes.data().getValue("AAPL", YahooField.PX_LAST_DATE);
    }


    @Test()
    public void latestQuotes2() {
        YahooQuoteLiveSource source = DataFrameSource.lookup(YahooQuoteLiveSource.class);
        DataFrame<String, YahooField> quotes = source.read(options -> {
            options.withTickers("AAPL", "BLK", "NFLX", "ORCL", "GS", "C", "GOOGL", "MSFT", "AMZN");
            options.withFields(
                YahooField.PX_LAST,
                YahooField.PX_BID,
                YahooField.PX_ASK,
                YahooField.PX_VOLUME,
                YahooField.PX_CHANGE,
                YahooField.PX_LAST_DATE,
                YahooField.PX_LAST_TIME
            );
        });

        quotes.out().print();

    }

}
