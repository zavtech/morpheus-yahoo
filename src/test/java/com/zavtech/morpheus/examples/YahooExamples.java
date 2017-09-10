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

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooField;
import com.zavtech.morpheus.yahoo.YahooFinance;
import com.zavtech.morpheus.yahoo.YahooQuoteHistorySource;
import com.zavtech.morpheus.yahoo.YahooReturnSource;

/**
 * Various examples of how to use the Yahoo Adapter
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
        DataFrameSource.register(new YahooReturnSource());
        DataFrameSource.register(new YahooQuoteHistorySource());
    }


    @Test()
    public void returns() throws Exception {
        final YahooReturnSource source = DataFrameSource.lookup(YahooReturnSource.class);
        final DataFrame<LocalDate,String> returns = source.read(options -> {
            options.withStartDate(LocalDate.of(2017, 1, 1));
            options.withEndDate(LocalDate.now());
            options.withTickers("VWO", "VNQ", "VEA", "DBC", "VTI", "BND");
            options.cumulative();
        }).cols().mapKeys(column -> {
            switch (column.key()) {
                case "VWO": return "EM Equities";
                case "VNQ": return "Real-estate";
                case "VEA": return "Foreign Equity";
                case "DBC": return "Commodities";
                case "VTI": return "Large Blend";
                case "BND": return "Fixed Income";
                default:    return column.key();
            }
        });

        Chart.create().asHtml().withAreaPlot(returns, false, chart -> {
            chart.title().withText("Major Asset Class Cumulative Returns YTD");
            chart.subtitle().withText("ETF Proxies");
            chart.legend().on().right();
            chart.plot().axes().domain().label().withText("Date");
            chart.plot().axes().range(0).label().withText("Return");
            chart.show();
        });
    }


    @Test()
    public void options() throws Exception {
        final YahooFinance yahoo = new YahooFinance();
        final DataFrame<String,YahooField> options = yahoo.getOptionQuotes("AAPL");
        options.rows().sort(false, YahooField.OPEN_INTEREST);
        options.out().print(200);
    }


}
