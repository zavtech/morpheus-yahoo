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
import java.util.Map;
import java.util.Set;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.text.SmartFormat;
import com.zavtech.morpheus.util.text.printer.Printer;

/**
 * A convenience class to expose a more specific API to request data from Yahoo Finance
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooFinance {

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

    /**
     * Constructor
     */
    public YahooFinance() {
        super();
    }

    /**
     * Returns a DataFrame containing equity statistics for the set of tickers specified
     * @param tickers   the set of security tickers
     * @return          the DataFrame of tickers
     */
    public DataFrame<String,YahooField> getStatistics(Iterable<String> tickers) {
        return DataFrameSource.lookup(YahooStatsSource.class).read(options -> {
            options.withTickers(tickers);
        });
    }


    /**
     * Returns a DataFrame containing equity statistics for the set of tickers specified
     * @param tickers   the set of security tickers
     * @return          the DataFrame of tickers
     */
    public DataFrame<String,YahooField> getStatistics(String... tickers) {
        return DataFrameSource.lookup(YahooStatsSource.class).read(options -> {
            options.withTickers(tickers);
        });
    }

    /**
     * Returns the option expiry dates for the underlying security ticker
     * @param ticker    the ticker for underlying security
     * @return          the set of option expiry dates
     */
    public Set<LocalDate> getOptionExpiryDates(String ticker) {
        return new YahooOptionSource().getExpiryDates(ticker);
    }

    /**
     * Returns a DataFrame with option quotes for the ticker on all expiry dates
     * @param ticker    the ticker reference for underlying security
     * @return          the DataFrame with option quotes
     */
    public DataFrame<String,YahooField> getOptionQuotes(String ticker) {
        return DataFrameSource.lookup(YahooOptionSource.class).read(options -> {
            options.withUnderlying(ticker);
        });
    }

    /**
     * Returns a DataFrame with option quotes for the ticker and expiry date specified
     * @param ticker    the ticker reference for underlying security
     * @param expiry    the expiry date expressed in ISO date formate yyyy-MM-dd
     * @return          the DataFrame with option quotes
     */
    public DataFrame<String,YahooField> getOptionQuotes(String ticker, String expiry) {
        return DataFrameSource.lookup(YahooOptionSource.class).read(options -> {
            options.withUnderlying(ticker);
            options.withExpiry(LocalDate.parse(expiry));
        });
    }

    /**
     * Returns a DataFrame with option quotes for the ticker and expiry date specified
     * @param ticker    the ticker reference for underlying security
     * @param expiry    the expiry date
     * @return          the DataFrame with option quotes
     */
    public DataFrame<String,YahooField> getOptionQuotes(String ticker, LocalDate expiry) {
        return DataFrameSource.lookup(YahooOptionSource.class).read(options -> {
            options.withUnderlying(ticker);
            options.withExpiry(expiry);
        });
    }


    /**
     * Returns a DataFrame of daily returns
     * @param start     the start date
     * @param end       the end date
     * @param tickers   the vector of tickers
     * @return          the frame of returns
     */
    public DataFrame<LocalDate,String> getDailyReturns(LocalDate start, LocalDate end, String...tickers) {
        return getDailyReturns(start, end, Collect.asList(tickers));
    }


    /**
     * Returns a DataFrame of daily returns
     * @param start     the start date
     * @param end       the end date
     * @param tickers   the vector of tickers
     * @return          the frame of returns
     */
    public DataFrame<LocalDate,String> getDailyReturns(LocalDate start, LocalDate end, Iterable<String> tickers) {
        return DataFrameSource.lookup(YahooReturnSource.class).read(options -> {
            options.withTickers(tickers);
            options.withStartDate(start);
            options.withEndDate(end);
            options.daily();
        });
    }


    /**
     * Returns a DataFrame of cumulative returns
     * @param start     the start date
     * @param end       the end date
     * @param tickers   the vector of tickers
     * @return          the frame of returns
     */
    public DataFrame<LocalDate,String> getCumReturns(LocalDate start, LocalDate end, String... tickers) {
        return getCumReturns(start, end, Collect.asList(tickers));
    }


    /**
     * Returns a DataFrame of cumulative returns
     * @param start     the start date
     * @param end       the end date
     * @param tickers   the vector of tickers
     * @return          the frame of returns
     */
    public DataFrame<LocalDate,String> getCumReturns(LocalDate start, LocalDate end, Iterable<String> tickers) {
        return DataFrameSource.lookup(YahooReturnSource.class).read(options -> {
            options.withTickers(tickers);
            options.withStartDate(start);
            options.withEndDate(end);
            options.cumulative();
        });
    }


    /**
     * Returns a DataFrame of live quotes for the tickers specified and all available fields
     * @param tickers   the set of security tickers
     * @return          the DataFrame of live quote data for all fields
     */
    public DataFrame<String,YahooField> getLiveQuotes(Iterable<String> tickers) {
        return DataFrameSource.lookup(YahooQuoteLiveSource.class).read(options -> {
           options.withTickers(tickers);
        });
    }


    /**
     * Returns a DataFrame of live quotes for the tickers and fields specified
     * @param tickers   the set of security tickers
     * @param fields    the set of fields
     * @return          the DataFrame of live quote data for all fields
     */
    public DataFrame<String,YahooField> getLiveQuotes(Iterable<String> tickers, YahooField... fields) {
        return DataFrameSource.lookup(YahooQuoteLiveSource.class).read(options -> {
            options.withTickers(tickers);
            options.withFields(fields);
        });
    }


    /**
     * Returns a DataFrame of live quotes for the tickers and fields specified
     * @param tickers   the set of security tickers
     * @param fields    the set of fields
     * @return          the DataFrame of live quote data for all fields
     */
    public DataFrame<String,YahooField> getLiveQuotes(Iterable<String> tickers, Iterable<YahooField> fields) {
        return DataFrameSource.lookup(YahooQuoteLiveSource.class).read(options -> {
            options.withTickers(tickers);
            options.withFields(fields);
        });
    }


    /**
     * Returns end of day OHLC quote bars for the security over the date range specified
     * @param ticker        the security ticker symbol
     * @param start         the start date for range, expressed as an ISO date of yyyy-MM-dd
     * @param end           the end date for range, expressed as an ISO date of yyyy-MM-dd
     * @param adjusted      true to adjust prices for splits and dividends
     * @return              the DataFrame contains OHLC end of day bars
     */
    public DataFrame<LocalDate,YahooField> getQuoteBars(String ticker, String start, String end, boolean adjusted) {
        return DataFrameSource.lookup(YahooQuoteHistorySource.class).read(options -> {
            options.withTicker(ticker);
            options.withStartDate(LocalDate.parse(start));
            options.withEndDate(LocalDate.parse(end));
            options.withDividendAdjusted(adjusted);
        });
    }

    /**
     * Returns end of day OHLC quote bars for the security over the date range specified
     * @param ticker        the security ticker symbol
     * @param start         the start date for range
     * @param end           the end date for range
     * @param adjusted      true to adjust prices for splits and dividends
     * @return              the DataFrame contains OHLC end of day bars
     */
    public DataFrame<LocalDate,YahooField> getQuoteBars(String ticker, LocalDate start, LocalDate end, boolean adjusted) {
        return DataFrameSource.lookup(YahooQuoteHistorySource.class).read(options -> {
            options.withTicker(ticker);
            options.withStartDate(start);
            options.withEndDate(end);
            options.withDividendAdjusted(adjusted);
        });
    }


    /**
     * Returns the standard set of headers to make us look like a browser
     * @return      the standard set of request headers
     */
    static Map<String,String> getRequestHeaders() {
        return Collect.asMap(map -> {
            map.put("User-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36");
            map.put("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8");
            map.put("Referer", "https://www.google.com/");
            map.put("Accept-encoding", "gzip, deflate");
            map.put("Accept-language", "en-US,en;q=0.8");
            map.put("Host", "request.urih.com");
            map.put("Cache-control", "max-age=259200");
            map.put("Connection", "keep-alive");
        });
    }

}
