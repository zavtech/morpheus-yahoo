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
package com.zavtech.morpheus.yahoo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.util.TextStreamReader;
import com.zavtech.morpheus.util.http.HttpClient;
import com.zavtech.morpheus.util.http.HttpException;
import com.zavtech.morpheus.util.http.HttpHeader;

/**
 * A DataFrameSource implementation that loads historical quote data from Yahoo Finance using their CSV API.
 *
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooQuoteHistorySource extends DataFrameSource<LocalDate,YahooField,YahooQuoteHistorySource.Options> {

    private static final String CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb";
    private static final String COOKIE_URL = "https://finance.yahoo.com/quote/SPY?p=SPY";
    private static final String QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%d&period2=%d&interval=1d&events=history&crumb=%s";

    private static Predicate<LocalDate> weekdayPredicate = date -> {
        if (date == null) {
            return false;
        } else {
            switch (date.getDayOfWeek()) {
                case SATURDAY:  return false;
                case SUNDAY:    return false;
                default:        return true;
            }
        }
    };

    private static Array<YahooField> fields = Array.of(
        YahooField.PX_OPEN,
        YahooField.PX_HIGH,
        YahooField.PX_LOW,
        YahooField.PX_CLOSE,
        YahooField.PX_VOLUME,
        YahooField.PX_SPLIT_RATIO,
        YahooField.PX_CHANGE,
        YahooField.PX_CHANGE_PERCENT
    );

    private String crumb;
    private Duration connectTimeout;
    private Duration readTimeout;
    private Map<String,String> cookies;


    /**
     * Constructor
     */
    public YahooQuoteHistorySource() {
        this(Duration.ofMillis(5000), Duration.ofMillis(15000));
    }

    /**
     * Constructor
     * @param connectTimeout    the http connect timeout
     * @param readTimeout       the http read timeout
     */
    public YahooQuoteHistorySource(Duration connectTimeout, Duration readTimeout) {
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.cookies = new HashMap<>();
    }


    @Override
    public DataFrame<LocalDate,YahooField> read(Consumer<Options> configurator) throws DataFrameException {
        final Options options = initOptions(new Options(), configurator);
        try {
            final URL url = createURL(options.ticker, options.startDate,options.endDate);
            IO.println("Calling " + url);
            return HttpClient.getDefault().<DataFrame<LocalDate,YahooField>>doGet(httpRequest -> {
                httpRequest.setUrl(url);
                httpRequest.setRetryCount(2);
                httpRequest.setReadTimeout((int)readTimeout.getSeconds() * 1000);
                httpRequest.setConnectTimeout((int)connectTimeout.getSeconds() * 1000);
                httpRequest.getCookies().putAll(getCookies());
                httpRequest.setResponseHandler(response -> {
                    if (response.getStatus().getCode() != 200) {
                        final int code = response.getStatus().getCode();
                        throw new HttpException(httpRequest, "Yahoo Finance responded with status code " + code, null);
                    } else {
                        final InputStream stream = response.getStream();
                        final TextStreamReader reader = new TextStreamReader(stream);
                        if (reader.hasNext()) reader.nextLine(); //Swallow the header
                        final Index<LocalDate> rowKeys = createDateIndex(options);
                        final Index<YahooField> colKeys = Index.of(fields.copy());
                        final DataFrame<LocalDate,YahooField> frame = DataFrame.ofDoubles(rowKeys, colKeys);
                        final DataFrameCursor<LocalDate,YahooField> cursor = frame.cursor();
                        while (reader.hasNext()) {
                            final String line = reader.nextLine();
                            final String[] elements = line.split(",");
                            final LocalDate date = parseDate(elements[0]);
                            final double open = Double.parseDouble(elements[1]);
                            final double high = Double.parseDouble(elements[2]);
                            final double low = Double.parseDouble(elements[3]);
                            final double close = Double.parseDouble(elements[4]);
                            final double closeAdj = Double.parseDouble(elements[5]);
                            final double volume = Double.parseDouble(elements[6]);
                            final double splitRatio = Math.abs(closeAdj - close) > 0.00001d ? closeAdj / close : 1d;
                            final double adjustment = options.dividendAdjusted ? splitRatio : 1d;
                            if (options.paddedHolidays) {
                                cursor.atRowKey(date);
                                cursor.atColOrdinal(0).setDouble(open * adjustment);
                                cursor.atColOrdinal(1).setDouble(high * adjustment);
                                cursor.atColOrdinal(2).setDouble(low * adjustment);
                                cursor.atColOrdinal(3).setDouble(close * adjustment);
                                cursor.atColOrdinal(4).setDouble(volume);
                                cursor.atColOrdinal(5).setDouble(splitRatio);
                            } else {
                                frame.rows().add(date, v -> {
                                    switch (v.colOrdinal()) {
                                        case 0: return open * adjustment;
                                        case 1: return high * adjustment;
                                        case 2: return low * adjustment;
                                        case 3: return close * adjustment;
                                        case 4: return volume;
                                        case 5: return splitRatio;
                                        default: return v.getDouble();
                                    }
                                });
                            }
                        }
                        if (options.paddedHolidays) {
                            frame.fill().down(2);
                        }
                        calculateChanges(frame);
                        return Optional.of(frame);
                    }
                });
            }).orElseGet(() -> {
                throw new RuntimeException("Failed to load DataFrame from URL: " + url);
            });
        } catch (Exception ex) {
            throw new DataFrameException("Market Data query failed for asset " +  options.ticker, ex);
        }
    }


    /**
     * Returns the date index to initialize the row axis
     * @param options   the options for the request
     * @return          the date index for result DataFrame
     */
    private Index<LocalDate> createDateIndex(Options options) {
        final Range<LocalDate> range = Range.of(options.startDate, options.endDate);
        if (options.paddedHolidays) {
            return Index.of(range.filter(weekdayPredicate).toArray());
        } else {
            final int size = (int)range.estimateSize() + 10;
            return Index.of(LocalDate.class, size);
        }
    }


    /**
     * Calculates price changes from close to close
     * @param frame the quote frame
     * @return      the same as input
     */
    private DataFrame<LocalDate,YahooField> calculateChanges(DataFrame<LocalDate,YahooField> frame) {
        frame.rows().sort(true);
        IntStream.range(1, frame.rowCount()).forEach(rowIndex -> {
            final double previous = frame.data().getDouble(rowIndex - 1, YahooField.PX_CLOSE);
            final double current = frame.data().getDouble(rowIndex, YahooField.PX_CLOSE);
            frame.data().setDouble(rowIndex, YahooField.PX_CHANGE, current - previous);
            frame.data().setDouble(rowIndex, YahooField.PX_CHANGE_PERCENT, (current / previous) - 1d);
        });
        return frame;
    }

    /**
     * Called internally to construct the query URL.
     * @param symbol    the asset symbol
     * @param start     the start date
     * @param end       the end date
     * @return          the query url
     * @throws Exception    if url construction fails
     */
    private URL createURL(String symbol, LocalDate start, LocalDate end) throws Exception {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("The start date must be after the end date");
        } else {
            final String crumb = getCrumb();
            final long startDate = start.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
            final long endDate = end.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
            return new URL(String.format(QUOTE_URL, symbol, startDate, endDate, crumb));
        }
    }

    /**
     * Parses dates in the formatSqlDate YYYY-MM-DD
     * @param dateString    the string to parse
     * @return  the parsed date value
     */
    private LocalDate parseDate(String dateString) {
        if (dateString == null) {
            return null;
        } else {
            final String[] elements = dateString.trim().split("-");
            final int year = Integer.parseInt(elements[0]);
            final int month = Integer.parseInt(elements[1]);
            final int date = Integer.parseInt(elements[2]);
            return LocalDate.of(year, month, date);
        }
    }


    /**
     * Returns the cookies to send with the request
     * @return      the cookies to send with request
     */
    private synchronized Map<String,String> getCookies() {
        if (cookies.isEmpty()) {
            this.cookies = HttpClient.getDefault().<Map<String,String>>doGet(httpRequest -> {
                httpRequest.setUrl(COOKIE_URL);
                httpRequest.setReadTimeout((int)readTimeout.getSeconds() * 1000);
                httpRequest.setConnectTimeout((int)connectTimeout.getSeconds() * 1000);
                httpRequest.getHeaders().putAll(YahooFinance.getRequestHeaders());
                httpRequest.setResponseHandler(response -> {
                    final List<HttpHeader> headers = response.getHeaders();
                    final Map<String,String> cookies = new HashMap<>();
                    headers.forEach(header -> {
                        if (header.getKey().equalsIgnoreCase("Set-Cookie")) {
                            final String cookieValue = header.getValue();
                            final String[] tokens = cookieValue.split(";");
                            final Matcher matcher = Pattern.compile("(B)=(.+)").matcher("");
                            for (String token : tokens) {
                                if (matcher.reset(token.trim()).matches()) {
                                    final String key = matcher.group(1);
                                    final String value = matcher.group(2);
                                    cookies.put(key, value);
                                }

                            }
                        }
                    });
                    return Optional.of(cookies);
                });
            }).orElseThrow(() -> new YahooException("Failed to capture cookies for historical quote request"));
        }
        return cookies;
    }


    /**
     * Returns the crumb to add to get request
     * @return      the crumb token
     */
    private synchronized String getCrumb() {
        if (crumb == null) {
            final Map<String,String> cookies = getCookies();
            this.crumb = HttpClient.getDefault().<String>doGet(httpRequest -> {
                httpRequest.setUrl(CRUMB_URL);
                httpRequest.setReadTimeout((int)readTimeout.getSeconds() * 1000);
                httpRequest.setConnectTimeout((int)connectTimeout.getSeconds() * 1000);
                httpRequest.getCookies().putAll(cookies);
                httpRequest.setResponseHandler(response -> {
                    try {
                        final String crumb = IO.readText(response.getStream());
                        return Optional.of(crumb.trim());
                    } catch (IOException ex) {
                        throw new HttpException(httpRequest, "Failed to load data from url", ex);
                    }
                });
            }).orElseThrow(() -> new YahooException("Failed to initialize crumb token for historical quote request"));
        }
        return crumb;
    }


    /**
     * The options for this source
     */
    public class Options implements DataFrameSource.Options<LocalDate,YahooField> {

        private String ticker;
        private LocalDate startDate;
        private LocalDate endDate = LocalDate.now();
        private boolean paddedHolidays;
        private boolean dividendAdjusted;

        @Override
        public void validate() {
            Asserts.assertTrue(ticker != null, "The ticker cannot be null");
            Asserts.assertTrue(startDate != null, "The start date cannot be null");
            Asserts.assertTrue(endDate != null, "The end date cannot be null");
            Asserts.assertTrue(startDate.isBefore(endDate), "The start date must be < end date");
        }

        /**
         * Sets the instrument ticker for these options
         * @param ticker    ticker
         */
        public Options withTicker(String ticker) {
            this.ticker = ticker;
            return this;
        }

        /**
         * Sets the start date for options
         * @param startDate the start date
         * @return      these options
         */
        public Options withStartDate(LocalDate startDate) {
            this.startDate = startDate;
            return this;
        }

        /**
         * Sets the end date for options
         * @param endDate   the end date
         * @return          these options
         */
        public Options withEndDate(LocalDate endDate) {
            this.endDate = endDate;
            return this;
        }

        /**
         * Sets the start date for options
         * @param startDate the start date in the form yyyy-MM-dd
         * @return      these options
         */
        public Options withStartDate(String startDate) {
            this.startDate = LocalDate.parse(startDate);
            return this;
        }

        /**
         * Sets the end date for options
         * @param endDate   the end date in the form yyyy-MM-dd
         * @return          these options
         */
        public Options withEndDate(String endDate) {
            this.endDate = LocalDate.parse(endDate);
            return this;
        }

        /**
         * Sets whether to include holidays with quotes padded from prior day
         * @param paddedHolidays    true to include holidays with padded down quotes
         * @return                  these options
         */
        public Options withPaddedHolidays(boolean paddedHolidays) {
            this.paddedHolidays = paddedHolidays;
            return this;
        }

        /**
         * Sets true if prices should be dividends
         * @param dividendAdjusted   true if prices should be adjusted for dividends
         * @return                  these options
         */
        public Options withDividendAdjusted(boolean dividendAdjusted) {
            this.dividendAdjusted = dividendAdjusted;
            return this;
        }
    }



    public static void main(String[] args) {
        final LocalDate start = LocalDate.of(2010, 1, 1);
        final LocalDate end = LocalDate.of(2012, 1, 1);
        final Array<String> tickers = Array.of("AAPL", "MSFT", "ORCL", "GE", "C");
        final YahooQuoteHistorySource source = new YahooQuoteHistorySource();
        tickers.forEach(ticker -> {
            System.out.println("");
            System.out.printf("\nLoading quotes for %s", ticker);
            final DataFrame<LocalDate,YahooField> frame = source.read(options -> {
                options.withTicker(ticker);
                options.withStartDate(start);
                options.withEndDate(end);
                options.withDividendAdjusted(true);
            });
            frame.out().print();
        });
    }
}
