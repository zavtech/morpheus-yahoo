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

import java.net.URL;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.TextStreamReader;
import com.zavtech.morpheus.util.http.HttpClient;

/**
 * A DataFrameSource implementation that loads historical quote data from Yahoo Finance using their CSV API.
 *
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooQuoteHistorySource implements DataFrameSource<LocalDate,YahooField,YahooQuoteHistoryOptions> {

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


    @Override
    public <T extends Options<?,?>> boolean isSupported(T options) {
        return options instanceof YahooQuoteHistoryOptions;
    }

    @Override
    public DataFrame<LocalDate, YahooField> read(YahooQuoteHistoryOptions options) throws DataFrameException {
        try {
            final URL url = createURL(Options.validate(options).getTicker(), options.getStart(),options.getEnd());
            return HttpClient.getDefault().<DataFrame<LocalDate,YahooField>>doGet(httpRequest -> {
                httpRequest.setUrl(url);
                httpRequest.setRetryCount(2);
                httpRequest.setReadTimeout(5000);
                httpRequest.setConnectTimeout(2000);
                httpRequest.setResponseHandler((status, stream) -> {
                    final TextStreamReader reader = new TextStreamReader(stream);
                    if (reader.hasNext()) reader.nextLine(); //Swallow the header
                    final Index<LocalDate> rowKeys = Index.of(LocalDate.class, 1000);
                    final Index<YahooField> colKeys = Index.of(fields);
                    final DataFrame<LocalDate,YahooField> frame = DataFrame.ofDoubles(rowKeys, colKeys);
                    while (reader.hasNext()) {
                        final String line = reader.nextLine();
                        final String[] elements = line.split(",");
                        final LocalDate date = parseDate(elements[0]);
                        final double open = Double.parseDouble(elements[1]);
                        final double high = Double.parseDouble(elements[2]);
                        final double low = Double.parseDouble(elements[3]);
                        final double close = Double.parseDouble(elements[4]);
                        final double volume = Double.parseDouble(elements[5]);
                        final double closeAdj = Double.parseDouble(elements[6]);
                        final double splitRatio = Math.abs(closeAdj - close) > 0.00001d ? closeAdj / close : 1d;
                        final double adjustment = options.isAdjustForSplits() ? splitRatio : 1d;
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
                    calculateChanges(frame);
                    return Optional.of(frame);
                });
            }).orElseGet(() -> {
                throw new RuntimeException("Failed to load DataFrame from URL: " + url);
            });
        } catch (Exception ex) {
            throw new DataFrameException("Market Data query failed for asset " +  options.getTicker(), ex);
        }
    }


    /**
     * Calculates price changes from close to close
     * @param frame the quote frame
     * @return      the same as input
     */
    private DataFrame<LocalDate,YahooField> calculateChanges(DataFrame<LocalDate,YahooField> frame) {
        frame.rows().sort((row1, row2) -> row1.key().compareTo(row2.key()));
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
     * http://ichart.finance.yahoo.com/table.csv?s=GOOG&d=9&e=15&f=2013&g=d&a=7&b=19&c=2004&ignore=.csv
     * http://real-chart.finance.yahoo.com/table.csv?s=AAPL&
     * https://query1.finance.yahoo.com/v7/finance/download/BLK?period1=1492810126&period2=1495402126&interval=1d&events=history&crumb=jValG4P3fLj
     * @param symbol    the asset symbol
     * @param start     the start date
     * @param end       the end date
     * @return          the query url
     * @throws Exception    if url construction fails
     */
    private URL createURL(String symbol, LocalDate start, LocalDate end) throws Exception {
        final String url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%d&period2=%d&interval=1d&events=history&crumb=jValG4P3fLj";
        final long startDate = start.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        final long endDate = end.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        return new URL(String.format(url, symbol, startDate, endDate));
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


    public static void main(String[] args) {
        final LocalDate start = LocalDate.of(2010, 1, 1);
        final LocalDate end = LocalDate.of(2012, 1, 1);
        final Array<String> tickers = Array.of("AAPL", "MSFT", "ORCL", "GE", "C");
        DataFrame.read().register(new YahooQuoteHistorySource());
        tickers.forEach(ticker -> {
            System.out.println("");
            System.out.printf("\nLoading quotes for %s", ticker);
            final DataFrame<LocalDate,YahooField> frame = DataFrame.read().apply(YahooQuoteHistoryOptions.class, options -> {
                options.setTicker(ticker);
                options.setStart(start);
                options.setEnd(end);
                options.setAdjustForSplits(true);
            });
            frame.out().print();
        });
    }
}
