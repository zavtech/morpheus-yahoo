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

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.http.HttpClient;

/**
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooOptionSource extends DataFrameSource<String,YahooField,YahooOptionSource.Options> {

    private String baseUrl;
    private String urlTemplate;
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

    /**
     * Constructor
     */
    public YahooOptionSource() {
        this("https://finance.yahoo.com/quote/TICKER/options?p=TICKER&date=EXPIRY");
    }

    /**
     * Constructor
     * @param urlTemplate   the URL template for this source
     */
    public YahooOptionSource(String urlTemplate) {
        this.urlTemplate = urlTemplate;
        this.baseUrl = urlTemplate.substring(0, urlTemplate.indexOf('/', 9));
    }

    @Override
    public DataFrame<String, YahooField> read(Consumer<Options> configurator) throws DataFrameException {
        final Options options = initOptions(new Options(), configurator);
        try {
            final DataFrame<String,YahooField> frame = createFrame(1000, fields);
            final Set<LocalDate> expiryDates = options.getExpiry().map(Collections::singleton).orElse(getExpiryDates(options.underlying));
            expiryDates.stream().parallel().forEach(expiry -> {
                try {
                    final long epochMillis = ZonedDateTime.of(expiry, LocalTime.of(0,0,0,0), ZoneId.of("GMT")).toEpochSecond();
                    final String url = urlTemplate.replace("TICKER", options.underlying).replace("EXPIRY", String.valueOf(epochMillis));
                    HttpClient.getDefault().doGet(httpRequest -> {
                        httpRequest.setUrl(url);
                        httpRequest.setConnectTimeout(5000);
                        httpRequest.setRetryCount(3);
                        httpRequest.setResponseHandler(response -> {
                            try {
                                System.out.println("Loading " + httpRequest.getUrl());
                                final InputStream stream = response.getStream();
                                final Document document = Jsoup.parse(stream, "UTF-8", "http://finance.yahoo.com");
                                final Element calls = document.select("table.calls").first();
                                final Element puts = document.select("table.puts").first();
                                this.addOptions(options.underlying, expiry, calls, "CALL", frame);
                                this.addOptions(options.underlying, expiry, puts, "PUT", frame);
                                return Optional.empty();
                            } catch (IOException ex) {
                                throw new RuntimeException("Failed to load DataFrame from URL: " + url, ex);
                            }
                        });
                    });
                } catch (Exception ex) {
                    throw new DataFrameException("Failed to load option quote data from Yahoo Finance for: " + options.underlying, ex);
                }
            });
            return frame.rows().sort(true, Collect.asList(YahooField.OPTION_TYPE, YahooField.EXPIRY_DATE, YahooField.PX_STRIKE));
        } catch (Exception ex) {
            throw new DataFrameException("Failed to load option quotes for " + options.underlying, ex);
        }
    }

    /**
     * Returns a newly created DataFrame to capture option results
     * @param rowCount  the row count
     * @param fields    the column keys
     * @return          the newly created frame
     */
    private DataFrame<String,YahooField> createFrame(int rowCount, Array<YahooField> fields) {
        final Index<String> rowIndex = Index.of(String.class, rowCount);
        return DataFrame.of(rowIndex, YahooField.class, columns -> {
            fields.forEach(colKey -> {
                final Class<?> dataType = colKey.getDataType();
                final Array<?> array = Array.of(dataType, rowIndex.size());
                columns.add(colKey, array);
            });
        });
    }


    /**
     * Add options as per the HTML table element specified
     * @param ticker        the underlying ticker
     * @param expiry        the expiry date
     * @param table         the HTML table element
     * @param optionType    the option type
     * @param frame         the data frame to populate
     */
    private void addOptions(String ticker, LocalDate expiry, Element table, String optionType, DataFrame<String,YahooField> frame) {
        try {
            synchronized (frame) {
                final YahooFinanceParser format = new YahooFinanceParser();
                final Elements body = table.select("tbody");
                body.select("tr").forEach(row -> {
                    final String symbol = extractValue(row.select("td.data-col0").first());
                    final double strike = format.parseDouble(extractValue(row.select("td.data-col2").first()));
                    final double lastPrice = format.parseDouble(extractValue(row.select("td.data-col3").first()));
                    final double bidPrice = format.parseDouble(extractValue(row.select("td.data-col4").first()));
                    final double askPrice = format.parseDouble(extractValue(row.select("td.data-col5").first()));
                    final double change = format.parseDouble(extractValue(row.select("td.data-col6").first()));
                    final double changePercent = format.parseDouble(extractValue(row.select("td.data-col7").first()));
                    final double volume = format.parseDouble(extractValue(row.select("td.data-col8").first()));
                    final double openInt = format.parseDouble(extractValue(row.select("td.data-col9").first()));
                    final double impliedVol = format.parseDouble(extractValue(row.select("td.data-col10").first()));
                    if (frame.rows().add(symbol)) {
                        final int rowOrdinal = frame.rowCount()-1;
                        frame.data().setValue(rowOrdinal, YahooField.OPTION_TYPE, optionType);
                        frame.data().setDouble(rowOrdinal, YahooField.PX_STRIKE, strike);
                        frame.data().setValue(rowOrdinal, YahooField.TICKER, ticker);
                        frame.data().setValue(rowOrdinal, YahooField.EXPIRY_DATE, expiry);
                        frame.data().setDouble(rowOrdinal, YahooField.PX_LAST, lastPrice);
                        frame.data().setDouble(rowOrdinal, YahooField.PX_BID, bidPrice);
                        frame.data().setDouble(rowOrdinal, YahooField.PX_ASK, askPrice);
                        frame.data().setDouble(rowOrdinal, YahooField.PX_CHANGE, change);
                        frame.data().setDouble(rowOrdinal, YahooField.PX_CHANGE_PERCENT, changePercent);
                        frame.data().setDouble(rowOrdinal, YahooField.PX_VOLUME, volume);
                        frame.data().setDouble(rowOrdinal, YahooField.OPEN_INTEREST, openInt);
                        frame.data().setDouble(rowOrdinal, YahooField.IMPLIED_VOLATILITY, impliedVol);
                    }
                });
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to parse option data from Yahoo Finance", ex);
        }
    }


    /**
     * Method that extracts the ultimate value from a table cell
     * @param td    the cell reference
     * @return      the extracted value
     */
    private String extractValue(Element td) {
        if (td == null) {
            return null;
        } else {
            final String text = td.text();
            if (text != null) {
                return text;
            }
            final Elements children = td.children();
            for (Element child : children) {
                if (child.tagName().equalsIgnoreCase("a")) {
                    return child.text();
                } else if (child.tagName().equalsIgnoreCase("span")) {
                    return child.text();
                }
            }
            return null;
        }
    }


    /**
     * Returns the list of expiry dates for the underlying security ticker
     * @param ticker    the underlying security ticker
     * @return          the list of expiry dates
     */
    public Set<LocalDate> getExpiryDates(String ticker) {
        try {
            final String url = urlTemplate.replace("TICKER", ticker).replace("&date=EXPIRY", "");
            return HttpClient.getDefault().<Set<LocalDate>>doGet(httpRequest -> {
                httpRequest.setUrl(url);
                httpRequest.setConnectTimeout(5000);
                httpRequest.setReadTimeout(10000);
                httpRequest.setResponseHandler(response -> {
                    try {
                        final InputStream stream = response.getStream();
                        final Document document = Jsoup.parse(stream, "UTF-8", baseUrl);
                        final Element div = document.select("div.option-contract-control").first();
                        final Elements elements = div.select("select").first().select("option");
                        final Set<LocalDate> expiryDates = new TreeSet<>();
                        elements.forEach(element -> {
                            final String value = element.attr("value");
                            if (value != null) {
                                final long epochSeconds = Long.parseLong(value);
                                final Instant instant = Instant.ofEpochSecond(epochSeconds);
                                final LocalDate expiryDate = LocalDateTime.ofInstant(instant, ZoneId.of("GMT")).toLocalDate();
                                expiryDates.add(expiryDate);
                            }
                        });
                        return Optional.of(expiryDates);
                    } catch (IOException ex) {
                        throw new RuntimeException("Failed to load DataFrame from URL: " + url, ex);
                    }
                });
            }).orElseGet(() -> {
                throw new RuntimeException("No DataFrame produced by http request to " + url);
            });
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load option expiry dates for: " + ticker, ex);
        }
    }


    /**
     * The options for this source
     */
    public class Options implements DataFrameSource.Options<String,YahooField> {

        private String underlying;
        private LocalDate expiry;

        @Override
        public void validate() {
            Asserts.notNull(underlying, "The underlying ticker cannot be null");
        }

        /**
         * Sets the ticker of the underlying security
         * @param underlying    the underlying instrument ticker
         * @return              these options
         */
        public Options withUnderlying(String underlying) {
            this.underlying = underlying;
            return this;
        }

        /**
         * Sets a specific expiry, can be null to select all expiry dates
         * @param expiry    the optional expiry date, null for all dates
         * @return          these options
         */
        public Options withExpiry(LocalDate expiry) {
            this.expiry = expiry;
            return this;
        }

        /**
         * Returns the optional expiry date
         * @return  optional expiry
         */
        Optional<LocalDate> getExpiry() {
            return Optional.ofNullable(expiry);
        }
    }


    public static void main(String[] args) {
        final YahooOptionSource source = new YahooOptionSource();
        final Array<String> tickers = Array.of("AAPL");
        tickers.forEach(ticker -> {
            final long t1 = System.currentTimeMillis();
            final DataFrame<String,YahooField> frame = source.read(options -> {
                options.withUnderlying(ticker);
                options.withExpiry(LocalDate.of(2017, 10, 20));
            });
            final long t2 = System.currentTimeMillis();
            System.out.println("Extracted table: " + frame + " in " + (t2 - t1) + " millis");
            frame.out().print();
        });
    }

}
