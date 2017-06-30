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

import java.io.IOException;
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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.http.HttpClient;

/**
 *
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooOptionQuoteSource implements DataFrameSource<String,YahooField,YahooOptionQuoteOptions> {

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
    public YahooOptionQuoteSource() {
        this("http://finance.yahoo.com/q/op?s=TICKER&date=EXPIRY");
    }

    /**
     * Constructor
     * @param urlTemplate   the URL template for this source
     */
    public YahooOptionQuoteSource(String urlTemplate) {
        this.urlTemplate = urlTemplate;
        this.baseUrl = urlTemplate.substring(0, urlTemplate.indexOf('/', 9));
    }

    @Override
    public <T extends Options<?,?>> boolean isSupported(T options) {
        return options instanceof YahooOptionQuoteOptions;
    }

    @Override
    public DataFrame<String,YahooField> read(YahooOptionQuoteOptions options) throws DataFrameException {
        try {
            final DataFrame<String,YahooField> frame = createFrame(1000, fields);
            final Set<LocalDate> expiryDates = Options.validate(options).getExpiry().map(Collections::singleton).orElse(getExpiryDates(options.getTicker()));
            expiryDates.forEach(expiry -> {
                try {
                    final long epochMillis = ZonedDateTime.of(expiry, LocalTime.of(0,0,0,0), ZoneId.of("GMT")).toEpochSecond();
                    final String url = urlTemplate.replace("TICKER", options.getTicker()).replace("EXPIRY", String.valueOf(epochMillis));
                    HttpClient.getDefault().doGet(httpRequest -> {
                        httpRequest.setUrl(url);
                        httpRequest.setConnectTimeout(3000);
                        httpRequest.setRetryCount(3);
                        httpRequest.setResponseHandler((status, stream) -> {
                            try {
                                System.out.println("Loading " + httpRequest);
                                final Document document = Jsoup.parse(stream, "UTF-8", "http://finance.yahoo.com");
                                final Element calls = document.select("#optionsCallsTable").first().select("table.quote-table").first();
                                final Element puts = document.select("#optionsPutsTable").first().select("table.quote-table").first();
                                this.addOptions(options.getTicker(), expiry, calls, "CALL", frame);
                                this.addOptions(options.getTicker(), expiry, puts, "PUT", frame);
                                return Optional.empty();
                            } catch (IOException ex) {
                                throw new RuntimeException("Failed to load DataFrame from URL: " + url, ex);
                            }
                        });
                    });
                } catch (Exception ex) {
                    throw new DataFrameException("Failed to load option quote data from Yahoo Finance for: " + options.getTicker(), ex);
                }
            });
            return frame;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to load option quotes for " + options.getTicker(), ex);
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
            final YahooFinanceParser format = new YahooFinanceParser();
            final Elements body = table.select("tbody");
            body.select("tr").forEach(row -> {
                final Elements cells = row.select("td");
                if (cells.size() == 10) {
                    final double strike = format.parseDouble(cells.get(0).select("a").text());
                    final String symbol = cells.get(1).select("a").text();
                    final double lastPrice = format.parseDouble(cells.get(2).select("div").text());
                    final double bidPrice = format.parseDouble(cells.get(3).select("div").text());
                    final double askPrice = format.parseDouble(cells.get(4).select("div").text());
                    final double change = format.parseDouble(cells.get(5).select("div").text());
                    final double changePercent = format.parseDouble(cells.get(6).select("div").text());
                    final double volume = format.parseDouble(cells.get(7).select("strong").text());
                    final double openInt = format.parseDouble(cells.get(8).select("div").text());
                    final double impliedVol = format.parseDouble(cells.get(9).select("div").text());
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
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException("Failed to parse option data from Yahoo Finance", ex);
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
                httpRequest.setResponseHandler((status, stream) -> {
                    try {
                        final Document document = Jsoup.parse(stream, "UTF-8", baseUrl);
                        final Elements elements = document.select("select.Start-0").first().select("option");
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


    public static void main(String[] args) {
        DataFrame.read().register(new YahooOptionQuoteSource());
        final Array<String> tickers = Array.of("AAPL");
        tickers.forEach(ticker -> {
            final long t1 = System.currentTimeMillis();
            final DataFrame<String,YahooField> frame = DataFrame.read().apply(new YahooOptionQuoteOptions(ticker, null));
            final long t2 = System.currentTimeMillis();
            System.out.println("Extracted table: " + frame + " in " + (t2 - t1) + " millis");
            frame.out().print();
        });
    }

}
