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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.http.HttpClient;

/**
 * Class summary goes here...
 *
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooQuoteLiveSource extends DataFrameSource<String,YahooField,YahooQuoteLiveSource.Options> {

    private static final Map<YahooField,String> codeMap = new LinkedHashMap<>();

    private String urlTemplate;

    /**
     * Static initializer
     */
    static {
        codeMap.put(YahooField.TICKER, "s");
        codeMap.put(YahooField.NAME, "n");
        codeMap.put(YahooField.PX_BID, "b");
        codeMap.put(YahooField.PX_BID_SIZE, "b6");
        codeMap.put(YahooField.PX_ASK, "a");
        codeMap.put(YahooField.PX_ASK_SIZE, "a5");
        codeMap.put(YahooField.PX_VOLUME, "v");
        codeMap.put(YahooField.PX_CHANGE, "c1");
        codeMap.put(YahooField.PX_CHANGE_PERCENT, "p2");
        codeMap.put(YahooField.PX_LAST_DATE, "d1");
        codeMap.put(YahooField.PX_LAST_TIME, "t1");
        codeMap.put(YahooField.PX_LAST, "l1");
        codeMap.put(YahooField.PX_LAST_SIZE, "k3");
        codeMap.put(YahooField.PX_LOW, "g");
        codeMap.put(YahooField.PX_HIGH, "h");
        codeMap.put(YahooField.PX_PREVIOUS_CLOSE, "p");
        codeMap.put(YahooField.PX_OPEN, "o");
        codeMap.put(YahooField.EXCHANGE, "x");
        codeMap.put(YahooField.AVG_DAILY_VOLUME, "a2");
        codeMap.put(YahooField.TRADE_DATE, "d2");
        codeMap.put(YahooField.DIVIDEND_PER_SHARE, "d");
        codeMap.put(YahooField.EPS, "e");
        codeMap.put(YahooField.EPS_ESTIMATE, "e7");
        codeMap.put(YahooField.EPS_NEXT_YEAR, "e8");
        codeMap.put(YahooField.EPS_NEXT_QUARTER, "e9");
        codeMap.put(YahooField.FLOAT_SHARES, "f6");
        codeMap.put(YahooField.FIFTY_TWO_WEEK_LOW, "j");
        codeMap.put(YahooField.ANNUALISED_GAIN, "g3");
        codeMap.put(YahooField.MARKET_CAP, "j1");
        codeMap.put(YahooField.EBITDA, "j4");
        codeMap.put(YahooField.PRICE_SALES_RATIO, "p5");
        codeMap.put(YahooField.PRICE_BOOK_RATIO, "p6");
        codeMap.put(YahooField.EX_DIVIDEND_DATE, "q");
        codeMap.put(YahooField.PRICE_EARNINGS_RATIO, "r");
        codeMap.put(YahooField.DIVIDEND_PAY_DATE, "r1");
        codeMap.put(YahooField.PEG_RATIO, "r5");
        codeMap.put(YahooField.PRICE_EPS_RATIO_CURRENT_YEAR, "r6");
        codeMap.put(YahooField.PRICE_EPS_RATIO_NEXT_YEAR, "r7");
        codeMap.put(YahooField.SHORT_RATIO, "s7");
    }



    /**
     * Constructor
     */
    public YahooQuoteLiveSource() {
        this("http://download.finance.yahoo.com/d/quotes.csv?");
    }

    /**
     * Constructor
     * @param urlTemplate   the URL template
     */
    public YahooQuoteLiveSource(String urlTemplate) {
        this.urlTemplate = urlTemplate;
    }

    /**
     * Retruns the set of fields supported by this source
     * @return  the fields supported by this source
     */
    public Set<YahooField> getFieldSet() {
        return Collections.unmodifiableSet(codeMap.keySet());
    }


    @Override
    public DataFrame<String,YahooField> read(Consumer<Options> configurator) throws DataFrameException {
        try {
            final StringBuilder url = new StringBuilder(urlTemplate);
            final Options options = initOptions(new Options(), configurator);
            final Map<String,String> tickerMap = appendTickers(url, options);
            final List<YahooField> fieldList = appendFields(url, options);
            final URL queryUrl = new URL(url.toString());
            return HttpClient.getDefault().<DataFrame<String,YahooField>>doGet(httpRequest -> {
                httpRequest.setUrl(queryUrl);
                httpRequest.getHeaders().putAll(YahooFinance.getRequestHeaders());
                httpRequest.setResponseHandler(response -> {
                    final InputStream stream = response.getStream();
                    final BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                    final DataFrame<String,YahooField> frame = createFrame(tickerMap.keySet(), fieldList);
                    final YahooContentProcessor processor = new YahooContentProcessor(fieldList, frame);
                    final CsvParserSettings settings = createSettings(processor);
                    final CsvParser parser = new CsvParser(settings);
                    parser.parse(reader);
                    return Optional.of(frame);
                });
            }).orElseGet(() -> {
                throw new RuntimeException("No DataFrame loaded for query URL: " + queryUrl);
            });
        } catch (Exception ex) {
            throw new DataFrameException("Failed to read quotes from Yahoo Finance: " + ex.getMessage(), ex);
        }
    }

    /**
     * Returns a newly created DataFrame for tickers and fields
     * @param tickers   the tickers for frame
     * @param fields    the fields for frame
     * @return          the newly created DataFrame
     */
    private DataFrame<String,YahooField> createFrame(Collection<String> tickers, Collection<YahooField> fields) {
        return DataFrame.of(tickers, YahooField.class, columns -> {
            fields.forEach(field -> {
                final Class<?> dataType = field.getDataType();
                final Array<?> array = Array.of(dataType, tickers.size());
                columns.add(field, array);
            });
        });
    }

    /**
     * Returns newly created parser settings
     * @param processor the row processor for parsing
     * @return          the newly created settings
     */
    private CsvParserSettings createSettings(RowProcessor processor) {
        final CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(',');
        settings.setHeaderExtractionEnabled(false);
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setRowProcessor(processor);
        return settings;
    }

    /**
     * Appends a sequence of tickers to the url
     * @param url       the url to append to
     * @param request   the request descriptor
     * @return          the apply of captured tickers
     */
    private Map<String,String> appendTickers(StringBuilder url, Options request) {
        url.append("s=");
        final Set<String> tickers = request.tickers;
        final Map<String,String> tickerMap = new LinkedHashMap<>(tickers.size());
        final int length = url.length();
        tickers.forEach(ticker -> {
            url.append(url.length() > length ? "+" : "");
            if (isFxRate(ticker)) {
                url.append(ticker);
                url.append("=X");
                tickerMap.put(ticker + "=X", ticker);
            }
            else {
                url.append(ticker);
                tickerMap.put(ticker, ticker);
            }
        });
        return tickerMap;
    }

    /**
     * Appends a sequence of fields to the url
     * @param url       the url to append to
     * @param request   the request descriptor
     * @return          the list of fields
     */
    private List<YahooField> appendFields(StringBuilder url, Options request) {
        url.append("&f=s");
        final Set<YahooField> fields = request.fields.size() > 0 ? request.fields : codeMap.keySet();
        final List<YahooField> fieldList = new ArrayList<>(fields.size());
        fields.forEach(field -> {
            final String code = codeMap.getOrDefault(field, null);
            if (code == null) {
                throw new DataFrameException("Quote field not supported for live queries: " + field);
            } else {
                url.append(code);
                fieldList.add(field);
            }
        });
        return fieldList;
    }

    /**
     * Returns true if the ticker represents an fx rate
     * @param ticker    the ticker
     * @return          true if matches an fx rate
     */
    private boolean isFxRate(String ticker) {
        try {
            if (ticker != null && ticker.length() == 6) {
                final String base = ticker.substring(0, 3);
                final String quote = ticker.substring(3, 6);
                final Currency ccy1 = Currency.getInstance(base);
                final Currency ccy2 = Currency.getInstance(quote);
                return ccy1 != null && ccy2 != null;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    /**
     * The CSV row processor
     */
    private class YahooContentProcessor implements RowProcessor {

        private List<YahooField> fieldList;
        private DataFrame<String,YahooField> frame;
        private YahooFinanceParser parser = new YahooFinanceParser();

        /**
         * Constructor
         * @param fieldList the query field list
         * @param frame     the frame to populate
         */
        private YahooContentProcessor(List<YahooField> fieldList, DataFrame<String,YahooField> frame) {
            this.fieldList = fieldList;
            this.frame = frame;
        }

        @Override
        public void processStarted(ParsingContext context) {

        }

        @Override
        public void rowProcessed(String[] row, ParsingContext context) {
            YahooField field = null;
            String text = null;
            try {
                final String ticker = row[0];
                for (int i=1; i<row.length; ++i) {
                    field = fieldList.get(i - 1);
                    text = row[i];
                    final Object value = parser.parse(text);
                    frame.data().setValue(ticker, field, value);
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to parse line: " + context.currentLine() + ", " +field + "=" + text , ex);
            }
        }

        @Override
        public void processEnded(ParsingContext context) {

        }
    }


    /**
     * The options for this DataFrameSource
     */
    public class Options implements DataFrameSource.Options<String,YahooField> {

        private Set<String> tickers = new LinkedHashSet<>();
        private Set<YahooField> fields = new LinkedHashSet<>();


        @Override
        public void validate() {
            Asserts.check(tickers.size() > 0, "At least one ticker must be specified");
        }

        /**
         * Sets the tickers for these options
         * @param tickers   the tickers
         */
        public Options withTickers(Iterable<String> tickers) {
            this.tickers.addAll(Collect.asList(tickers));
            return this;
        }

        /**
         * Sets the fields for these options
         * @param fields    the fields
         */
        public Options withFields(Iterable<YahooField> fields) {
            this.fields.addAll(Collect.asList(fields));
            return this;
        }

        /**
         * Sets the tickers for these options
         * @param tickers   the tickers
         */
        public Options withTickers(String... tickers) {
            this.tickers.addAll(Arrays.asList(tickers));
            return this;
        }

        /**
         * Sets the fields for these options
         * @param fields    the fields
         */
        public Options withFields(YahooField... fields) {
            this.fields.addAll(Arrays.asList(fields));
            return this;
        }

    }


    public static void main(String[] args) {
        final Set<String> tickers = new HashSet<>(Arrays.asList("GBPUSD", "MSFT", "GOOG", "GOOGL", "ORCL", "FDX", "NFLX", "XXX", "GE", "GS", "ML", "UBS", "BLK"));
        final YahooQuoteLiveSource source = new YahooQuoteLiveSource();
        final DataFrame<String,YahooField> frame = source.read(o -> o.withTickers(tickers));
        frame.out().print();
    }

}
