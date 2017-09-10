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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.http.HttpClient;

/**
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooStatsSource extends DataFrameSource<String,YahooField,YahooStatsSource.Options> {

    private static final Array<YahooField> fields = Array.of(
        YahooField.MARKET_CAP,
        YahooField.PE_TRAILING,
        YahooField.PE_FORWARD,
        YahooField.PRICE_SALES_RATIO,
        YahooField.PRICE_BOOK_RATIO,
        YahooField.FISCAL_YEAR_END,
        YahooField.MOST_RECENT_QUARTER,
        YahooField.PROFIT_MARGIN,
        YahooField.OPERATING_MARGIN,
        YahooField.RETURN_ON_ASSETS,
        YahooField.RETURN_ON_EQUITY,
        YahooField.REVENUE_TTM,
        YahooField.REVENUE_PER_SHARE,
        YahooField.REVENUE_GROWTH_QTLY,
        YahooField.GROSS_PROFIT,
        YahooField.EBITDA_TTM,
        YahooField.EPS_DILUTED,
        YahooField.EARNINGS_GRWOTH_QTLY,
        YahooField.BETA,
        YahooField.CASH_MRQ,
        YahooField.CASH_PER_SHARE,
        YahooField.DEBT_MRQ,
        YahooField.DEBT_OVER_EQUITY_MRQ,
        YahooField.CURRENT_RATIO,
        YahooField.BOOK_VALUE_PER_SHARE,
        YahooField.OPERATING_CASH_FLOW,
        YahooField.LEVERED_FREE_CASH_FLOW,
        YahooField.ADV_3MONTH,
        YahooField.ADV_10DAY,
        YahooField.SHARES_OUTSTANDING,
        YahooField.SHARES_FLOAT,
        YahooField.OWNER_PERCENT_INSIDER,
        YahooField.OWNER_PERCENT_INSTITUTION,
        YahooField.SHARES_SHORT,
        YahooField.SHARES_SHORT_RATIO,
        YahooField.SHARES_SHORT_PRIOR,
        YahooField.DIVIDEND_FWD,
        YahooField.DIVIDEND_FWD_YIELD,
        YahooField.DIVIDEND_TRAILING,
        YahooField.DIVIDEND_TRAILING_YIELD,
        YahooField.DIVIDEND_PAYOUT_RATIO,
        YahooField.DIVIDEND_PAY_DATE,
        YahooField.DIVIDEND_EX_DATE,
        YahooField.LAST_SPLIT_DATE
    );

    private String baseUrl;
    private String urlTemplate;

    /**
     * Constructor
     */
    public YahooStatsSource() {
        this("https://finance.yahoo.com/quote/%s/key-statistics?p=%s");
    }

    /**
     * Constructor
     * @param urlTemplate   the URL template on which to replace the security ticker
     */
    public YahooStatsSource(String urlTemplate) {
        this.urlTemplate = urlTemplate;
        this.baseUrl = urlTemplate.substring(0, urlTemplate.indexOf('/', 9));
    }

    /**
     * Returns the fields supported by this source
     * @return  the fields supported by this source
     */
    Array<YahooField> getFields() {
        return fields;
    }

    @Override
    public DataFrame<String, YahooField> read(Consumer<Options> configurator) throws DataFrameException {
        final Options options = initOptions(new Options(), configurator);
        final Set<String> tickers = options.tickers;
        try {
            final DataFrame<String,YahooField> frame = createFrame(tickers);
            tickers.parallelStream().forEach(ticker -> {
                final String url = String.format(urlTemplate, ticker, ticker);
                HttpClient.getDefault().doGet(httpRequest -> {
                    httpRequest.setUrl(url);
                    httpRequest.setResponseHandler(response -> {
                        try {
                            final long t1 = System.currentTimeMillis();
                            final InputStream stream = response.getStream();
                            final Document document = Jsoup.parse(stream, "UTF-8", baseUrl);
                            final long t2 = System.currentTimeMillis();
                            this.captureStatistics(ticker, document, frame);
                            final long t3 = System.currentTimeMillis();
                            System.out.println("Processed " + url + " in " + (t3 - t1) + " millis (" + (t2 - t1) + "+" + (t3 - t2) + " millis)");
                            return Optional.empty();
                        } catch (IOException ex) {
                            throw new RuntimeException("Failed to load content from Yahoo Finance: " + url, ex);
                        }
                    });
                });
            });
            return frame;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to query statistics for one or more tickers: " + tickers, ex);
        }
    }

    /**
     * Returns the DataFrame to capture results for request
     * @param tickers   the tickers
     * @return          the DataFrame result
     */
    private DataFrame<String,YahooField> createFrame(Set<String> tickers) {
        return DataFrame.of(tickers, YahooField.class, columns -> {
            fields.forEach(field -> {
                final Class<?> dataType = field.getDataType();
                final Array<?> array = Array.of(dataType, tickers.size());
                columns.add(field, array);
            });
        });
    }

    /**
     * Captures valuation stats from the document
     * @param ticker    the asset ticker
     * @param document  the document
     * @param frame    the result matrix
     */
    private void captureStatistics(String ticker, Document document, DataFrame<String,YahooField> frame) {
        try {
            final YahooFinanceParser parser = new YahooFinanceParser();
            final Map<String,String> attributeMap = parseAttributes(document);
            attributeMap.forEach((key, value) -> {
                try {
                    final Object datumValue = parser.parse(value);
                    if (key.contains("Market Cap")) frame.data().setValue(ticker, YahooField.MARKET_CAP, datumValue);
                    else if (key.startsWith("Market Cap")) frame.data().setValue(ticker, YahooField.MARKET_CAP, datumValue);
                    else if (key.startsWith("Trailing P/E")) frame.data().setValue(ticker, YahooField.PE_TRAILING, datumValue);
                    else if (key.startsWith("Forward P/E")) frame.data().setValue(ticker, YahooField.PE_FORWARD, datumValue);
                    else if (key.startsWith("PEG Ratio")) frame.data().setValue(ticker, YahooField.PE_FORWARD, datumValue);
                    else if (key.startsWith("Price/Sales")) frame.data().setValue(ticker, YahooField.PRICE_SALES_RATIO, datumValue);
                    else if (key.startsWith("Price/Book")) frame.data().setValue(ticker, YahooField.PRICE_BOOK_RATIO, datumValue);
                    else if (key.startsWith("Fiscal Year Ends")) frame.data().setValue(ticker, YahooField.FISCAL_YEAR_END, datumValue);
                    else if (key.contains("Most Recent Quarter")) frame.data().setValue(ticker, YahooField.MOST_RECENT_QUARTER, datumValue);
                    else if (key.startsWith("Profit Margin")) frame.data().setValue(ticker, YahooField.PROFIT_MARGIN, datumValue);
                    else if (key.startsWith("Operating Margin")) frame.data().setValue(ticker, YahooField.OPERATING_MARGIN, datumValue);
                    else if (key.startsWith("Return on Assets")) frame.data().setValue(ticker, YahooField.RETURN_ON_ASSETS, datumValue);
                    else if (key.startsWith("Return on Equity")) frame.data().setValue(ticker, YahooField.RETURN_ON_EQUITY, datumValue);
                    else if (key.startsWith("Revenue (ttm)")) frame.data().setValue(ticker, YahooField.REVENUE_TTM, datumValue);
                    else if (key.startsWith("Revenue Per Share")) frame.data().setValue(ticker, YahooField.REVENUE_PER_SHARE, datumValue);
                    else if (key.startsWith("Revenue")) frame.data().setValue(ticker, YahooField.REVENUE_TTM, datumValue);
                    else if (key.startsWith("Qtrly Revenue Growth")) frame.data().setValue(ticker, YahooField.REVENUE_GROWTH_QTLY, datumValue);
                    else if (key.startsWith("Quarterly Revenue Growth")) frame.data().setValue(ticker, YahooField.REVENUE_GROWTH_QTLY, datumValue);
                    else if (key.startsWith("Gross Profit")) frame.data().setValue(ticker, YahooField.GROSS_PROFIT, datumValue);
                    else if (key.startsWith("EBITDA (ttm)")) frame.data().setValue(ticker, YahooField.EBITDA_TTM, datumValue);
                    else if (key.startsWith("EBITDA")) frame.data().setValue(ticker, YahooField.EBITDA_TTM, datumValue);
                    else if (key.startsWith("Diluted EPS")) frame.data().setValue(ticker, YahooField.EPS_DILUTED, datumValue);
                    else if (key.startsWith("Qtrly Earnings Growth")) frame.data().setValue(ticker, YahooField.EARNINGS_GRWOTH_QTLY, datumValue);
                    else if (key.startsWith("Quarterly Earnings Growth")) frame.data().setValue(ticker, YahooField.EARNINGS_GRWOTH_QTLY, datumValue);
                    else if (key.startsWith("Total Cash (mrq)")) frame.data().setValue(ticker, YahooField.CASH_MRQ, datumValue);
                    else if (key.startsWith("Total Cash Per Share")) frame.data().setValue(ticker, YahooField.CASH_PER_SHARE, datumValue);
                    else if (key.startsWith("Total Cash")) frame.data().setValue(ticker, YahooField.CASH_MRQ, datumValue);
                    else if (key.startsWith("Total Debt (mrq)")) frame.data().setValue(ticker, YahooField.DEBT_MRQ, datumValue);
                    else if (key.startsWith("Total Debt/Equity (mrq)")) frame.data().setValue(ticker, YahooField.DEBT_OVER_EQUITY_MRQ, datumValue);
                    else if (key.startsWith("Total Debt/Equity")) frame.data().setValue(ticker, YahooField.DEBT_OVER_EQUITY_MRQ, datumValue);
                    else if (key.startsWith("Total Debt")) frame.data().setValue(ticker, YahooField.DEBT_MRQ, datumValue);
                    else if (key.startsWith("Beta")) frame.data().setValue(ticker, YahooField.BETA, datumValue);
                    else if (key.startsWith("Current Ratio (mrq)")) frame.data().setValue(ticker, YahooField.CURRENT_RATIO, datumValue);
                    else if (key.startsWith("Current Ratio")) frame.data().setValue(ticker, YahooField.CURRENT_RATIO, datumValue);
                    else if (key.startsWith("Book Value Per Share")) frame.data().setValue(ticker, YahooField.BOOK_VALUE_PER_SHARE, datumValue);
                    else if (key.startsWith("Operating Cash Flow")) frame.data().setValue(ticker, YahooField.OPERATING_CASH_FLOW, datumValue);
                    else if (key.startsWith("Levered Free Cash Flow")) frame.data().setValue(ticker, YahooField.LEVERED_FREE_CASH_FLOW, datumValue);
                    else if (key.startsWith("Avg Vol (3 month)")) frame.data().setValue(ticker, YahooField.ADV_3MONTH, datumValue);
                    else if (key.startsWith("Avg Vol (10 day)")) frame.data().setValue(ticker, YahooField.ADV_10DAY, datumValue);
                    else if (key.startsWith("Shares Outstanding")) frame.data().setValue(ticker, YahooField.SHARES_OUTSTANDING, datumValue);
                    else if (key.startsWith("Float")) frame.data().setValue(ticker, YahooField.SHARES_FLOAT, datumValue);
                    else if (key.startsWith("% Held by Insiders")) frame.data().setValue(ticker, YahooField.OWNER_PERCENT_INSIDER, datumValue);
                    else if (key.startsWith("% Held by Institutions")) frame.data().setValue(ticker, YahooField.OWNER_PERCENT_INSTITUTION, datumValue);
                    else if (key.startsWith("Short Ratio")) frame.data().setValue(ticker, YahooField.SHARES_SHORT_RATIO, datumValue);
                    else if (key.startsWith("Shares Short (prior month)")) frame.data().setValue(ticker, YahooField.SHARES_SHORT_PRIOR, datumValue);
                    else if (key.startsWith("Shares Short")) frame.data().setValue(ticker, YahooField.SHARES_SHORT, datumValue);
                    else if (key.startsWith("Forward Annual Dividend Rate")) frame.data().setValue(ticker, YahooField.DIVIDEND_FWD, datumValue);
                    else if (key.startsWith("Forward Annual Dividend Yield")) frame.data().setValue(ticker, YahooField.DIVIDEND_FWD_YIELD, datumValue);
                    else if (key.startsWith("Trailing Annual Dividend Rate")) frame.data().setValue(ticker, YahooField.DIVIDEND_TRAILING, datumValue);
                    else if (key.startsWith("Trailing Annual Dividend Yield")) frame.data().setValue(ticker, YahooField.DIVIDEND_TRAILING_YIELD, datumValue);
                    else if (key.startsWith("Payout Ratio")) frame.data().setValue(ticker, YahooField.DIVIDEND_PAYOUT_RATIO, datumValue);
                    else if (key.startsWith("Dividend Date")) frame.data().setValue(ticker, YahooField.DIVIDEND_PAY_DATE, datumValue);
                    else if (key.startsWith("Ex-Dividend Date")) frame.data().setValue(ticker, YahooField.DIVIDEND_EX_DATE, datumValue);
                    else if (key.startsWith("Last Split Date")) frame.data().setValue(ticker, YahooField.LAST_SPLIT_DATE, datumValue);
                } catch (Exception ex) {
                    throw new RuntimeException(String.format("Failed to process field %s = %s", key, value) , ex);
                }

            });
        } catch (Exception ex) {
            throw new DataFrameException("Failed to extract statistic for " + ticker, ex);
        }
    }


    /**
     * Parses all rows with two cells and returns cell values
     * @param document  the document to parse
     * @return          the map of property & values
     */
    private Map<String,String> parseAttributes(Document document) {
        final Map<String,String> attributeMap = new HashMap<>();
        final Elements rows = document.select("tr");
        for (Element row : rows) {
            final Elements cells = row.select("td");
            if (cells.size() == 2) {
                final String name = toString(cells.get(0));
                final String value = toString(cells.get(1));
                if (name != null && value != null) {
                    attributeMap.put(name, value);
                }
            }
        }
        return attributeMap;
    }


    /**
     * Extracts a terminal value from a table cell element
     * @param cell  the cell element reference
     * @return      the value extracted from cell
     */
    private String toString(Element cell) {
        final Elements span = cell.select("span");
        if (span.size() == 1) {
            final String text = span.get(0).text();
            return text == null ? null : text.trim();
        } else {
            final String text = cell.text();
            return text == null ? null : text.trim();
        }
    }


    /**
     * The options for this source
     */
    public class Options implements DataFrameSource.Options<String,YahooField> {

        private Set<String> tickers = new HashSet<>();


        @Override
        public void validate() {
            if (tickers == null) throw new IllegalStateException("The set of tickers cannot be null");
            if (tickers.size() == 0) throw new IllegalStateException("At least one ticker must be specified");
        }

        /**
         * Sets the tickers for these options
         * @param tickers   the tickers
         */
        public Options withTickers(String...tickers) {
            this.tickers.addAll(Arrays.asList(tickers));
            return this;
        }

        /**
         * Sets the tickers for these options
         * @param tickers   the tickers
         */
        public Options withTickers(Iterable<String> tickers) {
            this.tickers.addAll(Collect.asList(tickers));
            return this;
        }
    }


    public static void main(String[] args) {
        final String[] tickers = {"AAPL", "BLK", "ORCL"};
        final YahooStatsSource source = new YahooStatsSource();
        final DataFrame<String,YahooField> frame = source.read(o -> o.withTickers(tickers));
        frame.out().print();
    }

}
