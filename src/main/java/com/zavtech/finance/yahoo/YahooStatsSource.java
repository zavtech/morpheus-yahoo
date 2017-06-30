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
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.http.HttpClient;

/**
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooStatsSource implements DataFrameSource<String,YahooField,YahooStatsOptions> {

    private static final Array<YahooField> fields = Array.of(
            YahooField.ENTERPRISE_VALUE,
            YahooField.MARKET_CAP,
            YahooField.PE_TRAILING,
            YahooField.PE_FORWARD,
            YahooField.PRICE_SALES_RATIO,
            YahooField.PRICE_BOOK_RATIO,
            YahooField.ENTERPRISE_VALUE_REVENUE,
            YahooField.ENTERPRISE_VALUE_EBITDA,
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
            YahooField.DIVIDEND_FWD_RATE,
            YahooField.DIVIDEND_FWD_YIELD,
            YahooField.DIVIDEND_TRAIL_YIELD,
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
        this("http://finance.yahoo.com/q/ks?s=TICKER+Key+Statistics");
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
    public <T extends Options<?,?>> boolean isSupported(T options) {
        return options instanceof YahooStatsOptions;
    }

    @Override
    public DataFrame<String,YahooField> read(YahooStatsOptions options) throws DataFrameException {
        final Set<String> tickers = Options.validate(options).getTickers();
        try {
            final DataFrame<String,YahooField> frame = createFrame(tickers);
            tickers.forEach(ticker -> {
                final String url = urlTemplate.replace("TICKER", ticker);
                HttpClient.getDefault().doGet(httpRequest -> {
                    httpRequest.setUrl(url);
                    httpRequest.setResponseHandler((status, stream) -> {
                        try {
                            final long t1 = System.currentTimeMillis();
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
            final Elements tables = document.select("table.yfnc_datamodoutline1");
            for (Element table : tables) {
                final Elements headers = table.select("td.yfnc_tablehead1");
                final Elements values = table.select("td.yfnc_tabledata1");
                IntStream.range(0, Math.min(headers.size(), values.size())).forEach(index -> {
                    final String header = headers.get(index).text();
                    final String data = values.get(index).text();
                    if (header != null && data != null) {
                        try {
                            final Object value = parser.parse(data);
                            if (header.contains("Market Cap")) frame.data().setValue(ticker, YahooField.MARKET_CAP, value);
                            else if (header.startsWith("Market Cap")) frame.data().setValue(ticker, YahooField.MARKET_CAP, value);
                            else if (header.matches("Enterprise Value \\(.*\\).*")) frame.data().setValue(ticker, YahooField.ENTERPRISE_VALUE, value);
                            else if (header.startsWith("Trailing P/E")) frame.data().setValue(ticker, YahooField.PE_TRAILING, value);
                            else if (header.startsWith("Forward P/E")) frame.data().setValue(ticker, YahooField.PE_FORWARD, value);
                            else if (header.startsWith("PEG Ratio")) frame.data().setValue(ticker, YahooField.PE_FORWARD, value);
                            else if (header.startsWith("Price/Sales")) frame.data().setValue(ticker, YahooField.PRICE_SALES_RATIO, value);
                            else if (header.startsWith("Price/Book")) frame.data().setValue(ticker, YahooField.PRICE_BOOK_RATIO, value);
                            else if (header.startsWith("Enterprise Value/Revenue")) frame.data().setValue(ticker, YahooField.ENTERPRISE_VALUE_REVENUE, value);
                            else if (header.startsWith("Enterprise Value/EBITDA")) frame.data().setValue(ticker, YahooField.ENTERPRISE_VALUE_EBITDA, value);
                            else if (header.startsWith("Fiscal Year Ends")) frame.data().setValue(ticker, YahooField.FISCAL_YEAR_END, value);
                            else if (header.contains("Most Recent Quarter")) frame.data().setValue(ticker, YahooField.MOST_RECENT_QUARTER, value);
                            else if (header.startsWith("Profit Margin")) frame.data().setValue(ticker, YahooField.PROFIT_MARGIN, value);
                            else if (header.startsWith("Operating Margin")) frame.data().setValue(ticker, YahooField.OPERATING_MARGIN, value);
                            else if (header.startsWith("Return on Assets")) frame.data().setValue(ticker, YahooField.RETURN_ON_ASSETS, value);
                            else if (header.startsWith("Return on Equity")) frame.data().setValue(ticker, YahooField.RETURN_ON_EQUITY, value);
                            else if (header.startsWith("Revenue (ttm)")) frame.data().setValue(ticker, YahooField.REVENUE_TTM, value);
                            else if (header.startsWith("Revenue Per Share")) frame.data().setValue(ticker, YahooField.REVENUE_PER_SHARE, value);
                            else if (header.startsWith("Qtrly Revenue Growth")) frame.data().setValue(ticker, YahooField.REVENUE_GROWTH_QTLY, value);
                            else if (header.startsWith("Gross Profit")) frame.data().setValue(ticker, YahooField.GROSS_PROFIT, value);
                            else if (header.startsWith("EBITDA (ttm)")) frame.data().setValue(ticker, YahooField.EBITDA_TTM, value);
                            else if (header.startsWith("Diluted EPS")) frame.data().setValue(ticker, YahooField.EPS_DILUTED, value);
                            else if (header.startsWith("Qtrly Earnings Growth")) frame.data().setValue(ticker, YahooField.EARNINGS_GRWOTH_QTLY, value);
                            else if (header.startsWith("Total Cash (mrq)")) frame.data().setValue(ticker, YahooField.CASH_MRQ, value);
                            else if (header.startsWith("Total Cash Per Share")) frame.data().setValue(ticker, YahooField.CASH_PER_SHARE, value);
                            else if (header.startsWith("Total Debt (mrq)")) frame.data().setValue(ticker, YahooField.DEBT_MRQ, value);
                            else if (header.startsWith("Total Debt/Equity (mrq)")) frame.data().setValue(ticker, YahooField.DEBT_OVER_EQUITY_MRQ, value);
                            else if (header.startsWith("Current Ratio (mrq)")) frame.data().setValue(ticker, YahooField.CURRENT_RATIO, value);
                            else if (header.startsWith("Book Value Per Share (mrq)")) frame.data().setValue(ticker, YahooField.BOOK_VALUE_PER_SHARE, value);
                            else if (header.startsWith("Operating Cash Flow")) frame.data().setValue(ticker, YahooField.OPERATING_CASH_FLOW, value);
                            else if (header.startsWith("Levered Free Cash Flow")) frame.data().setValue(ticker, YahooField.LEVERED_FREE_CASH_FLOW, value);
                            else if (header.startsWith("Avg Vol (3 month)")) frame.data().setValue(ticker, YahooField.ADV_3MONTH, value);
                            else if (header.startsWith("Avg Vol (10 day)")) frame.data().setValue(ticker, YahooField.ADV_10DAY, value);
                            else if (header.startsWith("Shares Outstanding")) frame.data().setValue(ticker, YahooField.SHARES_OUTSTANDING, value);
                            else if (header.startsWith("Float")) frame.data().setValue(ticker, YahooField.SHARES_FLOAT, value);
                            else if (header.startsWith("% Held by Insiders")) frame.data().setValue(ticker, YahooField.OWNER_PERCENT_INSIDER, value);
                            else if (header.startsWith("% Held by Institutions")) frame.data().setValue(ticker, YahooField.OWNER_PERCENT_INSTITUTION, value);
                            else if (header.startsWith("Shares Short (as of ")) frame.data().setValue(ticker, YahooField.SHARES_SHORT, value);
                            else if (header.startsWith("Short Ratio")) frame.data().setValue(ticker, YahooField.SHARES_SHORT_RATIO, value);
                            else if (header.startsWith("Shares Short (prior month)")) frame.data().setValue(ticker, YahooField.SHARES_SHORT_PRIOR, value);
                            else if (header.startsWith("Forward Annual Dividend Rate")) frame.data().setValue(ticker, YahooField.DIVIDEND_FWD_RATE, value);
                            else if (header.startsWith("Forward Annual Dividend Yield")) frame.data().setValue(ticker, YahooField.DIVIDEND_FWD_YIELD, value);
                            else if (header.startsWith("Trailing Annual Dividend Yield")) frame.data().setValue(ticker, YahooField.DIVIDEND_TRAIL_YIELD, value);
                            else if (header.startsWith("Payout Ratio")) frame.data().setValue(ticker, YahooField.DIVIDEND_PAYOUT_RATIO, value);
                            else if (header.startsWith("Dividend Date")) frame.data().setValue(ticker, YahooField.DIVIDEND_PAY_DATE, value);
                            else if (header.startsWith("Ex-Dividend Date")) frame.data().setValue(ticker, YahooField.DIVIDEND_EX_DATE, value);
                            else if (header.startsWith("Last Split Date")) frame.data().setValue(ticker, YahooField.LAST_SPLIT_DATE, value);
                        } catch (Exception ex) {
                            throw new RuntimeException("Failed to process field " + header, ex);
                        }
                    }
                });
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to extract statistic for " + ticker, ex);
        }
    }


    public static void main(String[] args) {
        final String[] tickers = {"AAPL", "MSFT", "GOOG", "ORCL", "FDX", "NFLX", "DELL", "SYMC", "XXX", "GE", "GS", "ML", "UBS", "BLK"};
        DataFrame.read().register(new YahooStatsSource());
        DataFrame.read().apply(new YahooStatsOptions(tickers)).out().print();
    }

}
