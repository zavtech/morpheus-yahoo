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

import java.io.File;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.Try;

/**
 * A DataFrameSource that generates asset returns calculated from close prices downloaded from Yahoo Finance.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooReturnSource extends DataFrameSource<LocalDate,String,YahooReturnSource.Options> {

    private ExecutorService executor;

    /**
     * Constructor
     */
    public YahooReturnSource() {
        this(10);
    }

    /**
     * Constructor
     * @param threadCount   the thread count for this source
     */
    public YahooReturnSource(int threadCount) {
        this.executor = Executors.newFixedThreadPool(threadCount, runnable -> {
            final Thread thread = new Thread(runnable, "YahooReturnSourceThread");
            thread.setDaemon(true);
            return thread;
        });
    }


    @Override
    public DataFrame<LocalDate,String> read(Consumer<Options> configurator) throws DataFrameException {
        try {
            final Options options = initOptions(new Options(), configurator);
            final List<Callable<DataFrame<LocalDate,String>>> tasks = createTasks(options);
            final List<Future<DataFrame<LocalDate,String>>> futures = executor.invokeAll(tasks);
            final Stream<DataFrame<LocalDate,String>> frames = futures.stream().map(Try::get);
            return DataFrame.combineFirst(frames).rows().sort(true);
        } catch (YahooException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new YahooException("Failed to load returns from Yahoo Finance", ex);
        }
    }


    /**
     * Returns the list of tasks for the request specified
     * @param options   the request options
     * @return          the list of tasks
     */
    private List<Callable<DataFrame<LocalDate,String>>> createTasks(Options options) {
        switch (options.type) {
            case "daily":       return createDailyReturnTasks(options).collect(Collectors.toList());
            case "weekly":      return createPeriodReturnTasks(options, 5).collect(Collectors.toList());
            case "monthly":     return createPeriodReturnTasks(options, 20).collect(Collectors.toList());
            case "cumulative":  return createCumReturnTasks(options).collect(Collectors.toList());
            default:    throw new IllegalArgumentException("Unsupported return type: " + options.type);
        }
    }


    /**
     * Returns a stream of callables that compute 1-day returns
     * @param request   the request options
     * @return          the stream of callables for 1-day returns
     */
    private Stream<Callable<DataFrame<LocalDate,String>>> createDailyReturnTasks(Options request) {
        return request.tickers.stream().map(ticker -> () -> {
            try {
                final DataFrame<LocalDate,YahooField> quotes =  loadQuotes(request, ticker, 0);
                final Array<LocalDate> rowKeys = quotes.rows().keyArray();
                return DataFrame.of(rowKeys, String.class, columns -> {
                    columns.add(ticker, Double.class).applyDoubles(v -> {
                        final int rowOrdinal = v.rowOrdinal();
                        if (rowOrdinal == 0) {
                            return 0d;
                        } else {
                            final LocalDate date = quotes.rows().key(rowOrdinal);
                            final double p0 = quotes.data().getDouble(rowOrdinal-1, YahooField.PX_CLOSE);
                            final double p1 = quotes.data().getDouble(rowOrdinal, YahooField.PX_CLOSE);
                            final double result =  (p1 / p0) - 1d;
                            return result;
                        }
                    });
                });
            } catch (Exception ex) {
                throw new YahooException("Failed to load quote data from Yahoo Finance for " + ticker, ex);
            }
        });
    }


    /**
     * Returns a stream of callables that compute N-day returns
     * @param request   the request options
     * @param days      the number od business days for period
     * @return          the stream of callables for N-day returns
     */
    private Stream<Callable<DataFrame<LocalDate,String>>> createPeriodReturnTasks(Options request, int days) {
        return request.tickers.stream().map(ticker -> () -> {
            try {
                final LocalDate start = request.startDate;
                final LocalDate end = request.endDate;
                final DataFrame<LocalDate,YahooField> quotes =  loadQuotes(request, ticker, days * 2);
                final Array<LocalDate> dates = quotes.rows().keyArray().filter(v -> {
                    return start.compareTo(v.getValue()) <= 0 && end.compareTo(v.getValue()) >= 0;
                });
                return DataFrame.of(dates, String.class, columns -> {
                    columns.add(ticker, Double.class).applyDoubles(v -> {
                        if (v.rowOrdinal() == 0) {
                            return 0d;
                        } else {
                            final LocalDate date = v.rowKey();
                            final int rowOrdinal = quotes.rows().ordinalOf(date, true);
                            final double p0 = quotes.data().getDouble(rowOrdinal-days, YahooField.PX_CLOSE);
                            final double p1 = quotes.data().getDouble(rowOrdinal, YahooField.PX_CLOSE);
                            return p1 / p0 - 1d;
                        }
                    });
                });
            } catch (Exception ex) {
                throw new YahooException("Failed to load quote data from Yahoo Finance for " + ticker, ex);
            }
        });
    }


    /**
     * Returns a stream of callables that compute cumulative returns
     * @param request   the return request
     * @return          the stream of callables for cumulative returns
     */
    private Stream<Callable<DataFrame<LocalDate,String>>> createCumReturnTasks(Options request) {
        return request.tickers.stream().map(ticker -> () -> {
            try {
                final DataFrame<LocalDate,YahooField> quotes =  loadQuotes(request, ticker, 0);
                final Array<LocalDate> rowKeys = quotes.rows().keyArray();
                return DataFrame.of(rowKeys, String.class, columns -> {
                    columns.add(ticker, Double.class).applyDoubles(v -> {
                        final int rowOrdinal = v.rowOrdinal();
                        final double p0 = quotes.data().getDouble(0, YahooField.PX_CLOSE);
                        final double p1 = quotes.data().getDouble(rowOrdinal, YahooField.PX_CLOSE);
                        return p1 / p0 - 1d;
                    });
                });
            } catch (Exception ex) {
                throw new YahooException("Failed to load quote data from Yahoo Finance for " + ticker);
            }
        });
    }


    /**
     * Loads split and dividend adjusted daily bars from Yahoo Finance
     * @param request   the request options
     * @param ticker    the ticker to load quotes for
     * @param seedDays  the number of extra leading days to include at start
     * @return          the DataFrame of open, high, low, close, volume quotes
     */
    private DataFrame<LocalDate,YahooField> loadQuotes(Options request, String ticker, int seedDays) {
        final YahooQuoteHistorySource source = DataFrameSource.lookup(YahooQuoteHistorySource.class);
        return source.read(options -> {
            options.withTicker(ticker);
            options.withStartDate(request.startDate.minusDays(seedDays));
            options.withEndDate(request.endDate);
            options.withPaddedHolidays(false);
            options.withAdjustForSplitsAndDividends(true);
        });
    }



    /**
     * The options for this source
     */
    public class Options implements DataFrameSource.Options<LocalDate,String> {

        private String type = "daily";
        private LocalDate startDate;
        private LocalDate endDate = LocalDate.now();
        private Set<String> tickers = new HashSet<>();

        @Override
        public void validate() {
            Asserts.notNull(startDate != null, "A start date is required");
            Asserts.notNull(endDate != null, "A end date is required");
            Asserts.assertTrue(endDate.isAfter(startDate), "The start date must be < end date");
            Asserts.check(tickers.size() > 0, "At least one ticker must be specified");
        }

        /**
         * Signals a request for daily returns
         * @return  these options
         */
        public Options daily() {
            this.type = "daily";
            return this;
        }

        /**
         * Signals a request for weekly returns
         * @return  these options
         */
        public Options weekly() {
            this.type = "weekly";
            return this;
        }

        /**
         * Signals a request for monthly returns
         * @return  these options
         */
        public Options monthly() {
            this.type = "monthly";
            return this;
        }

        /**
         * Signals a request for cumulative daily returns
         * @return  these options
         */
        public Options cumulative() {
            this.type = "cumulative";
            return this;
        }

        /**
         * Sets the start date for these options
         * @param start the start date
         * @return      these options
         */
        public Options withStartDate(LocalDate start) {
            this.startDate = start;
            return this;
        }

        /**
         * Sets the end date for these options
         * @param end   the end date
         * @return      these options
         */
        public Options withEndDate(LocalDate end) {
            this.endDate = end;
            return this;
        }

        /**
         * Sets the start date for these options
         * @param startDate the start date  in the form yyyy-MM-dd
         * @return          these options
         */
        public Options withStartDate(String startDate) {
            this.startDate = LocalDate.parse(startDate);
            return this;
        }

        /**
         * Sets the end date for these options
         * @param endDate   the end date  in the form yyyy-MM-dd
         * @return          these options
         */
        public Options withEndDate(String endDate) {
            this.endDate = LocalDate.parse(endDate);
            return this;
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
        final LocalDate start = LocalDate.of(2005, 1, 1);
        final LocalDate end = LocalDate.of(2014, 6, 1);
        DataFrameSource.register(new YahooReturnSource());
        DataFrameSource.register(new YahooQuoteHistorySource());
        DataFrameSource.lookup(YahooReturnSource.class).read(options -> {
            options.withTickers("SPY", "AAPL", "BLK", "GE", "MSFT", "ORCL", "NFLX");
            options.withStartDate(start);
            options.withEndDate(end);
            options.daily();
        }).write().csv(options -> {
            options.setFile(new File("/Users/witdxav/returns.csv"));
        });
    }

}
