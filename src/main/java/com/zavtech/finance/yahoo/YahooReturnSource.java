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

import java.io.File;
import java.time.LocalDate;
import java.time.Period;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.Try;

/**
 * Class summary goes here...
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooReturnSource implements DataFrameSource<LocalDate,String,YahooReturnOptions> {


    @Override
    public <T extends Options<?,?>> boolean isSupported(T options) {
        return options instanceof YahooReturnOptions;
    }

    @Override
    public DataFrame<LocalDate, String> read(YahooReturnOptions options) throws DataFrameException {
        final ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            final List<Callable<DataFrame<LocalDate,String>>> tasks = createTasks(Options.validate(options));
            final List<Future<DataFrame<LocalDate,String>>> futures = executor.invokeAll(tasks);
            final int dayCount = Period.between(options.getStart(), options.getEnd()).getDays();
            final Index<LocalDate> rowKeys = Index.of(LocalDate.class, dayCount);
            final Index<String> colKeys = Index.of(String.class, options.getTickers().size());
            final DataFrame<LocalDate,String> result = DataFrame.ofDoubles(rowKeys, colKeys);
            return futures.stream().map(future -> Try.call(future::get)).reduce(result, (left, right) -> {
                try {
                    result.addAll(left);
                    result.addAll(right);
                    return result;
                } catch (Exception ex) {
                    throw new YahooException("Failed to load returns from Yahoo Finance", ex);
                }
            }).rows().sort((r1 , r2) -> r1.key().compareTo(r2.key()));
        } catch (YahooException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new YahooException("Failed to load returns from Yahoo Finance", ex);
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Returns the list of tasks for the request specified
     * @param request   the return request
     * @return          the list of tasks
     */
    private List<Callable<DataFrame<LocalDate,String>>> createTasks(YahooReturnOptions request) {
        switch (request.getType()) {
            case CUM:   return createCumReturnTasks(request).collect(Collectors.toList());
            case DAILY: return createDayReturnTasks(request).collect(Collectors.toList());
            default:    throw new IllegalArgumentException("Unsupported return type: " + request.getType());
        }
    }

    /**
     * Returns a stream of callables that compute cumulative returns
     * @param request   the return request
     * @return          the stream of callables for cumulative returns
     */
    private Stream<Callable<DataFrame<LocalDate,String>>> createCumReturnTasks(YahooReturnOptions request) {
        return request.getTickers().stream().map(ticker -> () -> {
            try {
                final DataFrame<LocalDate,YahooField> quotes = DataFrame.read().apply(YahooQuoteHistoryOptions.class, options -> {
                    options.setAdjustForSplits(true);
                    options.setTicker(ticker);
                    options.setStart(options.getStart());
                    options.setEnd(options.getEnd());
                });
                final Array<LocalDate> rowKeys = quotes.rows().keyArray();
                return DataFrame.of(rowKeys, String.class, columns -> {
                    columns.add(ticker, Double.class).applyDoubles(v -> {
                        final int rowIndex = v.rowOrdinal();
                        final double p0 = quotes.data().getDouble(0, YahooField.PX_CLOSE);
                        final double p1 = quotes.data().getDouble(rowIndex, YahooField.PX_CLOSE);
                        return p1 / p0 - 1d;
                    });
                });
            } catch (Exception ex) {
                throw new YahooException("Failed to load quote data from Yahoo Finance for " + ticker);
            }
        });
    }


    /**
     * Returns a stream of callables that compute 1-day returns
     * @param request   the return request
     * @return          the stream of callables for 1-day returns
     */
    private Stream<Callable<DataFrame<LocalDate,String>>> createDayReturnTasks(YahooReturnOptions request) {
        return request.getTickers().stream().map(ticker -> () -> {
            try {
                final DataFrame<LocalDate,YahooField> quotes = DataFrame.read().apply(YahooQuoteHistoryOptions.class, options -> {
                    options.setTicker(ticker);
                    options.setStart(request.getStart());
                    options.setEnd(request.getEnd());
                    options.setAdjustForSplits(true);
                });
                final Array<LocalDate> rowKeys = quotes.rows().keyArray();
                return DataFrame.of(rowKeys, String.class, columns -> {
                    columns.add(ticker, Double.class).applyDoubles(v -> {
                        final int rowIndex = v.rowOrdinal();
                        if (rowIndex == 0) {
                            return 0d;
                        } else {
                            final double p0 = quotes.data().getDouble(rowIndex-1, YahooField.PX_CLOSE);
                            final double p1 = quotes.data().getDouble(rowIndex, YahooField.PX_CLOSE);
                            return p1 / p0 - 1d;
                        }
                    });
                });
            } catch (Exception ex) {
                throw new YahooException("Failed to load quote data from Yahoo Finance for " + ticker, ex);
            }
        });
    }


    public static void main(String[] args) {
        final LocalDate start = LocalDate.of(2005, 1, 1);
        final LocalDate end = LocalDate.of(2014, 6, 1);
        DataFrame.read().register(new YahooReturnSource());
        DataFrame.read().register(new YahooQuoteHistorySource());
        DataFrame.read().apply(YahooReturnOptions.class, options -> {
            options.setTickers("SPY", "YHOO", "BLK", "AAPL", "GE", "MSFT", "ORCL", "NFLX");
            options.setType(YahooReturnOptions.Type.DAILY);
            options.setStart(start);
            options.setEnd(end);
        }).write().csv(options -> {
            options.setFile(new File("/Users/witdxav/returns.csv"));
        });
    }

}
