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
package com.zavtech.morpheus.examples;

import java.time.LocalDate;
import java.util.Map;
import java.util.Set;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.yahoo.YahooFinance;

/**
 * Examples of various portfolio analysis techniques
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class Portfolio {

    private double initialValue;
    private Set<String> tickers;
    private Map<String,Double> initialWeights;


    /**
     * Constructor
     * @param initialValue      the initial portfolio value
     * @param initialWeights    the initial weights
     */
    public Portfolio(double initialValue, Map<String,Double> initialWeights) {
        this.initialValue = initialValue;
        this.initialWeights = initialWeights;
        this.tickers = initialWeights.keySet();
    }

    public Stats calcStats(DataFrame<LocalDate,String> cumReturns) {
        final DataFrame<LocalDate,String> nav = calcNav(cumReturns);
        final DataFrame<LocalDate,String> returns = calcDailyTotalReturn(nav);
        final double startValue = nav.rows().first().map(row -> row.getDouble(0)).orElse(Double.NaN);
        final double endValue = nav.rows().last().map(row -> row.getDouble(0)).orElse(Double.NaN);
        final double returnVol = returns.colAt(0).stats().stdDev() * Math.sqrt(252);
        return new Stats(endValue / startValue - 1d, returnVol);
    }


    /**
     * Returns the daily returns for the portfolio calculated from the NAV time series
     * @param nav   single column DataFrame of portfolio NAV
     * @return      the portfolio daily returns
     */
    private DataFrame<LocalDate,String> calcDailyTotalReturn(DataFrame<LocalDate,String> nav) {
        return nav.copy().cols().replaceKey("NAV", "Returns").applyDoubles(v -> {
            final int rowOrdinal = v.rowOrdinal();
            if (rowOrdinal == 0) {
                return 0d;
            } else {
                final double v1 = nav.data().getDouble(rowOrdinal-1, 0);
                final double v2 = nav.data().getDouble(rowOrdinal, 0);
                return v2 / v1 - 1d;
            }
        });
    }


    /**
     * Returns the daily returns for the portfolio based on cumulative asset returns
     * @param cumReturns        the cumulative asset returns
     * @return                  the portfolio daily returns
     */
    public DataFrame<LocalDate,String> calcCumTotalReturn(DataFrame<LocalDate,String> cumReturns) {
        final DataFrame<LocalDate,String> nav = calcNav(cumReturns);
        return nav.copy().cols().replaceKey("NAV", "Returns").applyDoubles(v -> {
            final int rowOrdinal = v.rowOrdinal();
            if (rowOrdinal == 0) {
                return 0d;
            } else {
                final double v1 = nav.data().getDouble(0, 0);
                final double v2 = nav.data().getDouble(rowOrdinal, 0);
                return v2 / v1 - 1d;
            }
        });
    }


    /**
     * Calculates the portfolio total NAV given the asset returns
     * @param cumReturns    the cumulative asset returns
     * @return              the portfolio NAV
     */
    public DataFrame<LocalDate,String> calcNav(DataFrame<LocalDate,String> cumReturns) {
        final Array<LocalDate> dates = cumReturns.rows().keyArray();
        return DataFrame.ofDoubles(dates, "NAV").applyDoubles(v -> {
            final int rowOrdinal = v.rowOrdinal();
            return tickers.stream().mapToDouble(ticker -> {
                final double initialWeight = initialWeights.get(ticker);
                final double startingValue = initialWeight * initialValue;
                if (rowOrdinal == 0) {
                    return startingValue;
                } else {
                    final double cumReturn = cumReturns.data().getDouble(rowOrdinal, ticker);
                    return startingValue * (1d + cumReturn);
                }
            }).sum();
        });
    }


    /**
     * Calculates the DataFrame of PnL values for the portfolio
     * @param cumReturns    the cumulative returns
     * @return              the DataFrame of portfolio PnL at the ticker and total
     */
    public DataFrame<LocalDate,String> calcPnl(DataFrame<LocalDate,String> cumReturns) {
        final DataFrame<LocalDate,String> marketValue = calcMarketValue(cumReturns);
        return marketValue.copy().applyDoubles(v -> {
            return v.getDouble() - marketValue.data().getDouble(0, v.colKey());
        });
    }


    /**
     * Computes evolving market values for this portfolio given cumulative asset returns
     * @param cumReturns    the cumulative asset returns
     * @return              the DataFrame of market values
     */
    public DataFrame<LocalDate,String> calcMarketValue(DataFrame<LocalDate,String> cumReturns) {
        final DataFrame<LocalDate,String> result = cumReturns.copy().applyDoubles(v -> Double.NaN);
        result.rows().forEach(row -> {
            final int ordinal = row.ordinal();
            if (ordinal == 0) {
                this.initialWeights.forEach((ticker, weight) -> {
                    row.setDouble(ticker, weight * initialValue);
                });
            } else {
                row.applyDoubles(v -> {
                    final int rowOrdinal = v.rowOrdinal();
                    final String ticker = v.colKey();
                    final double initValue = v.col().getDouble(0);
                    final double cumReturn = cumReturns.data().getDouble(rowOrdinal, ticker);
                    return initValue * (1d + cumReturn);
                });
            }
        });
        return result;
    }



    public class Stats {

        private double portfolioReturn;
        private double portfolioRisk;
        private double portfolioSharpe;

        public Stats(double portfolioReturn, double portfolioRisk) {
            this.portfolioReturn = portfolioReturn;
            this.portfolioRisk = portfolioRisk;
            this.portfolioSharpe = portfolioReturn / portfolioRisk;
        }

        public double getPortfolioReturn() {
            return portfolioReturn;
        }

        public double getPortfolioRisk() {
            return portfolioRisk;
        }
    }



    public static void main(String[] args) {
        final Portfolio portfolio = new Portfolio(100d, Collect.asMap(map -> {
            map.put("VTI", 0.35d);
            map.put("VEA", 0.21d);
            map.put("VWO", 0.16d);
            map.put("VIG", 0.08d);
            map.put("XLE", 0.05d);
            map.put("VTEB", 0.15d);
        }));

        final LocalDate end = LocalDate.now();
        final LocalDate start = end.minusYears(2);
        final YahooFinance yahoo = new YahooFinance();
        final Array<String> tickers = Array.of("VTI", "VEA", "VWO", "VIG", "XLE", "VTEB");
        final DataFrame<LocalDate,String> cumReturns = yahoo.getCumReturns(start, end, tickers);
        final Stats stats = portfolio.calcStats(cumReturns);
        IO.println(stats);
    }

}
