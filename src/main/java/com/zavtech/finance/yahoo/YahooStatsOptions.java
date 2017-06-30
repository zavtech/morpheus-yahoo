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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.zavtech.morpheus.frame.DataFrameSource;

/**
 * Options used to load security fundamental statistics from Yahoo Finance
 *
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooStatsOptions implements DataFrameSource.Options<String,YahooField> {

    private Set<String> tickers = new HashSet<>();

    /**
     * Constructor
     */
    public YahooStatsOptions() {
        this(Collections.emptySet());
    }

    @Override
    public void validate() {
        if (tickers == null) throw new IllegalStateException("The set of tickers cannot be null");
        if (tickers.size() == 0) throw new IllegalStateException("At least one ticker must be specified");
    }

    /**
     * Constructor
     * @param tickers   the tickers to load
     */
    public YahooStatsOptions(Set<String> tickers) {
        this.tickers = tickers;
    }

    /**
     * Constructor
     * @param tickers   the tickers to load
     */
    public YahooStatsOptions(String... tickers) {
        this.tickers = new HashSet<>(Arrays.asList(tickers));
    }

    /**
     * Sets the tickers for these options
     * @param tickers   the tickers
     */
    public void setTickers(String...tickers) {
        this.tickers.addAll(Arrays.asList(tickers));
    }

    /**
     * Sets the tickers for these options
     * @param tickers   the tickers
     */
    public void setTickers(Collection<String> tickers) {
        this.tickers.addAll(tickers);
    }

    /**
     * Returns the set of tickers for this request
     * @return the tickers for this request
     */
    public Set<String> getTickers() {
        return tickers;
    }
}

