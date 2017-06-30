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

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;

/**
 * A DataFrameRequest used to select prices and compute returns from Yahoo Finance
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooReturnOptions implements DataFrameSource.Options<LocalDate, String> {

    public enum Type { CUM, DAILY }

    private Type type;
    private LocalDate start;
    private LocalDate end;
    private Set<String> tickers;

    /**
     * Constructor
     */
    public YahooReturnOptions() {
        this(new HashSet<>());
    }

    /**
     * Constructor
     * @param tickers   the tickers for these options
     */
    public YahooReturnOptions(Set<String> tickers) {
        this.tickers = tickers;
    }


    @Override
    public void validate() {
        Asserts.notNull(getType() != null, "The return type is required");
        Asserts.notNull(getStart() != null, "A start date is required");
        Asserts.notNull(getEnd() != null, "A end date is required");
        Asserts.check(getTickers().size() > 0, "At least one ticker must be specified");
    }

    /**
     * Sets the return type for these options
     * @param type  the return type
     */
    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Sets the start date for returns
     * @param start the start date
     */
    public void setStart(LocalDate start) {
        this.start = start;
    }

    /**
     * Sets the end date for returns
     * @param end   the end date
     */
    public void setEnd(LocalDate end) {
        this.end = end;
    }

    /**
     * Sets the security tickers for returns
     * @param tickers   the security tickers
     */
    public void setTickers(Set<String> tickers) {
        this.tickers.addAll(tickers);
    }

    /**
     * Sets the security tickers for returns
     * @param tickers   the security tickers
     */
    public void setTickers(String... tickers) {
        this.tickers.addAll(Arrays.asList(tickers));
    }

    /**
     * The return type for this request
     * @return  the return type for this request
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns the start date for this request
     * @return  the start date for request
     */
    public LocalDate getStart() {
        return start;
    }

    /**
     * Returns the end date for this request
     * @return  the end date for request
     */
    public LocalDate getEnd() {
        return end;
    }

    /**
     * Returns the set of tickers for this request
     * @return  the set of tickers for this request
     */
    public Set<String> getTickers() {
        return tickers;
    }
}
