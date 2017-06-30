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
import com.zavtech.morpheus.util.Asserts;

/**
 * Class summary goes here...
 *
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooQuoteLiveOptions implements DataFrameSource.Options<String,YahooField> {

    private Set<String> tickers = new HashSet<>();
    private Set<YahooField> fields = new HashSet<>();

    /**
     * Constructor
     */
    public YahooQuoteLiveOptions() {
        this(Collections.emptySet());
    }

    /**
     * Constructor
     * @param tickers   the tickers
     */
    public YahooQuoteLiveOptions(Set<String> tickers) {
        this.tickers = tickers;
    }


    @Override
    public void validate() {
        Asserts.check(getTickers().size() > 0, "At least one ticker must be specified");
        Asserts.check(getFields().size() > 0, "At least one field must be specified");
    }

    /**
     * Sets the tickers for these options
     * @param tickers   the tickers
     */
    public void setTickers(Collection<String> tickers) {
        this.tickers.addAll(tickers);
    }

    /**
     * Sets the fields for these options
     * @param fields    the fields
     */
    public void setFields(Collection<YahooField> fields) {
        this.fields.addAll(fields);
    }

    /**
     * Sets the tickers for these options
     * @param tickers   the tickers
     */
    public void setTickers(String... tickers) {
        this.tickers.addAll(Arrays.asList(tickers));
    }

    /**
     * Sets the fields for these options
     * @param fields    the fields
     */
    public void setFields(YahooField... fields) {
        this.fields.addAll(Arrays.asList(fields));
    }

    /**
     * Returns the set of tickers for this request
     * @return  the set of tickers
     */
    public Set<String> getTickers() {
        return tickers;
    }

    /**
     * Returns the set of fields for this request
     * @return  the set of fields for request
     */
    public Set<YahooField> getFields() {
        return fields;
    }

}
