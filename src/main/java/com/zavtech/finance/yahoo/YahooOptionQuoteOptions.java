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
import java.util.Optional;
import java.util.function.Consumer;

import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Initialiser;

/**
 * A DataFrame request used to load option quote data from the Yahoo Finance web site.
 *
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooOptionQuoteOptions implements DataFrameSource.Options<String,YahooField> {

    private String ticker;
    private LocalDate expiry;

    /**
     * Constructor
     */
    public YahooOptionQuoteOptions() {
        super();
    }

    /**
     * Constructor
     * @param ticker    the underlying security ticker
     * @param expiry    the optional expiry date
     */
    public YahooOptionQuoteOptions(String ticker, LocalDate expiry) {
        this.ticker = ticker;
        this.expiry = expiry;
    }

    @Override
    public void validate() {
        Asserts.notNull(getTicker(), "The underlying ticker cannot be null");
    }

    /**
     * Sets the underlying ticker for options
     * @param ticker    the underlying ticker
     */
    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    /**
     * Sets the expiry date for options
     * @param expiry    the expiry
     */
    public void setExpiry(LocalDate expiry) {
        this.expiry = expiry;
    }

    /**
     * Returns the underlying ticker for this request
     * @return  the underlying ticker
     */
    public String getTicker() {
        return ticker;
    }

    /**
     * Returns the expiry date for request
     * @return  the expiry date
     */
    public Optional<LocalDate> getExpiry() {
        return Optional.ofNullable(expiry);
    }
}
