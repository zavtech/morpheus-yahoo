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

import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;

/**
 * A DataFrameRequest designed to query for historical quote data from Yahoo Finance.
 *
 * Any use of the extracted data from this software should adhere to Yahoo Finance Terms and Conditions.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooQuoteHistoryOptions implements DataFrameSource.Options<LocalDate,YahooField> {

    private String ticker;
    private LocalDate start;
    private LocalDate end = LocalDate.now();
    private boolean adjustForSplits;

    /**
     * Constructor
     */
    public YahooQuoteHistoryOptions() {
        this(null, null, null, true);
    }

    /**
     * Constructor
     * @param ticker    the instrument ticker
     * @param start     the start date
     * @param end       the end date
     * @param adjustForSplits   true to adjust for splits
     */
    public YahooQuoteHistoryOptions(String ticker, LocalDate start, LocalDate end, boolean adjustForSplits) {
        this.ticker = ticker;
        this.start = start;
        this.end = end;
        this.adjustForSplits = adjustForSplits;
    }

    @Override
    public void validate() {
        Asserts.notNull(getTicker(), "The ticker cannot be null");
        Asserts.notNull(getStart(), "The start date cannot be null");
        Asserts.notNull(getEnd(), "The end date cannot be null");
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public void setStart(LocalDate start) {
        this.start = start;
    }

    public void setEnd(LocalDate end) {
        this.end = end;
    }

    public void setAdjustForSplits(boolean adjustForSplits) {
        this.adjustForSplits = adjustForSplits;
    }

    /**
     * Returns the ticker for this request
     * @return  the security ticker
     */
    public String getTicker() {
        return ticker;
    }

    /**
     * Returns the start date for request
     * @return  the start date
     */
    public LocalDate getStart() {
        return start;
    }

    /**
     * Returns the end date for request
     * @return  the end date
     */
    public LocalDate getEnd() {
        return end;
    }

    /**
     * Returns true if prices should be split and dividend adjusted
     * @return  true for price adjustments
     */
    public boolean isAdjustForSplits() {
        return adjustForSplits;
    }
}
