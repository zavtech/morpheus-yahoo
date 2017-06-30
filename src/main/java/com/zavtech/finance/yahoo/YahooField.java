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
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that defines all the supported fields in the Yahoo finance module.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooField implements Comparable<YahooField>, java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private static final Map<Class<?>,Object> nullValueMap = new HashMap<>();
    private static final Map<String,YahooField> fieldMap = new HashMap<>();

    /**
     * Static initializer
     */
    static {
        nullValueMap.put(Double.class, Double.NaN);
        nullValueMap.put(LocalDate.class, null);
        nullValueMap.put(String.class, null);
    }

    public static final YahooField TICKER = create("TICKER", String.class);
    public static final YahooField TIMESTAMP = create("TIMESTAMP",ZonedDateTime.class);
    public static final YahooField NAME = create("NAME", String.class);
    public static final YahooField PX_OPEN = create("PX_OPEN", Double.class);
    public static final YahooField PX_HIGH = create("PX_HIGH", Double.class);
    public static final YahooField PX_LOW = create("PX_LOW", Double.class);
    public static final YahooField PX_CLOSE = create("PX_CLOSE", Double.class);
    public static final YahooField PX_VOLUME = create("PX_VOLUME", Double.class);
    public static final YahooField PX_CHANGE = create("PX_CHANGE", Double.class);
    public static final YahooField PX_CHANGE_PERCENT = create("PX_CHANGE_PERCENT", Double.class);
    public static final YahooField PX_52W_LOW = create("PX_52W_LOW", Double.class);
    public static final YahooField PX_52W_HIGH = create("PX_52W_HIGH", Double.class);
    public static final YahooField PX_SPLIT_RATIO = create("PX_SPLIT_RATIO", Double.class);
    public static final YahooField PX_BID = create("PX_BID", Double.class);
    public static final YahooField PX_BID_SIZE = create("PX_BID_SIZE", Double.class);
    public static final YahooField PX_ASK = create("PX_ASK", Double.class);
    public static final YahooField PX_ASK_SIZE = create("PX_ASK_SIZE", Double.class);
    public static final YahooField PX_LAST_DATE = create("PX_LAST_DATE", LocalDate.class);
    public static final YahooField PX_LAST_TIME = create("PX_LAST_TIME",LocalTime.class);
    public static final YahooField PX_LAST = create("PX_LAST", Double.class);
    public static final YahooField PX_LAST_SIZE = create("PX_LAST_SIZE", Double.class);
    public static final YahooField PX_STRIKE = create("PX_STRIKE", Double.class);
    public static final YahooField PX_PREVIOUS_CLOSE = create("PX_PREVIOUS_CLOSE", Double.class);
    public static final YahooField PX_LAST_AFTER_HOURS = create("PX_LAST_AFTER_HOURS", Double.class);
    public static final YahooField PX_CHANGE_AFTER_HOURS = create("PX_CHANGE_AFTER_HOURS", Double.class);
    public static final YahooField PX_CHANGE_52W_LOW = create("PX_CHANGE_52W_LOW", Double.class);
    public static final YahooField PX_CHANGE_52W_HIGH = create("PX_CHANGE_52W_HIGH", Double.class);
    public static final YahooField PX_CHANGE_PERCENT_52W_LOW = create("PX_CHANGE_PERCENT_52W_LOW", Double.class);
    public static final YahooField PX_CHANGE_PERCENT_52W_HIGH = create("PX_CHANGE_PERCENT_52W_HIGH", Double.class);
    public static final YahooField PX_DAYS_RANGE = create("PX_DAYS_RANGE", Double.class);
    public static final YahooField PX_MOVING_AVG_52W = create("PX_MOVING_AVG_52W", Double.class);
    public static final YahooField PX_MOVING_AVG_200D = create("PX_MOVING_AVG_200D", Double.class);
    public static final YahooField OPTION_TYPE = create("OPTION_TYPE", String.class);
    public static final YahooField EXPIRY_DATE = create("EXPIRY_DATE", LocalDate.class);
    public static final YahooField TRADE_DATE = create("TRADE_DATE", LocalDate.class);
    public static final YahooField EXCHANGE = create("EXCHANGE", String.class);
    public static final YahooField AVG_DAILY_VOLUME = create("AVG_DAILY_VOLUME", Double.class);
    public static final YahooField BOOK_VALUE = create("BOOK_VALUE", Double.class);
    public static final YahooField DIVIDEND_PER_SHARE = create("DIVIDEND_PER_SHARE", Double.class);
    public static final YahooField IMPLIED_VOLATILITY = create("IMPLIED_VOLATILITY", Double.class);
    public static final YahooField OPEN_INTEREST = create("OPEN_INTEREST", Double.class);
    public static final YahooField EPS = create("EPS", Double.class);
    public static final YahooField EPS_ESTIMATE = create("EPS_ESTIMATE", Double.class);
    public static final YahooField EPS_NEXT_YEAR = create("EPS_NEXT_YEAR", Double.class);
    public static final YahooField EPS_NEXT_QUARTER = create("EPS_NEXT_QUARTER", Double.class);
    public static final YahooField FLOAT_SHARES = create("FLOAT_SHARES", Double.class);
    public static final YahooField FIFTY_TWO_WEEK_LOW = create("FIFTY_TWO_WEEK_LOW", Double.class);
    public static final YahooField FIFTY_TWO_WEEK_HIGH = create("FIFTY_TWO_WEEK_HIGH", Double.class);
    public static final YahooField ANNUALISED_GAIN = create("ANNUALISED_GAIN", Double.class);
    public static final YahooField PE_TRAILING = create("PE_TRAILING", Double.class);
    public static final YahooField PE_FORWARD = create("PE_FORWARD", Double.class);
    public static final YahooField MARKET_CAP = create("MARKET_CAP", Double.class);
    public static final YahooField ENTERPRISE_VALUE = create("ENTERPRISE_VALUE", Double.class);
    public static final YahooField EBITDA = create("EBITDA", Double.class);
    public static final YahooField PRICE_SALES_RATIO = create("PRICE_SALES_RATIO", Double.class);
    public static final YahooField PRICE_BOOK_RATIO = create("PRICE_BOOK_RATIO", Double.class);
    public static final YahooField PEG_RATIO = create("PEG_RATIO", Double.class);
    public static final YahooField PRICE_EPS_RATIO_CURRENT_YEAR = create("PRICE_EPS_RATIO_CURRENT_YEAR", Double.class);
    public static final YahooField PRICE_EPS_RATIO_NEXT_YEAR = create("PRICE_EPS_RATIO_NEXT_YEAR", Double.class);
    public static final YahooField ENTERPRISE_VALUE_REVENUE = create("ENTERPRISE_VALUE_REVENUE", Double.class);
    public static final YahooField ENTERPRISE_VALUE_EBITDA = create("ENTERPRISE_VALUE_EBITDA", Double.class);
    public static final YahooField FISCAL_YEAR_END = create("FISCAL_YEAR_END", LocalDate.class);
    public static final YahooField MOST_RECENT_QUARTER = create("MOST_RECENT_QUARTER", LocalDate.class);
    public static final YahooField PROFIT_MARGIN = create("PROFIT_MARGIN", Double.class);
    public static final YahooField OPERATING_MARGIN = create("OPERATING_MARGIN", Double.class);
    public static final YahooField RETURN_ON_ASSETS = create("RETURN_ON_ASSETS", Double.class);
    public static final YahooField RETURN_ON_EQUITY = create("RETURN_ON_EQUITY", Double.class);
    public static final YahooField REVENUE_TTM = create("REVENUE_TTM", Double.class);
    public static final YahooField REVENUE_PER_SHARE = create("REVENUE_PER_SHARE", Double.class);
    public static final YahooField REVENUE_GROWTH_QTLY = create("REVENUE_GROWTH_QTLY", Double.class);
    public static final YahooField GROSS_PROFIT = create("GROSS_PROFIT", Double.class);
    public static final YahooField EBITDA_TTM = create("EBITDA_TTM", Double.class);
    public static final YahooField EPS_DILUTED = create("EPS_DILUTED", Double.class);
    public static final YahooField EARNINGS_GRWOTH_QTLY = create("EARNINGS_GRWOTH_QTLY", Double.class);
    public static final YahooField CASH_MRQ = create("CASH_MRQ", Double.class);
    public static final YahooField CASH_PER_SHARE = create("CASH_PER_SHARE", Double.class);
    public static final YahooField DEBT_MRQ = create("DEBT_MRQ", Double.class);
    public static final YahooField DEBT_OVER_EQUITY_MRQ = create("DEBT_OVER_EQUITY_MRQ", Double.class);
    public static final YahooField CURRENT_RATIO = create("CURRENT_RATIO", Double.class);
    public static final YahooField BOOK_VALUE_PER_SHARE = create("BOOK_VALUE_PER_SHARE", Double.class);
    public static final YahooField OPERATING_CASH_FLOW = create("OPERATING_CASH_FLOW", Double.class);
    public static final YahooField LEVERED_FREE_CASH_FLOW = create("LEVERED_FREE_CASH_FLOW", Double.class);
    public static final YahooField ADV_3MONTH = create("ADV_3MONTH", Double.class);
    public static final YahooField ADV_10DAY = create("ADV_10DAY", Double.class);
    public static final YahooField SHARES_OUTSTANDING = create("SHARES_OUTSTANDING", Double.class);
    public static final YahooField SHARES_FLOAT = create("SHARES_FLOAT", Double.class);
    public static final YahooField OWNER_PERCENT_INSIDER = create("OWNER_PERCENT_INSIDER", Double.class);
    public static final YahooField OWNER_PERCENT_INSTITUTION = create("OWNER_PERCENT_INSTITUTION", Double.class);
    public static final YahooField SHARES_SHORT = create("SHARES_SHORT", Double.class);
    public static final YahooField SHARES_SHORT_RATIO = create("SHARES_SHORT_RATIO", Double.class);
    public static final YahooField SHARES_SHORT_PRIOR = create("SHARES_SHORT_PRIOR", Double.class);
    public static final YahooField DIVIDEND_PAY_DATE = create("DIVIDEND_PAY_DATE", LocalDate.class);
    public static final YahooField DIVIDEND_FWD_RATE = create("DIVIDEND_FWD_RATE", Double.class);
    public static final YahooField DIVIDEND_FWD_YIELD = create("DIVIDEND_FWD_YIELD", Double.class);
    public static final YahooField DIVIDEND_TRAIL_YIELD = create("DIVIDEND_TRAIL_YIELD", Double.class);
    public static final YahooField DIVIDEND_PAYOUT_RATIO = create("DIVIDEND_PAYOUT_RATIO", Double.class);
    public static final YahooField DIVIDEND_EX_DATE = create("DIVIDEND_EX_DATE", LocalDate.class);
    public static final YahooField LAST_SPLIT_DATE = create("LAST_SPLIT_DATE", LocalDate.class);
    public static final YahooField EX_DIVIDEND_DATE = create("EX_DIVIDEND_DATE", LocalDate.class);
    public static final YahooField PRICE_EARNINGS_RATIO = create("PRICE_EARNINGS_RATIO", Double.class);
    public static final YahooField SHORT_RATIO = create("SHORT_RATIO", Double.class);



    private String name;
    private Object nullValue;
    private Class<?> dataType;


    /**
     * Constructor
     * @param name      the field name
     * @param dataType  the field data type
     * @param nullValue the field null value
     */
    public YahooField(String name, Class<?> dataType, Object nullValue) {
        if (fieldMap.containsKey(name)) {
            throw new IllegalArgumentException("Field with name already registered: " + name);
        } else {
            this.name = name;
            this.dataType = dataType;
            this.nullValue = nullValue;
        }
    }

    /**
     * Returns the field for the name speciied
     * @param name  the field name
     * @return      the field match
     */
    public static YahooField getField(String name) {
        return fieldMap.get(name);
    }

    /**
     * Returns the name for this field
     * @return  the field name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the null value for this field
     * @return  the null value
     */
    public Object getNullValue() {
        return nullValue;
    }

    /**
     * Returns the data type for this field
     * @return  the data type for field
     */
    public Class<?> getDataType() {
        return dataType;
    }

    @Override()
    public int hashCode() {
        return name.hashCode();
    }

    @Override()
    public boolean equals(Object other) {
        return other != null && other instanceof YahooField && ((YahooField)other).name.equals(this.name);
    }

    @Override()
    public String toString() {
        return name;
    }

    @Override
    public int compareTo(YahooField other) {
        return getName().compareTo(other.getName());
    }

    /**
     * Caches and returns a newly created QuoteField
     * @param fieldName   the field name
     * @param dataType    the field data type
     * @return            the newly created field
     */
    @SuppressWarnings("unchecked")
    static YahooField create(String fieldName, Class<?> dataType) {
        try {
            final Object nullValue = nullValueMap.get(dataType);
            final YahooField field = new YahooField(fieldName, dataType, nullValue);
            fieldMap.put(fieldName, field);
            return field;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create quote field for " + fieldName, ex);
        }
    }

}

