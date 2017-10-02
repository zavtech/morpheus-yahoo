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

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parser that can parse string content from Yahoo Finance into an appropriate Java type.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooFinanceParser {

    private static final Map<String,Integer> monthMap = new HashMap<>();

    private Matcher numberMatcher1 = Pattern.compile("^\\+?(-?\\d*\\.?\\d*)([KMBT]?)$", Pattern.CASE_INSENSITIVE).matcher("");
    private Matcher numberMatcher2 = Pattern.compile("^\\+?(-?[\\d,]*\\.?\\d*)$", Pattern.CASE_INSENSITIVE).matcher("");
    private Matcher percentMatcher = Pattern.compile("^\\+?(-?\\d*\\.?\\d*)%$", Pattern.CASE_INSENSITIVE).matcher("");
    private Matcher dateMatcher1 = Pattern.compile("^([\\p{Alpha}]{3})\\s([0-9]{1,2})$", Pattern.CASE_INSENSITIVE).matcher("");
    private Matcher dateMatcher2 = Pattern.compile("^([\\p{Alpha}]{3})\\s([0-9]{1,2}),\\s([0-9]{4})$", Pattern.CASE_INSENSITIVE).matcher("");
    private Matcher dateMatcher3 = Pattern.compile("^(\\d+)/(\\d+)/(\\d+)$", Pattern.CASE_INSENSITIVE).matcher("");
    private Matcher timeMatcher1 = Pattern.compile("(\\d+):(\\d+)(am|pm)$", Pattern.CASE_INSENSITIVE).matcher("");

    /**
     * Static initializer
     */
    static {
        monthMap.put("jan", 1);
        monthMap.put("feb", 2);
        monthMap.put("mar", 3);
        monthMap.put("apr", 4);
        monthMap.put("may", 5);
        monthMap.put("jun", 6);
        monthMap.put("jul", 7);
        monthMap.put("aug", 8);
        monthMap.put("sep", 9);
        monthMap.put("oct", 10);
        monthMap.put("nov", 11);
        monthMap.put("dec", 12);
    }

    /**
     * Attempts to parse the string into another type
     * @param value     the value to parse
     * @return          the parsed value
     */
    public Object parse(String value) {
        try {
            if (value == null) return null;
            else if (value.equalsIgnoreCase("-")) return null;
            else if (value.equalsIgnoreCase("N/A")) return null;
            else if (value.equalsIgnoreCase("NaN")) return null;
            else if (numberMatcher1.reset(value.replace("," , "")).matches()) {
                final String token1 = numberMatcher1.group(1);
                final String token2 = numberMatcher1.group(2);
                final double number = Double.parseDouble(token1);
                if (token2 == null || token2.length() == 0) return number;
                else if (token2.equalsIgnoreCase("K")) return number * 1000d;
                else if (token2.equalsIgnoreCase("M")) return number * 1000000d;
                else if (token2.equalsIgnoreCase("B")) return number * 1000000000d;
                else if (token2.equalsIgnoreCase("T")) return number * 1000000000000d;
                else return number;
            } else if (numberMatcher2.reset(value.replace("," , "")).matches()) {
                final String token = numberMatcher2.group(1);
                final String stripped = token.replace(",", "");
                return Double.parseDouble(stripped);
            } else if (percentMatcher.reset(value.replace("," , "")).matches()) {
                final String token = percentMatcher.group(1);
                final double number = Double.parseDouble(token);
                return number / 100d;
            } else if (dateMatcher1.reset(value).matches()) {
                final String monthString = dateMatcher1.group(1);
                final String dateString = dateMatcher1.group(2);
                final int year = LocalDate.now().getYear();
                final Integer month = monthMap.get(monthString.toLowerCase());
                if (month == null) throw new RuntimeException("Unsupported month: " + monthString);
                return LocalDate.of(year, month, Integer.parseInt(dateString));
            } else if (dateMatcher2.reset(value).matches()) {
                final String monthString = dateMatcher2.group(1);
                final String dateString = dateMatcher2.group(2);
                final int year = Integer.parseInt(dateMatcher2.group(3));
                final Integer month = monthMap.get(monthString.toLowerCase());
                if (month == null) throw new RuntimeException("Unsupported month $monthString");
                return LocalDate.of(year, month, Integer.parseInt(dateString));
            } else if (dateMatcher3.reset(value).matches()) {
                final String monthString = dateMatcher3.group(1);
                final String dateString = dateMatcher3.group(2);
                final int year = Integer.parseInt(dateMatcher3.group(3));
                final int month = Integer.parseInt(monthString);
                return LocalDate.of(year, month, Integer.parseInt(dateString));
            } else if (timeMatcher1.reset(value).matches()) {
                final int hour = Integer.parseInt(timeMatcher1.group(1));
                final int minutes = Integer.parseInt(timeMatcher1.group(2));
                final boolean am = timeMatcher1.group(3).equalsIgnoreCase("am");
                if (am) {
                    switch (hour) {
                        case 12:    return LocalTime.of(0, minutes);
                        default:    return LocalTime.of(hour, minutes);
                    }
                } else {
                    switch (hour) {
                        case 12:    return LocalTime.of(hour, minutes);
                        default:    return LocalTime.of(12 + hour, minutes);
                    }
                }
            } else {
                return value;
            }
        } catch (Throwable ex) {
            throw new YahooException("Failed to parse value: " + value, ex);
        }
    }


    /**
     * Returns a double value parsed from the string
     * @param value     the text value
     * @return          the double value
     */
    public double parseDouble(String value) {
        if (value == null) {
            return Double.NaN;
        } else {
            value = value.trim();
            if (value.length() == 0 || value.equalsIgnoreCase("N/A") || value.equalsIgnoreCase("-")) {
                return Double.NaN;
            } else {
                final Object result = parse(value);
                if (result instanceof Number) {
                    return ((Number) result).doubleValue();
                } else {
                    throw new RuntimeException("Failed to parse value into double, returned result: " + value);
                }
            }
        }
    }

}
