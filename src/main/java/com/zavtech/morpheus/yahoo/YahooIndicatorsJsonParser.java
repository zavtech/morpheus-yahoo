package com.zavtech.morpheus.yahoo;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Parses the JSON returned by the Yahoo Finance Quote service,
 * enabling reading stocks metadata and indicators such as
 * open, high, low and close value (between other data).
 *
 * @author Manoel Campos da Silva Filho
 */
public class YahooIndicatorsJsonParser {
    /**
     * The name of the fields representing the indicators in the Yahoo Finance Quote service response.
     */
    public enum Indicator {OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE};

    /**
     * A timestamp array where each item correspond to a given date
     * represented in seconds since 1970, jan 1st.
     */
    private final JsonArray timestamps;

    /**
     * Values for the {@link Indicator}s, except the {@link Indicator#ADJCLOSE}.
     * Each indicator is a property in the JSON object.
     * Each property is an array containing the double value
     * for the indicator (one for each date defined in the {@link #timestamps} array).
     *
     * @see #adjClose
     */
    private final JsonObject quotes;

    /**
     * Values for the {@link Indicator#ADJCLOSE}.
     * It is an array containing for the adjusted close values
     * (one for each date defined in the {@link #timestamps} array).
     */
    private final JsonArray adjClose;

    /**
     * Instantiates the class, parsing the JSON response from a request sent to the Yahoo Finance Quote service.
     * It uses a given input stream to obtain the response data.
     * @param stream the input stream to read the JSON response data
     */
    public YahooIndicatorsJsonParser(final InputStream stream){
        final JsonObject result = parseYahooFinanceResult(new InputStreamReader(stream));
        final JsonObject indicators = result.getAsJsonObject("indicators");

        timestamps = result.getAsJsonArray("timestamp");
        quotes = indicators.getAsJsonArray("quote").get(0).getAsJsonObject();
        adjClose = indicators.getAsJsonArray("adjclose").get(0).getAsJsonObject().getAsJsonArray("adjclose");
    }

    /**
     * Parses a JSON response got from a reader and try to return the JSON object containing
     * the stocks quotes.
     *
     * @param reader the reader to get the JSON String from
     * @return an {@link JsonObject} containing the data for the chart.result JSON field
     *         or an empty object if the result is empty
     */
    private JsonObject parseYahooFinanceResult(final InputStreamReader reader) {
        final JsonElement element = new JsonParser().parse(reader);
        if(!element.isJsonObject()){
            throw new IllegalStateException("The Yahoo Finance response is not a JSON object as expected.");
        }

        try {
            return element
                    .getAsJsonObject()
                    .getAsJsonObject("chart")
                    .getAsJsonArray("result")
                    .get(0)
                    .getAsJsonObject();
        }catch(ArrayIndexOutOfBoundsException|NullPointerException e){
            return new JsonObject();
        }
    }

    /**
     * Gets the quote timestamp at a given position of the timestamp array
     * and converts to a LocalDate value.
     * @param index the desired position in the array
     * @return the quote date
     */
    public LocalDate getDate(final int index){
        return secondsToLocalDate(timestamps.get(index).getAsLong());
    }

    /**
     * Converts a given number of seconds (timestamp) since 1970/jan/01 to LocalDate.
     * This timestamp value is the date format in Yahoo Finance (at least since v8).
     * @param seconds the number of seconds to convert
     * @return a LocalDate representing that number of seconds
     */
    public LocalDate secondsToLocalDate(final long seconds) {
        return LocalDateTime.of(1970, 1, 1, 0, 0).plusSeconds(seconds).toLocalDate();
    }

    /**
     * Gets the value for a specific metric of the stock in a given date,
     * represented by the index of the quotes array.
     * The metric values are
     * @param index the desired position in the array
     * @return the metric value.
     */
    public double getQuote(final Indicator indicator, final int index){
        if(indicator.equals(Indicator.ADJCLOSE)) {
            return getJsonDoubleValue(adjClose.get(index));
        }

        final String metricName = indicator.name().toLowerCase();
        final JsonElement element = quotes.getAsJsonArray(metricName).get(index);
        return getJsonDoubleValue(element);
    }

    private double getJsonDoubleValue(final JsonElement element){
        return element.isJsonNull() ? Double.NaN : element.getAsDouble();
    }

    public boolean isEmpty() {
        return timestamps.size() == 0;
    }

    public int rows(){
        return timestamps.size();
    }
}
