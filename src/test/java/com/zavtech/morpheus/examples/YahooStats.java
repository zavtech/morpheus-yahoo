package com.zavtech.morpheus.examples;

import java.io.File;

import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.util.text.SmartFormat;
import com.zavtech.morpheus.util.text.printer.Printer;
import com.zavtech.morpheus.viz.chart.Chart;
import com.zavtech.morpheus.yahoo.YahooField;
import com.zavtech.morpheus.yahoo.YahooFinance;

/**
 * Examples of using the Yahoo stats api
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class YahooStats {

    @Test()
    public void assetStatistics() throws Exception {
        YahooFinance yahoo = new YahooFinance();
        Array<String> tickers = Array.of("AAPL", "ORCL", "BLK", "GS", "NFLX", "AMZN", "FB");
        DataFrame<String,YahooField> stats = yahoo.getStatistics(tickers);
        stats.transpose().out().print(200, formats -> {
            final SmartFormat smartFormat = new SmartFormat();
            formats.setPrinter(Object.class, Printer.forObject(smartFormat::format));
        });
    }



    @Test()
    public void assetStatistics3() throws Exception {
        YahooFinance yahoo = new YahooFinance();
        Array<String> tickers = Array.of("BLK", "GS", "MS", "JPM", "C", "BAC", "AAPL", "NVDA", "GOOGL");
        DataFrame<String,YahooField> data = yahoo.getStatistics(tickers);
        DataFrame<String,YahooField> stats = data.cols().select(
            YahooField.PROFIT_MARGIN,
            YahooField.OPERATING_MARGIN,
            YahooField.RETURN_ON_ASSETS,
            YahooField.RETURN_ON_EQUITY
        ).applyDoubles(v -> {
            return v.getDouble() * 100d;
        });

        data.out().print();

        Chart.create().withBarPlot(stats.transpose(), false, chart -> {
            chart.title().withText("Profitability & Return Metrics");
            chart.plot().axes().domain().label().withText("Statistic");
            chart.plot().axes().range(0).label().withText("Value");
            chart.plot().axes().range(0).format().withPattern("0.00'%';-0.00'%'");
            chart.legend().right().on();
            chart.writerPng(new File("../morpheus-docs/docs/images/yahoo/asset_stats.png"), 700, 400, true);
            chart.show();
        });

        Thread.currentThread().join();

    }


}
