package com.huobi;

import com.huobi.client.MarketClient;
import com.huobi.client.req.market.MarketDetailRequest;
import com.huobi.client.req.market.SubMarketDetailRequest;
import com.huobi.constant.HuobiOptions;
import com.huobi.model.market.MarketDetail;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Chris
 * @version 1.0.0
 * @since 2021/05/22
 */
public class TestMarketDataWs {
    @Test
    public void test1() throws InterruptedException {
        MarketClient marketClient = MarketClient.create(new HuobiOptions());

        String symbol = "btcusdt";
        // String symbol = "newusdt";

        System.out.println("币种\t\t开盘价\t\t最新价\t\t涨幅");

        marketClient.subMarketDetail(SubMarketDetailRequest.builder().symbol(symbol).build(), (marketDetailEvent) -> {
            // System.out.println(marketDetailEvent.toString());
            final MarketDetail marketDetail = marketDetailEvent.getDetail();
            System.out.println(marketDetail);
            // (1 + 涨幅)x = 0.001085
            // cal(marketDetail.getOpen(), marketDetail.getClose());
        });

        // List<MarketTicker> tickerList = marketClient.getTickers();
        // tickerList.stream()
        //         .filter(marketTicker -> StringUtils.equalsIgnoreCase(symbol, marketTicker.getSymbol()))
        //         .forEach(marketTicker -> {
        //             // 涨幅 = (最新价 - 开盘价) / 开盘价
        //             cal(marketTicker.getOpen(), marketTicker.getClose());
        //             System.out.println("NEW\t\t" + marketTicker.getClose() + "\t\t" + result + "%");
        //         });

        // final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        // scheduledExecutorService.scheduleAtFixedRate(() -> {
        //     MarketDetailMerged marketDetailMerged = marketClient.getMarketDetailMerged(MarketDetailMergedRequest.builder().symbol(symbol).build());
        //     System.out.println(marketDetailMerged);
        //     // cal(marketDetailMerged.getOpen(), marketDetailMerged.getClose());
        //     // cal(marketDetailMerged.getLow(), marketDetailMerged.getHigh());
        //
        //     // (1 + 涨幅)x = 0.001085
        //     cal(marketDetailMerged.getLow(), marketDetailMerged.getClose());
        //
        // }, 0, 1000L, TimeUnit.MILLISECONDS);

        // final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        // scheduledExecutorService.scheduleAtFixedRate(() -> {
        //     MarketDetail marketDetail = marketClient.getMarketDetail(MarketDetailRequest.builder().symbol(symbol).build());
        //     System.out.println(marketDetail);
        //     // cal(marketDetailMerged.getOpen(), marketDetailMerged.getClose());
        //     // cal(marketDetailMerged.getLow(), marketDetailMerged.getHigh());
        //
        //     // (1 + 涨幅)x = 0.001085
        //     cal(marketDetail.getLow(), marketDetail.getClose());
        //
        // }, 0, 1000L, TimeUnit.MILLISECONDS);

        TimeUnit.MINUTES.sleep(10);
    }

    private void cal(BigDecimal open, BigDecimal close) {
        // 涨幅 = (最新价 - 开盘价) / 开盘价
        BigDecimal result = close.subtract(open)
                .divide(open, 5, BigDecimal.ROUND_HALF_UP)
                .multiply(new BigDecimal("100.0"));
        System.out.println("NEW\t\t" + open + "\t\t" + close + "\t\t" + result + "%");
    }

    // public void test2() {
    //     System.out.println("===== 舰桥 =====");
    //     //加法
    //     BigDecimal result1 = num1.add(num2);
    //     BigDecimal result12 = num12.add(num22);
    //
    //     //减法
    //     BigDecimal result2 = num1.subtract(num2);
    //     BigDecimal result22 = num12.subtract(num22);
    //
    //     //乘法
    //     BigDecimal result3 = num1.multiply(num2);
    //     BigDecimal result32 = num12.multiply(num22);
    //
    //     //绝对值
    //     BigDecimal result4 = num3.abs();
    //     BigDecimal result42 = num32.abs();
    //
    //     //除法
    //     BigDecimal result5 = num2.divide(num1,20,BigDecimal.ROUND_HALF_UP);
    //     BigDecimal result52 = num22.divide(num12,20,BigDecimal.ROUND_HALF_UP);
    // }
}
