package com.huobi;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.huobi.client.GenericClient;
import com.huobi.client.MarketClient;
import com.huobi.client.req.market.MarketDetailRequest;
import com.huobi.constant.HuobiOptions;
import com.huobi.model.generic.Symbol;
import com.huobi.model.market.Candlestick;
import com.huobi.model.market.MarketDetail;
import com.huobi.model.market.MarketTicker;
import com.taobao.text.ui.BorderStyle;
import com.taobao.text.ui.Element;
import com.taobao.text.ui.Overflow;
import com.taobao.text.ui.TableElement;
import com.taobao.text.util.RenderUtil;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Chris
 * @version 1.0.0
 * @since 2021/05/22
 */
@Slf4j
public class TestMarketDataWs2 {
    MarketClient marketClient = MarketClient.create(new HuobiOptions());
    GenericClient genericService = GenericClient.create(HuobiOptions.builder().build());
    String symbol = "btcusdt";
    // String symbol = "newusdt";
    private static final String QUOTE_CURRENCY = "usdt";

    @Test
    public void readFile() throws IOException {
        System.out.println("===== readFile =====");
        final String path = this.getClass().getResource("/symbols-20200522.json").getPath();
        final File file = new File(path);
        log.info("file exists {}", file.exists());
        final String json = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    }

    @Test
    public void test1() throws InterruptedException, IOException {

        // 交易对
        final InputStream symbolInputStream = this.getClass().getResourceAsStream("/symbols-20200522.json");
        assert symbolInputStream != null;
        final String symbolsJson = IOUtils.toString(symbolInputStream, StandardCharsets.UTF_8);
        // final String symbolsJson = FileUtils.readFileToString(new File(symbolPath), StandardCharsets.UTF_8);
        final List<Symbol> symbols = JSON.parseObject(symbolsJson, new TypeReference<List<Symbol>>() {
        }.getType());

        // ["btcusdt","newusdt",...]
        final Set<String> symbolSet = symbols.stream()
                .sorted(Comparator.comparing(Symbol::getQuoteCurrency))
                // 过滤 usdt 币种对应的交易对
                .filter(s -> StringUtils.equalsIgnoreCase(QUOTE_CURRENCY, s.getQuoteCurrency()))
                .filter(s -> StringUtils.isBlank(s.getUnderlying()))
                // 聚合 usdt 交易对
                .map(Symbol::getSymbol)
                .collect(Collectors.toSet());

        // 最新
        // final List<MarketTicker> tickers = marketClient.getTickers();

        final InputStream tickerInputStream = this.getClass().getResourceAsStream("/tickers-20200522.json");
        assert tickerInputStream != null;
        final String tickerJson = IOUtils.toString(tickerInputStream, StandardCharsets.UTF_8);
        // final String tickerJson = FileUtils.readFileToString(tickerInputStream, StandardCharsets.UTF_8);
        final List<MarketTicker> tickers = JSON.parseObject(tickerJson, new TypeReference<List<MarketTicker>>() {
        }.getType());

        final List<MarketTicker> newTickers = tickers.stream()
                .filter(marketTicker -> symbolSet.contains(marketTicker.getSymbol()))
                .filter(marketTicker -> marketTicker.getOpen().compareTo(new BigDecimal("0.1")) <= 0)
                .collect(Collectors.toList());

        // final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        // scheduledExecutorService.scheduleAtFixedRate(() -> {
        //     MarketDetail marketDetail = marketClient.getMarketDetail(MarketDetailRequest.builder().symbol(symbol).build());
        //     log.info(marketDetail);
        //     // cal(marketDetailMerged.getOpen(), marketDetailMerged.getClose());
        //     // cal(marketDetailMerged.getLow(), marketDetailMerged.getHigh());
        //
        //     // (1 + 涨幅)x = 0.001085
        //     cal(marketDetail.getLow(), marketDetail.getClose());
        //
        // }, 0, 1000L, TimeUnit.MILLISECONDS);

        // 涨幅榜
        final List<Increment> increments = Flowable.fromIterable(newTickers).parallel()
                .runOn(Schedulers.io())
                .flatMap(
                        marketTicker -> Flowable.fromCallable(() -> {
                            // MarketDetailMerged marketDetailMerged = marketClient.getMarketDetailMerged(MarketDetailMergedRequest.builder()
                            //         .symbol(marketTicker.getSymbol()).build());
                            MarketDetail marketDetail = marketClient.getMarketDetail(MarketDetailRequest.builder()
                                    .symbol(marketTicker.getSymbol()).build());
                            // 当前涨幅
                            // log.info("thread {}", Thread.currentThread());
                            final BigDecimal change = this.cal(marketTicker.getOpen(), marketDetail.getClose());
                            return Increment.builder()
                                    .symbol(marketTicker.getSymbol())
                                    .open(marketTicker.getOpen())
                                    .close(marketDetail.getClose())
                                    .low(marketDetail.getLow())
                                    .high(marketDetail.getHigh())
                                    .change(change)
                                    .build();
                        })//.observeOn(Schedulers.newThread())
                )
                // .groupBy()
                .toSortedList((increment1, increment2) -> increment2.getChange().compareTo(increment1.getChange()))
                .blockingSingle();//.stream().limit(10).collect(Collectors.toList());

        // marketClient.subMarketDetail(SubMarketDetailRequest.builder().symbol(symbol).build(), (marketDetailEvent) -> {
        //     // log.info(marketDetailEvent.toString());
        //     final MarketDetail marketDetail = marketDetailEvent.getDetail();
        //     log.info(marketDetail);
        //     // (1 + 涨幅)x = 0.001085
        //     // cal(marketDetail.getOpen(), marketDetail.getClose());
        // });
        printTable(increments, QUOTE_CURRENCY);

        TimeUnit.MINUTES.sleep(10);
    }

    private void printTable(List<Increment> increments, String baseCurrent) {
        // header定义
        // String[] fields = {"币种", "涨幅", "开盘价", "收盘价", "最高价", "最低价"};
        String[] fields = {"Currency", "Change", "Open", "Close", "High", "Low"};

        // 设置两列的比例是1:1，如果不设置的话，列宽是自动按元素最长的处理。
        // 设置table的外部边框，默认是没有外边框
        // 还有内部的分隔线，默认内部没有分隔线
        // TableElement tableElement = new TableElement(1, 1).border(BorderStyle.DASHED).separator(BorderStyle.DASHED);
        TableElement tableElement = new TableElement(1, 1, 1, 1, 1, 1).border(BorderStyle.DASHED).separator(BorderStyle.DASHED);

        // 设置单元格的左右边框间隔，默认是没有，看起来会有点挤，空间足够时，可以设置为1，看起来清爽
        tableElement.leftCellPadding(1).rightCellPadding(1);

        // 设置header
        tableElement.row(true, fields);

        // 设置cell里的元素超出了处理方式，Overflow.HIDDEN 表示隐藏
        // Overflow.WRAP表示会向外面排出去，即当输出宽度有限时，右边的列可能会显示不出，被挤掉了
        tableElement.overflow(Overflow.HIDDEN);

        // 设置第一列输出字体蓝色，红色背景
        // 设置第二列字体加粗，加下划线
        for (Increment increment : increments) {
            String currency = StringUtils.replace(increment.getSymbol(), baseCurrent, "");//.toUpperCase();
            tableElement.add(
                    Element.row()
                            .add(Element.label(currency))
                            .add(Element.label(increment.getChange().toString()))
                            .add(Element.label(increment.getOpen().toString()))
                            .add(Element.label(increment.getClose().toString()))
                            .add(Element.label(increment.getHigh().toString()))
                            .add(Element.label(increment.getLow().toString()))
            );
        }

        // 默认输出宽度是80
        System.err.println(RenderUtil.render(tableElement, 120));
        final List<String> symbols = increments.stream().map(Increment::getSymbol).collect(Collectors.toList());
        log.info("top 10 symbols {}", JSON.toJSONString(symbols));
    }

    private BigDecimal cal(BigDecimal open, BigDecimal close) {
        // 涨幅 = (最新价 - 开盘价) / 开盘价
        BigDecimal result = close.subtract(open)
                .divide(open, 5, BigDecimal.ROUND_HALF_UP)
                .multiply(new BigDecimal("100.0"));
        // log.info("NEW\t\t" + open + "\t\t" + close + "\t\t" + result + "%");
        return result;
    }

    @Test
    public void test2() {
        // final List<Candlestick> candlestick = marketClient.getCandlestick(CandlestickRequest.builder()
        //         .symbol(symbol)
        //         .interval(CandlestickIntervalEnum.MIN1)
        //         .size(10)
        //         .build());
        // log.info(JSON.toJSONString(candlestick));

        final List<Candlestick> candlestick = JSON.parseObject(json, new TypeReference<List<Candlestick>>() {
        }.getType());
        System.err.println(RenderUtil.render(candlestick));
    }

    @Test
    public void test3() {
        // final List<Candlestick> candlestick = marketClient.getCandlestick(CandlestickRequest.builder()
        //         .symbol(symbol)
        //         .interval(CandlestickIntervalEnum.MIN1)
        //         .size(10)
        //         .build());
        // log.info(JSON.toJSONString(candlestick));

        final List<Candlestick> candlesticks = JSON.parseObject(json, new TypeReference<List<Candlestick>>() {
        }.getType());
        // header定义
        // String[] fields = {"交易次数", "交易量", "开盘价", "收盘价", "最高价", "最低价"};
        String[] fields = {"Count", "Amount", "Open", "Close", "High", "Low"};

        // 设置两列的比例是1:1，如果不设置的话，列宽是自动按元素最长的处理。
        // 设置table的外部边框，默认是没有外边框
        // 还有内部的分隔线，默认内部没有分隔线
        // TableElement tableElement = new TableElement(1, 1).border(BorderStyle.DASHED).separator(BorderStyle.DASHED);
        TableElement tableElement = new TableElement(1, 1, 1, 1, 1, 1).border(BorderStyle.DASHED).separator(BorderStyle.DASHED);

        // 设置单元格的左右边框间隔，默认是没有，看起来会有点挤，空间足够时，可以设置为1，看起来清爽
        tableElement.leftCellPadding(1).rightCellPadding(1);

        // 设置header
        tableElement.row(true, fields);

        // 设置cell里的元素超出了处理方式，Overflow.HIDDEN 表示隐藏
        // Overflow.WRAP表示会向外面排出去，即当输出宽度有限时，右边的列可能会显示不出，被挤掉了
        tableElement.overflow(Overflow.HIDDEN);

        // 设置第一列输出字体蓝色，红色背景
        // 设置第二列字体加粗，加下划线
        for (Candlestick candlestick : candlesticks) {
            tableElement.add(
                    Element.row()
                            .add(Element.label(candlestick.getCount().toString()))
                            .add(Element.label(candlestick.getAmount().toString()))
                            .add(Element.label(candlestick.getOpen().toString()))
                            .add(Element.label(candlestick.getClose().toString()))
                            .add(Element.label(candlestick.getHigh().toString()))
                            .add(Element.label(candlestick.getLow().toString()))
            );
        }

        // 默认输出宽度是80
        System.err.println(RenderUtil.render(tableElement));
    }

    private static final String json = "[{\"amount\":0.7285207952137226,\"close\":38225.97,\"count\":32,\"high\":38237.85,\"id\":1621685340,\"low\":38225.94,\"open\":38225.94,\"vol\":27849.8696346},{\"amount\":13.86479735175163,\"close\":38225.95,\"count\":806,\"high\":38331.2,\"id\":1621685280,\"low\":38225.87,\"open\":38248.49,\"vol\":530815.0700686837},{\"amount\":11.44865938126004,\"close\":38248.49,\"count\":677,\"high\":38272.64,\"id\":1621685220,\"low\":38224.36,\"open\":38255.01,\"vol\":437854.9057539088},{\"amount\":16.198623302110377,\"close\":38255.01,\"count\":961,\"high\":38308.88,\"id\":1621685160,\"low\":38240.0,\"open\":38292.54,\"vol\":619969.02170284},{\"amount\":56.672735004236,\"close\":38292.54,\"count\":1560,\"high\":38418.58,\"id\":1621685100,\"low\":38280.48,\"open\":38393.32,\"vol\":2174051.299310616},{\"amount\":16.72168324291044,\"close\":38393.32,\"count\":977,\"high\":38416.33,\"id\":1621685040,\"low\":38369.79,\"open\":38416.33,\"vol\":641909.40271781},{\"amount\":44.58692753424356,\"close\":38414.57,\"count\":2216,\"high\":38444.38,\"id\":1621684980,\"low\":38330.0,\"open\":38342.99,\"vol\":1712069.0645322797},{\"amount\":39.563341578980854,\"close\":38343.68,\"count\":1047,\"high\":38346.88,\"id\":1621684920,\"low\":38266.74,\"open\":38284.04,\"vol\":1514903.2634477601},{\"amount\":37.71984178814462,\"close\":38284.04,\"count\":1502,\"high\":38359.88,\"id\":1621684860,\"low\":38239.88,\"open\":38270.36,\"vol\":1444750.57641508},{\"amount\":62.92402245497439,\"close\":38270.36,\"count\":2045,\"high\":38310.01,\"id\":1621684800,\"low\":38144.49,\"open\":38144.49,\"vol\":2405941.009860827}]";

    /**
     * 涨幅榜
     */
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    static class Increment {
        /** 币种 */
        private String coin;
        /** 交易对 */
        private String symbol;
        /** 涨幅 */
        private BigDecimal change;
        /** 开盘价 */
        private BigDecimal open;
        /** 最新价格 */
        private BigDecimal close;
        /** 最低价 */
        private BigDecimal low;
        /** 最高价 */
        private BigDecimal high;
    }

    // public void test2() {
    //     log.info("===== 舰桥 =====");
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