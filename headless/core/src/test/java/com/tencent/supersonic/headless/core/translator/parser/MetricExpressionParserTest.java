package com.tencent.supersonic.headless.core.translator.parser;

import com.tencent.supersonic.headless.api.pojo.Measure;
import com.tencent.supersonic.headless.api.pojo.enums.MetricDefineType;
import com.tencent.supersonic.headless.api.pojo.response.MetricSchemaResp;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MetricExpressionParserTest {

    @Test
    public void testMeasureDefineReplacesBizNameWithPhysicalExpr() throws Exception {
        MetricExpressionParser parser = new MetricExpressionParser();
        Map<String, Measure> measures = buildMeasures();

        String expr = invokeBuildFieldExpr(parser, List.of(), measures,
                "fact_internet_sales_sales_amount", MetricDefineType.MEASURE);

        Assert.assertTrue(expr.toUpperCase(Locale.ROOT).contains("SUM"),
                "Expected measure aggregation to be applied");
        Assert.assertTrue(expr.contains("sales_amount"),
                "Expected physical column name to be used");
        Assert.assertFalse(expr.contains("fact_internet_sales_sales_amount"),
                "Measure bizName should not be treated as a physical column");
    }

    @Test
    public void testMeasureDefineHandlesMultipleMeasures() throws Exception {
        MetricExpressionParser parser = new MetricExpressionParser();
        Map<String, Measure> measures = buildMeasures();

        String expr = invokeBuildFieldExpr(parser, List.of(), measures,
                "fact_internet_sales_sales_amount / fact_internet_sales_order_quantity",
                MetricDefineType.MEASURE);

        Assert.assertTrue(expr.contains("/"), "Expected operator to be preserved");
        Assert.assertTrue(expr.contains("sales_amount"), "Expected numerator physical expr");
        Assert.assertTrue(expr.contains("order_quantity"), "Expected denominator physical expr");
        Assert.assertFalse(expr.contains("fact_internet_sales_sales_amount"),
                "Measure bizName should not leak into final expr");
        Assert.assertFalse(expr.contains("fact_internet_sales_order_quantity"),
                "Measure bizName should not leak into final expr");
    }

    @Test
    public void testFieldDefineReplacesMeasureBizNameWithoutExtraAgg() throws Exception {
        MetricExpressionParser parser = new MetricExpressionParser();
        Map<String, Measure> measures = buildMeasures();

        String expr = invokeBuildFieldExpr(parser, List.of(), measures,
                "SUM(fact_internet_sales_sales_amount)", MetricDefineType.FIELD);

        Assert.assertTrue(expr.toUpperCase(Locale.ROOT).startsWith("SUM("),
                "Expected existing aggregation to be kept");
        Assert.assertTrue(expr.contains("sales_amount"),
                "Expected measure bizName to be replaced with physical expr");
        Assert.assertFalse(expr.toUpperCase(Locale.ROOT).contains("SUM(SUM"),
                "Should not introduce nested SUM() in FIELD mode");
    }

    @Test
    public void testMetricDefineRecursivelyExpandsToPhysicalExpr() throws Exception {
        MetricExpressionParser parser = new MetricExpressionParser();
        Map<String, Measure> measures = buildMeasures();

        MetricSchemaResp metric = new MetricSchemaResp();
        metric.setBizName("total_sales_amount");
        metric.setMetricDefineType(MetricDefineType.MEASURE);
        metric.setMetricDefineByMeasureParams(
                new com.tencent.supersonic.headless.api.pojo.MetricDefineByMeasureParams());
        metric.getMetricDefineByMeasureParams().setExpr("fact_internet_sales_sales_amount");

        String expr = invokeBuildFieldExpr(parser, List.of(metric), measures, "total_sales_amount",
                MetricDefineType.METRIC);

        Assert.assertTrue(expr.contains("sales_amount"), "Expected physical expr after recursion");
        Assert.assertFalse(expr.contains("fact_internet_sales_sales_amount"),
                "Measure bizName should not leak into final expr");
    }

    @Test
    public void testMeasureDefineFallsBackToAliasWhenExprMissing() throws Exception {
        MetricExpressionParser parser = new MetricExpressionParser();
        Map<String, Measure> measures = buildMeasures();

        // simulate legacy / buggy metadata where expr is missing but alias is present
        Measure broken = new Measure();
        broken.setBizName("fact_internet_sales_sales_amount_missing_expr");
        broken.setExpr("");
        broken.setAlias("sales_amount");
        broken.setAgg("SUM");
        measures.put(broken.getBizName(), broken);
        measures.put(broken.getBizName().toLowerCase(Locale.ROOT), broken);

        String expr = invokeBuildFieldExpr(parser, List.of(), measures,
                "fact_internet_sales_sales_amount_missing_expr", MetricDefineType.MEASURE);

        Assert.assertTrue(expr.contains("sales_amount"),
                "Expected alias to be used as physical expr fallback");
        Assert.assertFalse(expr.contains("fact_internet_sales_sales_amount_missing_expr"),
                "Measure bizName should not leak into final expr even when expr is missing");
    }

    private static Map<String, Measure> buildMeasures() {
        Map<String, Measure> measures = new HashMap<>();

        Measure salesAmount = new Measure();
        salesAmount.setBizName("fact_internet_sales_sales_amount");
        salesAmount.setExpr("sales_amount");
        salesAmount.setAgg("SUM");
        measures.put(salesAmount.getBizName(), salesAmount);
        measures.put(salesAmount.getBizName().toLowerCase(Locale.ROOT), salesAmount);

        Measure orderQty = new Measure();
        orderQty.setBizName("fact_internet_sales_order_quantity");
        orderQty.setExpr("order_quantity");
        orderQty.setAgg("SUM");
        measures.put(orderQty.getBizName(), orderQty);
        measures.put(orderQty.getBizName().toLowerCase(Locale.ROOT), orderQty);

        return measures;
    }

    private static String invokeBuildFieldExpr(MetricExpressionParser parser,
            List<MetricSchemaResp> metrics, Map<String, Measure> measures, String metricExpr,
            MetricDefineType defineType) throws Exception {

        Method method = MetricExpressionParser.class.getDeclaredMethod("buildFieldExpr", List.class,
                Map.class, String.class, MetricDefineType.class, Map.class);
        method.setAccessible(true);
        Object result = method.invoke(parser, metrics, measures, metricExpr, defineType,
                new HashMap<String, String>());
        return (String) result;
    }
}
