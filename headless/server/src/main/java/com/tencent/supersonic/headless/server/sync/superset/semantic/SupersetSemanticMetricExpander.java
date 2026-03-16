package com.tencent.supersonic.headless.server.sync.superset.semantic;

import com.tencent.supersonic.headless.api.pojo.MetricParam;
import com.tencent.supersonic.headless.api.pojo.enums.MetricDefineType;
import com.tencent.supersonic.headless.api.pojo.response.MetricResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SupersetSemanticMetricExpander {

    private static final Pattern AGGREGATE_FUNCTION_PATTERN = Pattern.compile(
            "^\\s*(?i)(sum|count|avg|min|max|count_distinct|approx_count_distinct)\\s*\\(");

    public String expandMetricExpression(MetricResp metric,
            Map<String, MetricResp> metricsByBizNameLowerCase) {
        return expandMetricExpression(metric, metricsByBizNameLowerCase, new java.util.HashSet<>());
    }

    private String expandMetricExpression(MetricResp metric,
            Map<String, MetricResp> metricsByBizNameLowerCase, Set<String> visiting) {
        if (metric == null) {
            return null;
        }
        String bizName = StringUtils.trimToNull(metric.getBizName());
        if (bizName != null) {
            String key = bizName.toLowerCase(Locale.ROOT);
            if (!visiting.add(key)) {
                log.warn("superset semantic metric cycle detected, bizName={}", bizName);
                return StringUtils.trimToNull(metric.getExpr());
            }
        }
        try {
            MetricDefineType defineType = metric.getMetricDefineType();
            String expr = StringUtils.trimToNull(metric.getExpr());
            if (expr == null) {
                return null;
            }
            if (!MetricDefineType.METRIC.equals(defineType)) {
                return applyDefaultAgg(expr, metric);
            }
            if (metric.getMetricDefineByMetricParams() == null
                    || metric.getMetricDefineByMetricParams().getMetrics() == null) {
                return expr;
            }
            for (MetricParam dep : metric.getMetricDefineByMetricParams().getMetrics()) {
                String depBizName = dep == null ? null : StringUtils.trimToNull(dep.getBizName());
                if (depBizName == null) {
                    continue;
                }
                MetricResp depMetric =
                        metricsByBizNameLowerCase.get(depBizName.toLowerCase(Locale.ROOT));
                if (depMetric == null) {
                    continue;
                }
                String depExpr =
                        expandMetricExpression(depMetric, metricsByBizNameLowerCase, visiting);
                if (StringUtils.isBlank(depExpr)) {
                    continue;
                }
                expr = replaceIdentifier(expr, depBizName, "(" + depExpr + ")");
            }
            return expr;
        } finally {
            if (bizName != null) {
                visiting.remove(bizName.toLowerCase(Locale.ROOT));
            }
        }
    }

    private String applyDefaultAgg(String expr, MetricResp metric) {
        if (StringUtils.isBlank(expr) || metric == null) {
            return StringUtils.trimToNull(expr);
        }
        if (AGGREGATE_FUNCTION_PATTERN.matcher(expr).find()) {
            return expr;
        }
        String agg = resolveDefaultAgg(metric);
        if (StringUtils.isBlank(agg)) {
            return expr;
        }
        return agg.toUpperCase(Locale.ROOT) + "(" + expr + ")";
    }

    private String resolveDefaultAgg(MetricResp metric) {
        if (metric == null) {
            return null;
        }
        String agg = StringUtils.trimToNull(metric.getDefaultAgg());
        if (StringUtils.isNotBlank(agg)) {
            return agg;
        }
        if (metric.getMetricDefineByMeasureParams() == null
                || metric.getMetricDefineByMeasureParams().getMeasures() == null) {
            return null;
        }
        return metric.getMetricDefineByMeasureParams().getMeasures().stream()
                .map(item -> item == null ? null : StringUtils.trimToNull(item.getAgg()))
                .filter(StringUtils::isNotBlank).findFirst().orElse(null);
    }

    /**
     * Replace an identifier token (case-insensitive) in an SQL expression. This avoids accidental
     * substring matches (e.g. aov should not match aov_x).
     */
    private String replaceIdentifier(String expr, String identifier, String replacement) {
        if (StringUtils.isBlank(expr) || StringUtils.isBlank(identifier) || replacement == null) {
            return expr;
        }
        String escaped = Pattern.quote(identifier);
        Pattern pattern = Pattern.compile("(?i)(?<![A-Za-z0-9_])" + escaped + "(?![A-Za-z0-9_])");
        Matcher matcher = pattern.matcher(expr);
        return matcher.replaceAll(Matcher.quoteReplacement(replacement));
    }
}
