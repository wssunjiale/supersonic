package com.tencent.supersonic.headless.core.translator.parser;

import com.tencent.supersonic.common.jsqlparser.SqlReplaceHelper;
import com.tencent.supersonic.common.jsqlparser.SqlSelectHelper;
import com.tencent.supersonic.headless.api.pojo.Measure;
import com.tencent.supersonic.headless.api.pojo.enums.MetricDefineType;
import com.tencent.supersonic.headless.api.pojo.response.MetricSchemaResp;
import com.tencent.supersonic.headless.api.pojo.response.SemanticSchemaResp;
import com.tencent.supersonic.headless.core.pojo.OntologyQuery;
import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import com.tencent.supersonic.headless.core.pojo.SqlQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * This parser replaces metric bizName in the S2SQL with calculation expression (if configured).
 */
@Component("MetricExpressionParser")
@Slf4j
public class MetricExpressionParser implements QueryParser {
    @Override
    public boolean accept(QueryStatement queryStatement) {
        return Objects.nonNull(queryStatement.getSqlQuery())
                && Objects.nonNull(queryStatement.getOntologyQuery())
                && StringUtils.isNotBlank(queryStatement.getSqlQuery().getSql())
                && !CollectionUtils.isEmpty(queryStatement.getOntologyQuery().getMetrics());
    }

    @Override
    public void parse(QueryStatement queryStatement) throws Exception {

        SemanticSchemaResp semanticSchema = queryStatement.getSemanticSchema();
        SqlQuery sqlQuery = queryStatement.getSqlQuery();
        OntologyQuery ontologyQuery = queryStatement.getOntologyQuery();

        Map<String, String> bizName2Expr = getMetricExpressions(semanticSchema, ontologyQuery);
        if (!CollectionUtils.isEmpty(bizName2Expr)) {
            String sql = SqlReplaceHelper.replaceSqlByExpression(sqlQuery.getTable(),
                    sqlQuery.getSql(), bizName2Expr);
            sqlQuery.setSql(sql);
        }
    }

    private Map<String, String> getMetricExpressions(SemanticSchemaResp semanticSchema,
            OntologyQuery ontologyQuery) {

        List<MetricSchemaResp> allMetrics = semanticSchema.getMetrics();
        Set<MetricSchemaResp> queryMetrics = ontologyQuery.getMetrics();
        Set<String> queryFields = ontologyQuery.getFields();
        log.debug("begin to generateDerivedMetric {} [{}]", queryMetrics);

        Set<String> allFields = new HashSet<>();
        Map<String, Measure> allMeasures = new HashMap<>();
        semanticSchema.getModelResps().forEach(modelResp -> {
            allFields.addAll(modelResp.getFieldList());
            if (modelResp.getModelDetail().getMeasures() != null) {
                modelResp.getModelDetail().getMeasures().forEach(measure -> {
                    if (StringUtils.isBlank(measure.getBizName())) {
                        return;
                    }
                    allMeasures.put(measure.getBizName(), measure);
                    allMeasures.put(measure.getBizName().toLowerCase(Locale.ROOT), measure);
                });
            }
        });

        Map<String, String> visitedMetrics = new HashMap<>();
        Map<String, String> metric2Expr = new HashMap<>();
        for (MetricSchemaResp queryMetric : queryMetrics) {
            String fieldExpr = buildFieldExpr(allMetrics, allMeasures, queryMetric.getExpr(),
                    queryMetric.getMetricDefineType(), visitedMetrics);
            // add all fields referenced in the expression
            queryMetric.getFields().addAll(SqlSelectHelper.getFieldsFromExpr(fieldExpr));
            queryFields.addAll(queryMetric.getFields());
            if (!queryMetric.getBizName().equals(fieldExpr)) {
                metric2Expr.put(queryMetric.getBizName(), fieldExpr);
            }
        }

        return metric2Expr;
    }

    private String buildFieldExpr(final List<MetricSchemaResp> metricResps,
            final Map<String, Measure> allMeasures, final String metricExpr,
            final MetricDefineType metricDefineType, Map<String, String> visitedMetric) {
        Set<String> fields = SqlSelectHelper.getFieldsFromExpr(metricExpr);
        if (!CollectionUtils.isEmpty(fields)) {
            Map<String, String> replace = new HashMap<>();
            for (String field : fields) {
                switch (metricDefineType) {
                    case METRIC:
                        // if defineType=METRIC, field should be the bizName of its parent metric
                        Optional<MetricSchemaResp> metricItem = metricResps.stream()
                                .filter(m -> m.getBizName().equalsIgnoreCase(field)).findFirst();
                        if (metricItem.isPresent()) {
                            if (visitedMetric.keySet().contains(field)) {
                                replace.put(field, visitedMetric.get(field));
                                break;
                            }
                            replace.put(field,
                                    buildFieldExpr(metricResps, allMeasures,
                                            metricItem.get().getExpr(),
                                            metricItem.get().getMetricDefineType(), visitedMetric));
                            visitedMetric.put(field, replace.get(field));
                        }
                        break;
                    case MEASURE:
                        // if defineType=MEASURE, field should be the bizName of its measure
                        Measure measure = getMeasure(allMeasures, field);
                        if (measure != null) {
                            replace.put(field, buildMeasureExpr(measure, true));
                        }
                        break;
                    case FIELD:
                        // if defineType=FIELD, field should be the physical field name; however,
                        // some
                        // expressions may still reference measure bizName (e.g. from UI tokens). In
                        // that
                        // case, replace it with its physical expression without adding aggregation.
                        Measure measureInField = getMeasure(allMeasures, field);
                        if (measureInField != null) {
                            replace.put(field, buildMeasureExpr(measureInField, false));
                        }
                        break;
                    default:
                        break;
                }
            }
            if (!CollectionUtils.isEmpty(replace)) {
                String expr = SqlReplaceHelper.replaceExpression(metricExpr, replace);
                log.debug("derived metric {}->{}", metricExpr, expr);
                return expr;
            }
        }
        return metricExpr;
    }

    private Measure getMeasure(Map<String, Measure> allMeasures, String field) {
        if (allMeasures == null || StringUtils.isBlank(field)) {
            return null;
        }
        Measure measure = allMeasures.get(field);
        if (measure != null) {
            return measure;
        }
        return allMeasures.get(field.toLowerCase(Locale.ROOT));
    }

    /**
     * Build the physical expression for a measure. When {@code withAgg} is true, use the measure's
     * aggregation function (if configured) on top of {@code measure.expr}. Note: measure.bizName is
     * a semantic identifier and may not exist as a physical column name.
     */
    private String buildMeasureExpr(Measure measure, boolean withAgg) {
        if (measure == null) {
            return "";
        }
        String physicalExpr = StringUtils.defaultIfBlank(measure.getExpr(), measure.getBizName());
        physicalExpr = String.format("(%s)", physicalExpr);
        if (withAgg && StringUtils.isNotBlank(measure.getAgg())) {
            return String.format("%s(%s)", measure.getAgg().trim(), physicalExpr);
        }
        return physicalExpr;
    }

}
