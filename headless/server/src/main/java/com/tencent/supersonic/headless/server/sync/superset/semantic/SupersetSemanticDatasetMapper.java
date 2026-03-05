package com.tencent.supersonic.headless.server.sync.superset.semantic;

import com.tencent.supersonic.common.jsqlparser.SqlSelectHelper;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.common.util.MD5Util;
import com.tencent.supersonic.headless.api.pojo.MetaFilter;
import com.tencent.supersonic.headless.api.pojo.request.QuerySqlReq;
import com.tencent.supersonic.headless.api.pojo.response.DataSetResp;
import com.tencent.supersonic.headless.api.pojo.response.DatabaseResp;
import com.tencent.supersonic.headless.api.pojo.response.DimensionResp;
import com.tencent.supersonic.headless.api.pojo.response.MetricResp;
import com.tencent.supersonic.headless.api.pojo.response.ModelResp;
import com.tencent.supersonic.headless.api.pojo.response.SemanticTranslateResp;
import com.tencent.supersonic.headless.server.facade.service.SemanticLayerService;
import com.tencent.supersonic.headless.server.service.DataSetService;
import com.tencent.supersonic.headless.server.service.DatabaseService;
import com.tencent.supersonic.headless.server.service.DimensionService;
import com.tencent.supersonic.headless.server.service.MetricService;
import com.tencent.supersonic.headless.server.service.ModelService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SupersetSemanticDatasetMapper {

    private static final int NAME_LIMIT = 250;
    private static final Pattern POSTGRES_CAST_PATTERN =
            Pattern.compile("::\\s*[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern IDENTIFIER_PATTERN =
            Pattern.compile("\\b[A-Za-z_][A-Za-z0-9_]*\\b");
    private static final Set<String> DEPENDENCY_TOKEN_IGNORE = Set.of("as", "and", "or", "not",
            "in", "is", "null", "like", "ilike", "between", "case", "when", "then", "else", "end",
            "distinct", "true", "false", "cast", "coalesce", "nullif", "sum", "count", "avg", "min",
            "max", "upper", "lower", "substring", "substr", "trim", "ltrim", "rtrim", "round",
            "floor", "ceil", "ceiling", "date", "timestamp", "timestamptz", "time", "interval",
            "text", "varchar", "char", "character", "integer", "int", "bigint", "smallint",
            "numeric", "decimal", "double", "float", "real", "boolean");

    private final DataSetService dataSetService;
    private final ModelService modelService;
    private final DatabaseService databaseService;
    private final DimensionService dimensionService;
    private final MetricService metricService;
    private final SemanticLayerService semanticLayerService;
    private final SupersetSemanticMetricExpander metricExpander =
            new SupersetSemanticMetricExpander();

    public SupersetSemanticDatasetMapper(DataSetService dataSetService, ModelService modelService,
            DatabaseService databaseService, DimensionService dimensionService,
            MetricService metricService, SemanticLayerService semanticLayerService) {
        this.dataSetService = dataSetService;
        this.modelService = modelService;
        this.databaseService = databaseService;
        this.dimensionService = dimensionService;
        this.metricService = metricService;
        this.semanticLayerService = semanticLayerService;
    }

    public SemanticDatasetMapping buildMapping(Long dataSetId, User user) {
        if (dataSetId == null) {
            return null;
        }
        DataSetResp dataSet = dataSetService.getDataSet(dataSetId);
        if (dataSet == null) {
            return null;
        }
        return buildMapping(dataSet, user);
    }

    public SemanticDatasetMapping buildMapping(DataSetResp dataSet, User user) {
        if (dataSet == null || dataSet.getId() == null) {
            return null;
        }
        Long dataSetId = dataSet.getId();
        List<Long> modelIds = safeList(dataSet.getAllModels());
        if (modelIds.isEmpty()) {
            log.warn("superset semantic dataset skip, no models, dataSetId={}", dataSetId);
            return null;
        }

        Long primaryModelId = resolvePrimaryModelId(dataSet);
        ModelResp primaryModel =
                primaryModelId == null ? null : modelService.getModel(primaryModelId);
        Long databaseId = primaryModel == null ? null : primaryModel.getDatabaseId();
        if (databaseId == null) {
            log.warn("superset semantic dataset skip, database unresolved, dataSetId={}",
                    dataSetId);
            return null;
        }
        DatabaseResp databaseResp = databaseService.getDatabase(databaseId);
        String schemaName = resolveSchema(databaseResp);

        List<DimensionResp> selectedDimensions = resolveSelectedDimensions(dataSet, modelIds);
        List<MetricResp> selectedMetrics = resolveSelectedMetrics(dataSet, modelIds);

        Map<String, MetricResp> metricsByBizName =
                selectedMetrics.stream().filter(m -> StringUtils.isNotBlank(m.getBizName()))
                        .collect(Collectors.toMap(m -> m.getBizName().toLowerCase(Locale.ROOT),
                                Function.identity(), (left, right) -> left));

        List<SupersetDatasetMetric> supersetMetrics = new ArrayList<>();
        Set<String> seenMetricNamesLower = new LinkedHashSet<>();
        Set<String> dependencyFields = new LinkedHashSet<>();
        for (MetricResp metric : selectedMetrics) {
            String metricBizName = StringUtils.trimToNull(metric.getBizName());
            if (metricBizName == null) {
                continue;
            }
            String metricKey = metricBizName.toLowerCase(Locale.ROOT);
            if (!seenMetricNamesLower.add(metricKey)) {
                continue;
            }
            String expandedExpr = metricExpander.expandMetricExpression(metric, metricsByBizName);
            if (StringUtils.isBlank(expandedExpr)) {
                continue;
            }
            Set<String> fields =
                    extractDependencyFields(expandedExpr, seenMetricNamesLower, metricsByBizName);
            if (!CollectionUtils.isEmpty(fields)) {
                dependencyFields.addAll(fields);
            }
            SupersetDatasetMetric m = new SupersetDatasetMetric();
            m.setMetricName(metricBizName);
            m.setExpression(expandedExpr);
            m.setMetricType("SQL");
            m.setVerboseName(metric.getName());
            m.setDescription(metric.getDescription());
            supersetMetrics.add(m);
        }

        List<SupersetDatasetColumn> supersetColumns = new ArrayList<>();
        Set<String> seenColumnNamesLower = new LinkedHashSet<>();
        String mainDttmCol = null;
        for (DimensionResp dim : selectedDimensions) {
            if (dim == null || StringUtils.isBlank(dim.getBizName())) {
                continue;
            }
            if (!isValidSemanticIdentifier(dim.getBizName())) {
                log.warn(
                        "superset semantic dataset ignore invalid dimension bizName, dataSetId={}, bizName={}",
                        dataSetId, dim.getBizName());
                continue;
            }
            String dimKey = dim.getBizName().toLowerCase(Locale.ROOT);
            if (!seenColumnNamesLower.add(dimKey)) {
                continue;
            }
            SupersetDatasetColumn col = new SupersetDatasetColumn();
            col.setColumnName(dim.getBizName());
            col.setVerboseName(dim.getName());
            col.setDescription(dim.getDescription());
            col.setGroupby(true);
            col.setFilterable(true);
            boolean isDttm = dim.isTimeDimension() || dim.isPartitionTime();
            col.setIsDttm(isDttm);
            col.setType(resolveSupersetColumnType(dim, isDttm));
            supersetColumns.add(col);
            if (mainDttmCol == null && dim.isPartitionTime()) {
                mainDttmCol = dim.getBizName();
            }
        }

        // Dependency fields are physical columns used by metrics. Expose them so Superset metric
        // SQL can reference them.
        for (String field : dependencyFields) {
            if (StringUtils.isBlank(field)) {
                continue;
            }
            if (!isValidSemanticIdentifier(field)) {
                continue;
            }
            String fieldKey = field.toLowerCase(Locale.ROOT);
            if (!seenColumnNamesLower.add(fieldKey)) {
                continue;
            }
            SupersetDatasetColumn col = new SupersetDatasetColumn();
            col.setColumnName(field);
            col.setVerboseName(field);
            col.setDescription("metric dependency field");
            col.setGroupby(false);
            col.setFilterable(false);
            col.setIsDttm(false);
            col.setType("STRING");
            supersetColumns.add(col);
        }

        LinkedHashSet<String> selectItems = new LinkedHashSet<>();
        supersetColumns.stream().map(SupersetDatasetColumn::getColumnName)
                .filter(StringUtils::isNotBlank).forEach(selectItems::add);
        if (selectItems.isEmpty()) {
            log.warn("superset semantic dataset skip, no select items, dataSetId={}", dataSetId);
            return null;
        }

        String s2sql = "SELECT " + String.join(", ", selectItems) + " FROM t_" + dataSetId;
        String translatedSql = translateToPhysicalSql(dataSetId, s2sql, user);
        if (StringUtils.isBlank(translatedSql)) {
            log.warn("superset semantic dataset skip, translate failed, dataSetId={}", dataSetId);
            return null;
        }

        String supersetTableName = "s2_ds_" + dataSetId;
        String sqlHash = MD5Util.getMD5("semantic_dataset:" + dataSetId, false, MD5Util.BIT32);

        String datasetName = StringUtils.defaultIfBlank(dataSet.getName(),
                StringUtils.defaultIfBlank(dataSet.getBizName(), "语义数据集"));
        String desc = StringUtils.defaultIfBlank(dataSet.getDescription(), "Supersonic 语义数据集");
        desc = desc + "，dataSetId=" + dataSetId;

        List<String> tags = new ArrayList<>();
        tags.add("supersonic");
        tags.add("semantic");
        tags.add("datasetId:" + dataSetId);
        if (StringUtils.isNotBlank(dataSet.getBizName())) {
            tags.add("datasetBizName:" + dataSet.getBizName());
        }

        return SemanticDatasetMapping.builder().dataSetId(dataSetId).dataSetName(datasetName)
                .dataSetBizName(dataSet.getBizName()).databaseId(databaseId).schemaName(schemaName)
                .supersetTableName(truncateName(supersetTableName)).supersetSql(translatedSql)
                .sqlHash(sqlHash).datasetDesc(desc).tagsJson(JsonUtil.toString(tags))
                .mainDttmCol(mainDttmCol).columns(supersetColumns).metrics(supersetMetrics).build();
    }

    private Set<String> extractDependencyFields(String expr, Set<String> metricNamesLower,
            Map<String, MetricResp> metricsByBizName) {
        if (StringUtils.isBlank(expr)) {
            return Collections.emptySet();
        }
        String sanitized = sanitizeMetricExpr(expr);
        try {
            Set<String> columns = SqlSelectHelper.getFieldsFromExpr(sanitized);
            if (CollectionUtils.isEmpty(columns)) {
                return Collections.emptySet();
            }
            return columns.stream().filter(StringUtils::isNotBlank).map(String::trim)
                    .filter(this::isValidDependencyToken)
                    .filter(token -> !metricNamesLower.contains(token.toLowerCase(Locale.ROOT)))
                    .filter(token -> !metricsByBizName.containsKey(token.toLowerCase(Locale.ROOT)))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } catch (Exception ex) {
            // JSqlParser doesn't support some dialect fragments (e.g. postgres :: cast). Fallback
            // to a conservative identifier scan.
            return regexExtractDependencies(sanitized, metricNamesLower, metricsByBizName);
        }
    }

    private String sanitizeMetricExpr(String expr) {
        if (StringUtils.isBlank(expr)) {
            return expr;
        }
        String sanitized = expr;
        sanitized = POSTGRES_CAST_PATTERN.matcher(sanitized).replaceAll("");
        return sanitized;
    }

    private Set<String> regexExtractDependencies(String sanitizedExpr, Set<String> metricNamesLower,
            Map<String, MetricResp> metricsByBizName) {
        if (StringUtils.isBlank(sanitizedExpr)) {
            return Collections.emptySet();
        }
        // Remove string literals to avoid treating them as identifiers.
        String noStrings = sanitizedExpr.replaceAll("'([^']|'')*'", " ");
        Matcher matcher = IDENTIFIER_PATTERN.matcher(noStrings);
        Set<String> tokens = new LinkedHashSet<>();
        while (matcher.find()) {
            String token = StringUtils.trimToNull(matcher.group());
            if (token == null) {
                continue;
            }
            if (!isValidDependencyToken(token)) {
                continue;
            }
            String key = token.toLowerCase(Locale.ROOT);
            if (metricNamesLower.contains(key) || metricsByBizName.containsKey(key)) {
                continue;
            }
            tokens.add(token);
        }
        return tokens;
    }

    private boolean isValidDependencyToken(String token) {
        if (StringUtils.isBlank(token)) {
            return false;
        }
        if ("_".equals(token)) {
            return false;
        }
        if (!isValidSemanticIdentifier(token)) {
            return false;
        }
        return !DEPENDENCY_TOKEN_IGNORE.contains(token.toLowerCase(Locale.ROOT));
    }

    private boolean isValidSemanticIdentifier(String token) {
        if (StringUtils.isBlank(token)) {
            return false;
        }
        if ("_".equals(token)) {
            return false;
        }
        return token.matches("[A-Za-z_][A-Za-z0-9_]*");
    }

    private String translateToPhysicalSql(Long dataSetId, String s2sql, User user) {
        try {
            QuerySqlReq req = new QuerySqlReq();
            req.setDataSetId(dataSetId);
            req.setSql(s2sql);
            User safeUser = user == null ? User.getDefaultUser() : user;
            SemanticTranslateResp resp = semanticLayerService.translate(req, safeUser);
            return resp == null ? null : StringUtils.trimToNull(resp.getQuerySQL());
        } catch (Exception ex) {
            log.warn("superset semantic dataset translate failed, dataSetId={}, s2sql={}",
                    dataSetId, s2sql, ex);
            return null;
        }
    }

    private Long resolvePrimaryModelId(DataSetResp dataSet) {
        if (dataSet == null) {
            return null;
        }
        List<Long> includeAll = dataSet.getAllIncludeAllModels();
        if (!CollectionUtils.isEmpty(includeAll)) {
            return includeAll.get(0);
        }
        List<Long> all = dataSet.getAllModels();
        if (CollectionUtils.isEmpty(all)) {
            return null;
        }
        return all.get(0);
    }

    private List<DimensionResp> resolveSelectedDimensions(DataSetResp dataSet,
            List<Long> modelIds) {
        MetaFilter filter = new MetaFilter(modelIds);
        List<DimensionResp> dims = dimensionService.getDimensions(filter);
        if (CollectionUtils.isEmpty(dims)) {
            return new ArrayList<>();
        }
        return com.tencent.supersonic.headless.server.utils.DimensionConverter.filterByDataSet(dims,
                dataSet);
    }

    private List<MetricResp> resolveSelectedMetrics(DataSetResp dataSet, List<Long> modelIds) {
        MetaFilter filter = new MetaFilter(modelIds);
        List<MetricResp> metrics = metricService.getMetrics(filter);
        if (CollectionUtils.isEmpty(metrics)) {
            return new ArrayList<>();
        }
        return com.tencent.supersonic.headless.server.utils.MetricConverter.filterByDataSet(metrics,
                dataSet);
    }

    private String resolveSchema(DatabaseResp databaseResp) {
        if (databaseResp == null) {
            return null;
        }
        String schema = StringUtils.defaultIfBlank(databaseResp.getSchema(), null);
        if (StringUtils.isBlank(schema)
                && EngineType.POSTGRESQL.getName().equalsIgnoreCase(databaseResp.getType())) {
            schema = "public";
        }
        return StringUtils.defaultIfBlank(schema, null);
    }

    private String resolveSupersetColumnType(DimensionResp dim, boolean isDttm) {
        if (isDttm) {
            return "DATE";
        }
        if (dim != null && dim.getDataType() != null) {
            switch (dim.getDataType()) {
                case BIGINT:
                case INT:
                case DOUBLE:
                case FLOAT:
                case DECIMAL:
                    return "NUMBER";
                case DATE:
                    return "DATE";
                default:
                    break;
            }
        }
        return "STRING";
    }

    private List<Long> safeList(List<Long> ids) {
        if (ids == null) {
            return new ArrayList<>();
        }
        return ids.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
    }

    private String truncateName(String name) {
        if (StringUtils.isBlank(name)) {
            return name;
        }
        if (name.length() <= NAME_LIMIT) {
            return name;
        }
        return name.substring(0, NAME_LIMIT);
    }
}
