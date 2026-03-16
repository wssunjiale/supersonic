# trouble_shooting compact 前快照（自动生成）

- session_id: `4f0cb4d1-5c16-44cb-a7a7-f44c733ab9cf`
- trigger: `auto`

## 关键错误/警告片段（脱敏）

```
1→package com.tencent.supersonic.headless.server.aspect;
     2→
     3→import com.google.common.collect.Lists;
     4→import com.tencent.supersonic.common.pojo.exception.InvalidArgumentException;
     5→import com.tencent.supersonic.headless.api.pojo.DrillDownDimension;
     6→import com.tencent.supersonic.headless.api.pojo.response.DimSchemaResp;
     7→import com.tencent.supersonic.headless.api.pojo.response.MetricSchemaResp;
     8→import com.tencent.supersonic.headless.api.pojo.response.SemanticSchemaResp;
     9→import com.tencent.supersonic.headless.server.utils.DataUtils;
    10→import com.tencent.supersonic.headless.server.utils.MetricDrillDownChecker;
    11→import lombok.extern.slf4j.Slf4j;
    12→import org.junit.jupiter.api.Test;
    13→
    14→import java.util.List;
    15→
    16→import static org.junit.jupiter.api.Assertions.assertThrows;
    17→
    18→@Slf4j
    19→public class MetricDrillDownCheckerTest {
    20→
    21→    @Test
    22→    void test_groupBy_in_drillDownDimension() {
    23→        MetricDrillDownChecker metricDrillDownChecker = new MetricDrillDownChecker();
    24→        String sql = "select user_name, sum(pv) from t_1 group by user_name";
    25→        SemanticSchemaResp semanticSchemaResp = mockModelSchemaResp();
    26→        metricDrillDownChecker.checkQuery(semanticSchemaResp, sql);
    27→    }
    28→
    29→    @Test
    30→    void test_groupBy_not_in_drillDownDimension() {
    31→        MetricDrillDownChecker metricDrillDownChecker = new MetricDrillDownChecker();
    32→        String sql = "select page, sum(pv) from t_1 group by page";
    33→        SemanticSchemaResp semanticSchemaResp = mockModelSchemaResp();
    34→        assertThrows(InvalidArgumentException.class,
    35→                () -> metricDrillDownChecker.checkQuery(semanticSchemaResp, sql));
    36→    }
    37→
    38→    @Test
    39→    void test_groupBy_not_in_necessary_dimension() {
    40→        MetricDrillDownChecker metricDrillDownChecker = new MetricDrillDownChecker();
    41→        String sql = "select user_name, count(distinct uv) from t_1 group by user_name";
    42→        SemanticSchemaResp semanticSchemaResp = mockModelSchemaResp();
    43→        assertThrows(InvalidArgumentException.class,
    44→                () -> metricDrillDownChecker.checkQuery(semanticSchemaResp, sql));
    45→    }
    46→
    47→    @Test
    48→    void test_groupBy_no_necessary_dimension_setting() {
    49→        MetricDrillDownChecker metricDrillDownChecker = new MetricDrillDownChecker();
    50→        String sql = "select user_name, page, count(distinct uv) from t_1 group by user_name,page";
    51→        SemanticSchemaResp semanticSchemaResp = mockModelSchemaNoDimensionSetting();
    52→        metricDrillDownChecker.checkQuery(semanticSchemaResp, sql);
    53→    }
    54→
    55→    @Test
    56→    void test_groupBy_no_necessary_dimension_setting_no_metric() {
    57→        MetricDrillDownChecker metricDrillDownChecker = new MetricDrillDownChecker();
    58→        String sql = "select user_name, page, count(*) from t_1 group by user_name,page";
    59→        SemanticSchemaResp semanticSchemaResp = mockModelSchemaNoDimensionSetting();
    60→        metricDrillDownChecker.checkQuery(semanticSchemaResp, sql);
    61→    }
    62→
    63→    private SemanticSchemaResp mockModelSchemaResp() {
    64→        SemanticSchemaResp semanticSchemaResp = new SemanticSchemaResp();
    65→        semanticSchemaResp.setMetrics(mockMetrics());
    66→        semanticSchemaResp.setDimensions(mockDimensions());
    67→        return semanticSchemaResp;
    68→    }
    69→
    70→    private SemanticSchemaResp mockModelSchemaNoDimensionSetting() {
    71→        SemanticSchemaResp semanticSchemaResp = new SemanticSchemaResp();
    72→        List<MetricSchemaResp> metricSchemaResps =
    73→                Lists.newArrayList(mockMetricsNoDrillDownSetting());
    74→        semanticSchemaResp.setMetrics(metricSchemaRe
```

## 命中经验库（已合并）

### 1. MetricFlow validate-configs 编码错误解决方案
- 路径: `assets\kb\trouble_shooting\metricflow-validate-configs-encoding-error.md`
- 摘要: ## 问题背景

### 2. PostgreSQL列名规范规则
- 路径: `assets\kb\trouble_shooting\postgresql-column-naming.md`
- 摘要: ## 问题背景

### 3. 短命令闭环校验 常见根因与修复
- 路径: `assets\kb\trouble_shooting\short-loop-root-cause.md`
- 摘要: 本文件沉淀短命令闭环校验的高频故障根因与修复手法，作为通用排查流程的固定参照。

### 4. dbt单元测试数据完整性规则
- 路径: `assets\kb\trouble_shooting\dbt-unit-test-completeness.md`
- 摘要: ## 问题背景

### 5. MetricFlow Time Spine YAML 配置要求
- 路径: `assets\kb\trouble_shooting\metricflow-time-spine-yaml-requirement.md`
- 摘要: ## 问题背景
