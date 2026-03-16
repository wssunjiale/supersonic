package com.tencent.supersonic.headless.server.tools;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.persistence.mapper.SupersetDatasetMapper;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSourceType;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSyncState;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncClient;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncResult;
import com.tencent.supersonic.headless.server.sync.superset.migration.SupersetDatasetRegistrySchemaMigrator;
import com.tencent.supersonic.headless.server.sync.superset.semantic.SupersetSemanticDatasetSyncService;
import org.apache.commons.lang3.StringUtils;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

@Profile("tools-cli")
@SpringBootApplication(scanBasePackages = {"com.tencent.supersonic"},
        exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@MapperScan(basePackages = {"com.tencent.supersonic.headless.server.persistence.mapper",
                "com.tencent.supersonic.common.persistence.mapper"})
public class SupersetSemanticDatasetSyncCli {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SupersetSemanticDatasetSyncCli.class);
        app.setAdditionalProfiles("tools-cli");
        app.setWebApplicationType(WebApplicationType.NONE);
        try (ConfigurableApplicationContext context = app.run(args)) {
            SupersetSemanticDatasetSyncService syncService =
                    context.getBean(SupersetSemanticDatasetSyncService.class);
            SupersetDatasetMapper datasetMapper = context.getBean(SupersetDatasetMapper.class);
            SupersetSyncClient syncClient = context.getBean(SupersetSyncClient.class);

            SupersetDatasetRegistrySchemaMigrator migrator =
                    context.getBean(SupersetDatasetRegistrySchemaMigrator.class);
            SupersetDatasetRegistrySchemaMigrator.MigrationReport report = migrator.migrate(false);
            System.out.println(JsonUtil.toString(report));
            if (report != null && report.getErrors() != null && !report.getErrors().isEmpty()) {
                throw new IllegalStateException(
                        "registry schema migrate failed, errors=" + report.getErrors().size());
            }

            SupersetSyncResult result = syncService.syncAllSemanticDatasets(User.getDefaultUser());
            System.out.println(JsonUtil.toString(result));

            printRegistryStatus(datasetMapper);
            verifySyncedSemanticDatasets(datasetMapper, syncClient);
        }
    }

    private static void printRegistryStatus(SupersetDatasetMapper datasetMapper) {
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SupersetDatasetDO::getSourceType,
                SupersetDatasetSourceType.SEMANTIC_DATASET.name());
        List<SupersetDatasetDO> all = datasetMapper.selectList(wrapper);
        if (all == null || all.isEmpty()) {
            return;
        }
        for (SupersetDatasetDO record : all) {
            if (record == null) {
                continue;
            }
            String msg = record.getSyncErrorMsg();
            if (msg != null && msg.length() > 300) {
                msg = msg.substring(0, 300) + "...";
            }
            System.out.println(String.format(
                    "REGISTRY id=%s, supersetId=%s, syncState=%s, errType=%s, errMsg=%s",
                    record.getId(), record.getSupersetDatasetId(), record.getSyncState(),
                    record.getSyncErrorType(), msg));
        }
    }

    private static void verifySyncedSemanticDatasets(SupersetDatasetMapper datasetMapper,
            SupersetSyncClient syncClient) {
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SupersetDatasetDO::getSourceType,
                SupersetDatasetSourceType.SEMANTIC_DATASET.name());
        List<SupersetDatasetDO> all = datasetMapper.selectList(wrapper);

        int total = 0;
        int ok = 0;
        int issues = 0;
        for (SupersetDatasetDO record : all) {
            if (record == null) {
                continue;
            }
            total++;
            if (!SupersetDatasetSyncState.SUCCESS.name().equalsIgnoreCase(record.getSyncState())
                    || record.getSupersetDatasetId() == null) {
                continue;
            }
            SupersetDatasetInfo remote = null;
            try {
                remote = syncClient.fetchDataset(record.getSupersetDatasetId());
            } catch (Exception ex) {
                issues++;
                System.out.println(
                        String.format("VERIFY fetch failed, registryId=%s, supersetId=%s, msg=%s",
                                record.getId(), record.getSupersetDatasetId(), ex.getMessage()));
                continue;
            }

            List<SupersetDatasetColumn> expectedColumns =
                    JsonUtil.toList(record.getColumns(), SupersetDatasetColumn.class);
            List<SupersetDatasetMetric> expectedMetrics =
                    JsonUtil.toList(record.getMetrics(), SupersetDatasetMetric.class);
            Set<String> expectedCols = toLowerSet(expectedColumns == null ? List.of()
                    : expectedColumns.stream().map(SupersetDatasetColumn::getColumnName).toList());
            Set<String> expectedMetricNames = toLowerSet(expectedMetrics == null ? List.of()
                    : expectedMetrics.stream().map(SupersetDatasetMetric::getMetricName).toList());

            Set<String> remoteCols =
                    toLowerSet(remote == null || remote.getColumns() == null ? List.of()
                            : remote.getColumns().stream().map(SupersetDatasetColumn::getColumnName)
                                    .collect(Collectors.toList()));
            Set<String> remoteMetrics =
                    toLowerSet(remote == null || remote.getMetrics() == null ? List.of()
                            : remote.getMetrics().stream().map(SupersetDatasetMetric::getMetricName)
                                    .collect(Collectors.toList()));

            boolean colOk = remoteCols.containsAll(expectedCols);
            boolean metricOk = remoteMetrics.containsAll(expectedMetricNames);
            if (colOk && metricOk) {
                ok++;
                continue;
            }
            issues++;
            Set<String> missingCols = new HashSet<>(expectedCols);
            missingCols.removeAll(remoteCols);
            Set<String> missingMetrics = new HashSet<>(expectedMetricNames);
            missingMetrics.removeAll(remoteMetrics);
            System.out.println(String.format(
                    "VERIFY mismatch, registryId=%s, supersetId=%s, missingCols=%s, missingMetrics=%s",
                    record.getId(), record.getSupersetDatasetId(), missingCols, missingMetrics));
        }
        System.out.println(
                String.format("VERIFY summary: total=%s, ok=%s, issues=%s", total, ok, issues));
    }

    private static Set<String> toLowerSet(List<String> items) {
        if (items == null || items.isEmpty()) {
            return Set.of();
        }
        Set<String> set = new HashSet<>();
        for (String item : items) {
            if (StringUtils.isBlank(item)) {
                continue;
            }
            set.add(item.trim().toLowerCase(Locale.ROOT));
        }
        return set;
    }

    @Profile("tools-cli")
    @Configuration(proxyBeanMethods = false)
    static class CliOverrides {

        @Bean
        @Primary
        public com.tencent.supersonic.auth.api.authentication.service.UserService noopUserService() {
            return new NoopUserService();
        }

        @Bean
        @Primary
        public com.tencent.supersonic.auth.api.authorization.service.AuthService noopAuthService() {
            return new NoopAuthService();
        }
    }

    static class NoopUserService
            implements com.tencent.supersonic.auth.api.authentication.service.UserService {

        @Override
        public com.tencent.supersonic.common.pojo.User getCurrentUser(
                jakarta.servlet.http.HttpServletRequest httpServletRequest,
                jakarta.servlet.http.HttpServletResponse httpServletResponse) {
            return com.tencent.supersonic.common.pojo.User.getDefaultUser();
        }

        @Override
        public List<String> getUserNames() {
            return List.of();
        }

        @Override
        public List<com.tencent.supersonic.common.pojo.User> getUserList() {
            return List.of();
        }

        @Override
        public void register(
                com.tencent.supersonic.auth.api.authentication.request.UserReq userCmd) {
            throw new UnsupportedOperationException("CLI noop");
        }

        @Override
        public void deleteUser(long userId) {
            throw new UnsupportedOperationException("CLI noop");
        }

        @Override
        public String login(com.tencent.supersonic.auth.api.authentication.request.UserReq userCmd,
                jakarta.servlet.http.HttpServletRequest request) {
            throw new UnsupportedOperationException("CLI noop");
        }

        @Override
        public String login(com.tencent.supersonic.auth.api.authentication.request.UserReq userCmd,
                String appKey) {
            throw new UnsupportedOperationException("CLI noop");
        }

        @Override
        public Set<String> getUserAllOrgId(String userName) {
            return Set.of();
        }

        @Override
        public List<com.tencent.supersonic.common.pojo.User> getUserByOrg(String key) {
            return List.of();
        }

        @Override
        public List<com.tencent.supersonic.auth.api.authentication.pojo.Organization> getOrganizationTree() {
            return List.of();
        }

        @Override
        public String getPassword(String userName) {
            return "";
        }

        @Override
        public void resetPassword(String userName, String password, String newPassword) {
            throw new UnsupportedOperationException("CLI noop");
        }

        @Override
        public com.tencent.supersonic.auth.api.authentication.pojo.UserToken generateToken(
                String name, String userName, long expireTime) {
            throw new UnsupportedOperationException("CLI noop");
        }

        @Override
        public List<com.tencent.supersonic.auth.api.authentication.pojo.UserToken> getUserTokens(
                String userName) {
            return List.of();
        }

        @Override
        public com.tencent.supersonic.auth.api.authentication.pojo.UserToken getUserToken(Long id) {
            return null;
        }

        @Override
        public void deleteUserToken(Long id) {
            throw new UnsupportedOperationException("CLI noop");
        }
    }

    static class NoopAuthService
            implements com.tencent.supersonic.auth.api.authorization.service.AuthService {

        @Override
        public List<com.tencent.supersonic.auth.api.authorization.pojo.AuthGroup> queryAuthGroups(
                String domainId, Integer groupId) {
            return List.of();
        }

        @Override
        public void addOrUpdateAuthGroup(
                com.tencent.supersonic.auth.api.authorization.pojo.AuthGroup group) {
            throw new UnsupportedOperationException("CLI noop");
        }

        @Override
        public void removeAuthGroup(
                com.tencent.supersonic.auth.api.authorization.pojo.AuthGroup group) {
            throw new UnsupportedOperationException("CLI noop");
        }

        @Override
        public com.tencent.supersonic.auth.api.authorization.response.AuthorizedResourceResp queryAuthorizedResources(
                com.tencent.supersonic.auth.api.authorization.request.QueryAuthResReq req,
                com.tencent.supersonic.common.pojo.User user) {
            return new com.tencent.supersonic.auth.api.authorization.response.AuthorizedResourceResp();
        }
    }
}
