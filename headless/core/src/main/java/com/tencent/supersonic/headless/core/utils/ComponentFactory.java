package com.tencent.supersonic.headless.core.utils;

import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.headless.core.cache.DefaultQueryCache;
import com.tencent.supersonic.headless.core.cache.QueryCache;
import com.tencent.supersonic.headless.core.executor.QueryAccelerator;
import com.tencent.supersonic.headless.core.executor.QueryExecutor;
import com.tencent.supersonic.headless.core.translator.optimizer.QueryOptimizer;
import com.tencent.supersonic.headless.core.translator.parser.QueryParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.support.SpringFactoriesLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** QueryConverter QueryOptimizer QueryExecutor object factory */
@Slf4j
public class ComponentFactory {

    private static final List<String> DEFAULT_QUERY_OPTIMIZER_ORDER =
            Arrays.asList("DbDialectOptimizer", "ResultLimitOptimizer");

    private static final List<String> DEFAULT_QUERY_PARSER_ORDER =
            Arrays.asList("SqlVariableParser", "StructQueryParser", "SqlQueryParser",
                    "DefaultDimValueParser", "DimExpressionParser", "MetricExpressionParser",
                    "MetricRatioParser", "OntologyQueryParser");

    private static final List<String> DEFAULT_QUERY_EXECUTOR_ORDER = Arrays.asList("JdbcExecutor");

    private static Map<String, QueryOptimizer> queryOptimizers = new HashMap<>();
    private static List<QueryExecutor> queryExecutors = new ArrayList<>();
    private static List<QueryAccelerator> queryAccelerators = new ArrayList<>();
    private static List<QueryParser> queryParsers = new ArrayList<>();
    private static QueryCache queryCache;

    static {
        initQueryOptimizer();
        initQueryExecutors();
        initQueryAccelerators();
        initQueryParser();
        initQueryCache();
    }

    public static List<QueryOptimizer> getQueryOptimizers() {
        if (queryOptimizers.isEmpty()) {
            initQueryOptimizer();
        }
        return queryOptimizers.values().stream().collect(Collectors.toList());
    }

    public static List<QueryExecutor> getQueryExecutors() {
        if (queryExecutors.isEmpty()) {
            initQueryExecutors();
        }
        return queryExecutors;
    }

    public static List<QueryAccelerator> getQueryAccelerators() {
        if (queryAccelerators.isEmpty()) {
            initQueryAccelerators();
        }
        return queryAccelerators;
    }

    public static List<QueryParser> getQueryParsers() {
        if (queryParsers == null || queryParsers.isEmpty()) {
            initQueryParser();
        }
        return queryParsers;
    }

    public static QueryCache getQueryCache() {
        if (queryCache == null) {
            initQueryCache();
        }
        return queryCache;
    }

    public static void addQueryOptimizer(String name, QueryOptimizer queryOptimizer) {
        queryOptimizers.put(name, queryOptimizer);
    }

    private static void initQueryOptimizer() {
        List<QueryOptimizer> queryOptimizerList = new ArrayList<>();
        init(QueryOptimizer.class, queryOptimizerList);
        if (!queryOptimizerList.isEmpty()) {
            queryOptimizerList.stream()
                    .forEach(q -> addQueryOptimizer(q.getClass().getSimpleName(), q));
        }
    }

    private static void initQueryExecutors() {
        // queryExecutors.add(ContextUtils.getContext().getBean("JdbcExecutor",
        // JdbcExecutor.class));
        List<QueryExecutor> executors = new ArrayList<>();
        init(QueryExecutor.class, executors);
        queryExecutors = executors;
    }

    private static void initQueryAccelerators() {
        // queryExecutors.add(ContextUtils.getContext().getBean("JdbcExecutor",
        // JdbcExecutor.class));
        List<QueryAccelerator> accelerators = new ArrayList<>();
        init(QueryAccelerator.class, accelerators);
        queryAccelerators = accelerators;
    }

    private static void initQueryParser() {
        List<QueryParser> parsers = new ArrayList<>();
        init(QueryParser.class, parsers);
        queryParsers = parsers;
    }

    private static void initQueryCache() {
        queryCache = init(QueryCache.class);
        if (queryCache == null) {
            log.warn("No QueryCache implementation found via SpringFactoriesLoader, fallback to {}",
                    DefaultQueryCache.class.getName());
            queryCache = new DefaultQueryCache();
        }
    }

    public static <T> T getBean(String name, Class<T> tClass) {
        return ContextUtils.getContext().getBean(name, tClass);
    }

    private static <T> List<T> init(Class<T> factoryType, List list) {
        List<T> factories = SpringFactoriesLoader.loadFactories(factoryType,
                Thread.currentThread().getContextClassLoader());
        if (factories != null && !factories.isEmpty()) {
            list.addAll(factories);
            return list;
        }

        // Fallback for module classpaths that don't include META-INF/spring.factories (e.g. CLI
        // running from headless/server module). After Spring context is ready, most implementations
        // are still discoverable as beans.
        try {
            if (ContextUtils.getContext() != null) {
                Map<String, T> beans = ContextUtils.getBeansOfType(factoryType);
                if (beans != null && !beans.isEmpty()) {
                    List<T> beanList = new ArrayList<>(beans.values());
                    list.addAll(sortByDefaultOrderIfNeeded(factoryType, beanList));
                }
            }
        } catch (Exception e) {
            log.warn("Init {} from Spring context failed, ignored", factoryType.getName(), e);
        }
        return list;
    }

    private static <T> T init(Class<T> factoryType) {
        List<T> factories = SpringFactoriesLoader.loadFactories(factoryType,
                Thread.currentThread().getContextClassLoader());
        if (factories == null || factories.isEmpty()) {
            return null;
        }
        return factories.get(0);
    }

    private static <T> List<T> sortByDefaultOrderIfNeeded(Class<T> factoryType, List<T> factories) {
        List<String> order = null;
        if (QueryParser.class.equals(factoryType)) {
            order = DEFAULT_QUERY_PARSER_ORDER;
        } else if (QueryOptimizer.class.equals(factoryType)) {
            order = DEFAULT_QUERY_OPTIMIZER_ORDER;
        } else if (QueryExecutor.class.equals(factoryType)) {
            order = DEFAULT_QUERY_EXECUTOR_ORDER;
        }
        if (order == null || factories == null || factories.size() <= 1) {
            return factories;
        }

        Map<String, Integer> orderIndex = new LinkedHashMap<>();
        for (int i = 0; i < order.size(); i++) {
            orderIndex.put(order.get(i), i);
        }
        factories.sort(Comparator.comparingInt(
                o -> orderIndex.getOrDefault(o.getClass().getSimpleName(), Integer.MAX_VALUE)));
        return factories;
    }
}
