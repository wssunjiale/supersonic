package com.tencent.supersonic.common.jsqlparser;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Set;

class SqlNormalizeHelperTest {

    @Test
    void testNormalizeSqlTrimsAndRemovesSemicolon() {
        String normalized = SqlNormalizeHelper.normalizeSql("  select * from demo_table ; ");
        Assert.assertFalse(normalized.endsWith(";"));
        Assert.assertTrue(normalized.toLowerCase().startsWith("select"));
    }

    @Test
    void testExtractTableNamesDetectsMultipleTables() {
        String sql = "select a.id, b.name from schema_a.table_a a join table_b b on a.id = b.id";
        Set<String> tables = SqlNormalizeHelper.extractTableNames(sql);
        Assert.assertEquals(2, tables.size());
        Assert.assertTrue(tables.contains("schema_a.table_a"));
        Assert.assertTrue(tables.contains("table_b"));
    }

    @Test
    void testIsSingleTableReturnsTrue() {
        String sql = "select * from table_a where id = 1";
        Assert.assertTrue(SqlNormalizeHelper.isSingleTable(sql));
    }
}
