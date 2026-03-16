package com.tencent.supersonic.chat.server.plugin.build.superset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SupersetPluginConfigTest {

    @Test
    public void testAuthConfigDisabled() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setAuthEnabled(false);
        Assertions.assertTrue(config.hasValidAuthConfig());
    }

    @Test
    public void testAuthConfigJwt() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setAuthEnabled(true);
        config.setJwtUsername("admin");
        config.setJwtPassword("admin");
        Assertions.assertTrue(config.hasValidAuthConfig());
    }

    @Test
    public void testAuthConfigMissing() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setAuthEnabled(true);
        Assertions.assertFalse(config.hasValidAuthConfig());
    }
}
