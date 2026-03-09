package com.tencent.supersonic.headless.server.tools;

import javax.sql.DataSource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@SpringBootApplication(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
public class SupersetDatasetRegistrySchemaInspectCli {

    public static void main(String[] args) throws Exception {
        SpringApplication app =
                new SpringApplication(SupersetDatasetRegistrySchemaInspectCli.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        try (ConfigurableApplicationContext context = app.run(args)) {
            DataSource dataSource = context.getBean(DataSource.class);
            try (Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement()) {
                String sql = """
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = 's2_superset_dataset'
                        ORDER BY ordinal_position
                        """;
                try (ResultSet rs = statement.executeQuery(sql)) {
                    while (rs.next()) {
                        System.out.println(rs.getString(1) + "\\t" + rs.getString(2));
                    }
                }
            }
        }
    }
}
