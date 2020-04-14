package app.config;

import groovy.sql.Sql;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
@ComponentScan("com.cheche365")
@Configuration
public class AppConfig {
    @Bean("baseSql")
    public Sql getSql(DataSource dataSource) {
        return new Sql(dataSource);
    }
}
