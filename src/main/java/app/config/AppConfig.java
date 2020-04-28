package app.config;

import com.cheche365.util.LocalDateConverter;
import groovy.sql.Sql;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.*;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;

@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
@ComponentScan("com.cheche365")
@Configuration
public class AppConfig implements ApplicationRunner {
    @Bean("baseSql")
    public Sql getSql(DataSource dataSource) {
        return new Sql(dataSource);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        ConvertUtils.register(new DateConverter(null), Date.class);
        ConvertUtils.register(new StringConverter(null), String.class);
        ConvertUtils.register(new LongConverter(null), Long.class);
        ConvertUtils.register(new ShortConverter(null), Short.class);
        ConvertUtils.register(new IntegerConverter(null), Integer.class);
        ConvertUtils.register(new DoubleConverter(null), Double.class);
        ConvertUtils.register(new BigDecimalConverter(null), BigDecimal.class);
        ConvertUtils.register(new LocalDateConverter(null), LocalDate.class);
    }

}
