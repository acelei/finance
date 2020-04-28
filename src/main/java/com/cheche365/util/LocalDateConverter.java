package com.cheche365.util;

import org.apache.commons.beanutils.converters.AbstractConverter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.util.Date;

import static java.time.temporal.ChronoField.*;

public class LocalDateConverter extends AbstractConverter {
    public static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
            .withChronology(IsoChronology.INSTANCE);

    public LocalDateConverter() {
        super();
    }

    public LocalDateConverter(final Object defaultValue) {
        super(defaultValue);
    }

    @Override
    protected <T> T convertToType(Class<T> type, Object value) throws Throwable {
        if (value instanceof String) {
            if ("".equals(value)) {
                return null;
            } else {
                return type.cast(LocalDate.parse(value.toString(), DATE_TIME_FORMATTER));
            }
        } else if (value instanceof Date) {
            return type.cast(LocalDateTime.ofInstant(((Date) value).toInstant(), ZoneId.systemDefault()).toLocalDate());
        } else if (value instanceof LocalDateTime) {
            return type.cast(((LocalDateTime) value).toLocalDate());
        } else if (value instanceof Long) {
            return type.cast(LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value), ZoneId.systemDefault()).toLocalDate());
        }
        throw conversionException(type, value);
    }

    @Override
    protected Class<?> getDefaultType() {
        return LocalDate.class;
    }
}
