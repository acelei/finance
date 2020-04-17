import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.filter.ThresholdFilter

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

statusListener OnConsoleStatusListener

def patternLayout = "[%d{yyyy/MM/dd HH:mm:ss} %-5level [%thread] %class{5}:%M:%line] --  %msg]%n"
def configMap = [
        "level"    : WARN,
        "path"     : "/home/logs",
        "appenders": ["FILE-INFO", "FILE-ERROR"]
]

def curConfig = configMap

appender("FILE-INFO", RollingFileAppender) {
    filter(ThresholdFilter) {
        level = INFO
    }
    file = "${curConfig.path}/info-logFile.log"
    rollingPolicy(FixedWindowRollingPolicy) {
        fileNamePattern = "${curConfig.path}/info-logFile.%i.log"
        minIndex = 1
        maxIndex = 50
    }
    triggeringPolicy(SizeBasedTriggeringPolicy) {
        maxFileSize = "20MB"
    }
    encoder(PatternLayoutEncoder) {
        pattern = patternLayout
    }
}
appender("FILE-ERROR", RollingFileAppender) {
    filter(ThresholdFilter) {
        level = ERROR
    }
    file = "${curConfig.path}/error-logFile.log"
    rollingPolicy(FixedWindowRollingPolicy) {
        fileNamePattern = "${curConfig.path}/error-logFile.%i.log"
        minIndex = 1
        maxIndex = 20
    }
    triggeringPolicy(SizeBasedTriggeringPolicy) {
        maxFileSize = "20MB"
    }
    encoder(PatternLayoutEncoder) {
        pattern = patternLayout
    }
}

root(curConfig.level, curConfig.appenders)