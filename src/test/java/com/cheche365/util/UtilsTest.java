package com.cheche365.util;

import lombok.extern.log4j.Log4j2;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.abs;

@Log4j2
public class UtilsTest {
    @Test
    public void matchCombine() {
        Random random = new Random(30);
        List<Integer> list = Stream.generate(() -> random.nextInt(5000)).limit(10).collect(Collectors.toList());
        List<Integer> list2 = Stream.generate(() -> random.nextInt(10000)).limit(20).collect(Collectors.toList());
        log.info(list);
        log.info(list2);
        log.info("开始");
        MatchResult<Integer, Integer> listListMap = Utils.matchCombine(list, list2, (a, b) -> {
            Integer n = a.stream().reduce(Integer::sum).get();
            Integer m = b.stream().reduce(Integer::sum).get();
            return abs((n - m) / n) < 1;
        });

        log.info(listListMap);
        log.info("结束");
    }
}