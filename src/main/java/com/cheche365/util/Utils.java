package com.cheche365.util;

import com.google.common.collect.Lists;
import lombok.NonNull;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class Utils {
    /**
     * 指定个数数据组合(不重复)
     *
     * @param source 数据源列表
     * @param k      排列元素个数
     * @return
     */
    public static <T> List<List<T>> combine(@NonNull List<T> source, int k) {
        int n = source.size();
        List<List<T>> res = new LinkedList<>();
        if (n <= 0 || k <= 0 || k > n) {
            return res;
        }
        List<T> list = new ArrayList<>();
        generateCombinations(source, k, 0, list, res);

        return res;
    }

    private static <T> void generateCombinations(List<T> source, int k, int start, List<T> list, List<List<T>> res) {
        int n = source.size() - 1;
        if (list.size() == k) {
            res.add(Lists.newArrayList(list));
            return;
        }
        for (int i = start; i <= n - (k - list.size()) + 1; i++) {
            list.add(source.get(i));
            generateCombinations(source, k, i + 1, list, res);
            list.remove(list.size() - 1);
        }
    }

    public static <T> List<T> combine(@NonNull List<T> source, @NonNull Predicate<List<T>> p) {
        int n = source.size();
        while (n > 0) {
            List<List<T>> sList = combine(source, n--);
            for (List<T> s : sList) {
                if (p.test(s)) {
                    return s;
                }
            }
        }
        return null;
    }

    public static <T, K> Map<List<T>, List<K>> matchCombine(@NonNull List<T> sources, @NonNull List<K> targets, @NonNull BiPredicate<List<T>, List<K>> bp) {
        int n = sources.size();
        while (n > 0) {
            List<List<T>> sList = combine(sources, n--);
            for (List<T> s : sList) {
                int m = targets.size();
                while (m > 0) {
                    List<List<K>> tList = combine(targets, m--);
                    for (List<K> t : tList) {
                        if (bp.test(s, t)) {
                            Map<List<T>, List<K>> map = new HashMap<>(2);
                            synchronized (s) {
                                map.put(s, t);
                            }
                            return map;
                        }
                    }
                }
            }
        }
        return null;
    }

    public static <T> List<T> getRandomList(List<T> paramList, int count) {
        if (paramList.size() < count) {
            return paramList;
        }
        Random random = new Random();
        List<Integer> tempList = new ArrayList<>();
        List<T> newList = new ArrayList<>();
        int temp = 0;
        for (int i = 0; i < count; i++) {
            //将产生的随机数作为被抽list的索引
            temp = random.nextInt(paramList.size());
            if (!tempList.contains(temp)) {
                tempList.add(temp);
                newList.add(paramList.get(temp));
            } else {
                i--;
            }
        }
        return newList;
    }
}
