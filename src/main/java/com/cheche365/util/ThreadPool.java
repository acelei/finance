package com.cheche365.util;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * 线程池服务.
 *
 * @author WangLei
 * @date 2018 -05-25 13:47:46
 */
public class ThreadPool {
    private static final int CORE_THREADS = 200;
    private static final int MAX_THREADS = 20000;
    private static final long ALIVE_TIME = 0L;
    public static final Map EMPTY_MAP = Maps.newHashMapWithExpectedSize(0);

    private final ExecutorService pool;

    private ThreadPool(String name) {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(name + "-pool-%d").build();
        pool = new ThreadPoolExecutor(CORE_THREADS, MAX_THREADS, ALIVE_TIME,
                TimeUnit.MINUTES, new LinkedBlockingDeque<>(),
                namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
    }


    public static ThreadPool newInstance(String name) {
        return new ThreadPool(name);
    }

    public ExecutorService getPool() {
        return pool;
    }

    public <T> ExecutorCompletionService<T> getCompletionPool() {
        return new ExecutorCompletionService<>(pool);
    }

    public <T, K> List<K> submitWithResult(List<T> list, FunctionWithException<T, K> func) throws ExecutionException, InterruptedException {
        return submit(list, func, getCompletionPool());
    }

    public <T, K> List<K> submitWithResultByOrder(List<T> list, FunctionWithException<T, K> func) throws ExecutionException, InterruptedException {
        return submit(list, func, pool);
    }

    public <T> CountDownLatch executeWithLatch(List<T> list, Consumer<T> func) {
        return execute(list, func, getPool());
    }

    private static <T, K> List<K> submit(List<T> list, FunctionWithException<T, K> func, ExecutorService service) throws InterruptedException, ExecutionException {
        List<Future<K>> futureList = new ArrayList<>(list.size());
        List<K> resultList = new ArrayList<>(list.size());
        for (T o : list) {
            futureList.add(service.submit(() -> func.apply(o)));
        }
        for (Future<K> future : futureList) {
            resultList.add(future.get());
        }
        return resultList;
    }

    private static <T, K> List<K> submit(List<T> list, FunctionWithException<T, K> func, ExecutorCompletionService<K> service) throws InterruptedException, ExecutionException {
        List<K> resultList = new ArrayList<>(list.size());
        for (T o : list) {
            service.submit(() -> func.apply(o));
        }

        for (int i = 0; i < list.size(); i++) {
            resultList.add(service.take().get());
        }

        return resultList;
    }

    private static <T> CountDownLatch execute(List<T> list, Consumer<T> func, ExecutorService service) {
        CountDownLatch count = new CountDownLatch(list.size());
        for (T o : list) {
            service.execute(() -> {
                try {
                    func.accept(o);
                } finally {
                    count.countDown();
                }
            });
        }
        return count;
    }

    @FunctionalInterface
    public interface FunctionWithException<T, R> {
        R apply(T t) throws Exception;
    }
}
