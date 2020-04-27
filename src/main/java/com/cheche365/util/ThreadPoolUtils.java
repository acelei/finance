package com.cheche365.util;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 线程池服务.
 *
 * @author WangLei
 * @date 2018 -05-25 13:47:46
 */
public class ThreadPoolUtils {
    public static final Map EMPTY_MAP = Maps.newHashMapWithExpectedSize(0);

    private ThreadPoolUtils() {
    }

    public static ExecutorService getTaskPool() {
        return TaskThreadPool.taskPool;
    }

    public static ExecutorService getRunPool() {
        return RunThreadPool.runPool;
    }

    public static <T> ExecutorCompletionService<T> getCompletionTaskPool() {
        return new ExecutorCompletionService<>(TaskThreadPool.taskPool);
    }

    public static <T> ExecutorCompletionService<T> getCompletionRunPool() {
        return new ExecutorCompletionService<>(RunThreadPool.runPool);
    }

    private static class TaskThreadPool {
        private static ExecutorService taskPool;

        static {
            ThreadFactory taskThreadFactory = new ThreadFactoryBuilder().setNameFormat("TaskManager-pool-%d").build();
            taskPool = new ThreadPoolExecutor(ThreadConstants.TASK_THREADS, ThreadConstants.MAX_THREADS, ThreadConstants.ALIVE_TIME,
                    TimeUnit.MINUTES, new LinkedBlockingDeque<>(),
                    taskThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        }
    }

    private static class RunThreadPool {
        private static ExecutorService runPool;

        static {
            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("RunManager-pool-%d").build();
            runPool = new ThreadPoolExecutor(ThreadConstants.CORE_THREADS, ThreadConstants.MAX_THREADS, ThreadConstants.ALIVE_TIME,
                    TimeUnit.MINUTES, new LinkedBlockingDeque<>(),
                    namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        }
    }

    private static <T, K> List<Future<K>> submit(List<T> list, Function<T, K> func, ExecutorCompletionService service) {
        List<Future<K>> fList = new ArrayList<>();
        for (T o : list) {
            fList.add(service.submit(() -> func.apply(o)));
        }
        return fList;
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

    private static <T, K> K getFirst(List<T> list, Function<T, K> func, CompletionService<K> service) throws InterruptedException, ExecutionException {
        for (T o : list) {
            service.submit(() -> func.apply(o));
        }

        int n = list.size();
        K result = null;
        while (result == null && n-- > 0) {
            Future<K> f = service.take();
            result = f.get();
        }

        return result;
    }

    public static <T, K> List<Future<K>> submitTask(List<T> list, Function<T, K> func) {
        return submit(list, func, getCompletionTaskPool());
    }

    public static <T, K> List<Future<K>> submitRun(List<T> list, Function<T, K> func) {
        return submit(list, func, getCompletionRunPool());
    }

    public static <T> CountDownLatch executeTask(List<T> list, Consumer<T> func) {
        return execute(list, func, getTaskPool());
    }

    public static <T> CountDownLatch executeRun(List<T> list, Consumer<T> func) {
        return execute(list, func, getRunPool());
    }

    public static <T, K> K getTaskFirst(List<T> list, Function<T, K> func) throws ExecutionException, InterruptedException {
        return getFirst(list, func, getCompletionTaskPool());
    }

    public static <T, K> K getRunFirst(List<T> list, Function<T, K> func) throws ExecutionException, InterruptedException {
        return getFirst(list, func, getCompletionRunPool());
    }

}
