package com.tonybro.sillybatch;

import java.util.concurrent.*;

public class PriorityThreadPoolExecutor extends ThreadPoolExecutor {

    public PriorityThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public PriorityThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public PriorityThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public PriorityThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new ComparableFutureTask<>(callable);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new ComparableFutureTask<>(runnable, value);
    }

    @SuppressWarnings("unchecked")
    private class ComparableFutureTask<T> extends FutureTask<T> implements Comparable<ComparableFutureTask> {

        private Comparable cmp;

        public ComparableFutureTask(Callable<T> callable) {
            super(callable);
            if (callable instanceof Comparable) {
                cmp = (Comparable) callable;
            } else {
                throw new ClassCastException("Callable task must implement java.lang.Comparable interface!");
            }
        }

        public ComparableFutureTask(Runnable runnable, T result) {
            super(runnable, result);
            if (runnable instanceof Comparable) {
                cmp = (Comparable<T>) runnable;
            } else {
                throw new ClassCastException("Runnable task must implement java.lang.Comparable interface!");
            }
        }

        @Override
        public int compareTo(ComparableFutureTask o) {
            return this.cmp.compareTo(o.cmp);
        }
    }
}
