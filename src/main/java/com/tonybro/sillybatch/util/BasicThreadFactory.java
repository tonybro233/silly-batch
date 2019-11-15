package com.tonybro.sillybatch.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copied from commons-lang3
 */
public class BasicThreadFactory implements ThreadFactory {

    /** A counter for the threads created by this factory. */
    private final AtomicLong threadCounter;

    /** Stores the wrapped factory. */
    private final ThreadFactory wrappedFactory;

    /** Stores the uncaught exception handler. */
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    /** Stores the naming pattern for newly created threads. */
    private final String namingPattern;

    /** Stores the priority. */
    private final Integer priority;

    /** Stores the daemon status flag. */
    private final Boolean daemon;

    /**
     * Creates a new instance of {@code ThreadFactoryImpl} and configures it
     * from the specified {@code Builder} object.
     *
     * @param builder the {@code Builder} object
     */
    private BasicThreadFactory(final Builder builder) {
        if (builder.wrappedFactory == null) {
            wrappedFactory = Executors.defaultThreadFactory();
        } else {
            wrappedFactory = builder.wrappedFactory;
        }

        namingPattern = builder.namingPattern;
        priority = builder.priority;
        daemon = builder.daemon;
        uncaughtExceptionHandler = builder.exceptionHandler;

        threadCounter = new AtomicLong();
    }

    /**
     * Returns the wrapped {@code ThreadFactory}. This factory is used for
     * actually creating threads. This method never returns <b>null</b>. If no
     * {@code ThreadFactory} was passed when this object was created, a default
     * thread factory is returned.
     *
     * @return the wrapped {@code ThreadFactory}
     */
    public final ThreadFactory getWrappedFactory() {
        return wrappedFactory;
    }

    /**
     * Returns the naming pattern for naming newly created threads. Result can
     * be <b>null</b> if no naming pattern was provided.
     *
     * @return the naming pattern
     */
    public final String getNamingPattern() {
        return namingPattern;
    }

    /**
     * Returns the daemon flag. This flag determines whether newly created
     * threads should be daemon threads. If <b>true</b>, this factory object
     * calls {@code setDaemon(true)} on the newly created threads. Result can be
     * <b>null</b> if no daemon flag was provided at creation time.
     *
     * @return the daemon flag
     */
    public final Boolean getDaemonFlag() {
        return daemon;
    }

    /**
     * Returns the priority of the threads created by this factory. Result can
     * be <b>null</b> if no priority was specified.
     *
     * @return the priority for newly created threads
     */
    public final Integer getPriority() {
        return priority;
    }

    /**
     * Returns the {@code UncaughtExceptionHandler} for the threads created by
     * this factory. Result can be <b>null</b> if no handler was provided.
     *
     * @return the {@code UncaughtExceptionHandler}
     */
    public final Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return uncaughtExceptionHandler;
    }

    /**
     * Returns the number of threads this factory has already created. This
     * class maintains an internal counter that is incremented each time the
     * {@link #newThread(Runnable)} method is invoked.
     *
     * @return the number of threads created by this factory
     */
    public long getThreadCount() {
        return threadCounter.get();
    }

    /**
     * Creates a new thread. This implementation delegates to the wrapped
     * factory for creating the thread. Then, on the newly created thread the
     * corresponding configuration options are set.
     *
     * @param runnable the {@code Runnable} to be executed by the new thread
     * @return the newly created thread
     */
    @Override
    public Thread newThread(final Runnable runnable) {
        final Thread thread = getWrappedFactory().newThread(runnable);
        initializeThread(thread);

        return thread;
    }

    /**
     * Initializes the specified thread. This method is called by
     * {@link #newThread(Runnable)} after a new thread has been obtained from
     * the wrapped thread factory. It initializes the thread according to the
     * options set for this factory.
     *
     * @param thread the thread to be initialized
     */
    private void initializeThread(final Thread thread) {

        if (getNamingPattern() != null) {
            final long count = threadCounter.incrementAndGet();
            thread.setName(String.format(getNamingPattern(), count));
        }

        if (getUncaughtExceptionHandler() != null) {
            thread.setUncaughtExceptionHandler(getUncaughtExceptionHandler());
        }

        if (getPriority() != null) {
            thread.setPriority(getPriority());
        }

        if (getDaemonFlag() != null) {
            thread.setDaemon(getDaemonFlag());
        }
    }

    /**
     * <p>
     * A <em>builder</em> class for creating instances of {@code
     * BasicThreadFactory}.
     * </p>
     * <p>
     * Using this builder class instances of {@code BasicThreadFactory} can be
     * created and initialized. The class provides methods that correspond to
     * the configuration options supported by {@code BasicThreadFactory}. Method
     * chaining is supported. Refer to the documentation of {@code
     * BasicThreadFactory} for a usage example.
     * </p>
     *
     */
    public static class Builder {

        /** The wrapped factory. */
        private ThreadFactory wrappedFactory;

        /** The uncaught exception handler. */
        private Thread.UncaughtExceptionHandler exceptionHandler;

        /** The naming pattern. */
        private String namingPattern;

        /** The priority. */
        private Integer priority;

        /** The daemon flag. */
        private Boolean daemon;

        /**
         * Sets the {@code ThreadFactory} to be wrapped by the new {@code
         * BasicThreadFactory}.
         *
         * @param factory the wrapped {@code ThreadFactory} (must not be
         * <b>null</b>)
         * @return a reference to this {@code Builder}
         * @throws NullPointerException if the passed in {@code ThreadFactory}
         * is <b>null</b>
         */
        public Builder wrappedFactory(final ThreadFactory factory) {
            if (null == factory) {
                throw new NullPointerException("Wrapped ThreadFactory must not be null!");
            }

            wrappedFactory = factory;
            return this;
        }

        /**
         * Sets the naming pattern to be used by the new {@code
         * BasicThreadFactory}.
         *
         * @param pattern the naming pattern (must not be <b>null</b>)
         * @return a reference to this {@code Builder}
         * @throws NullPointerException if the naming pattern is <b>null</b>
         */
        public Builder namingPattern(final String pattern) {
            if (null == pattern) {
                throw new NullPointerException("Naming pattern must not be null!");
            }

            namingPattern = pattern;
            return this;
        }

        /**
         * Sets the daemon flag for the new {@code BasicThreadFactory}. If this
         * flag is set to <b>true</b> the new thread factory will create daemon
         * threads.
         *
         * @param daemon the value of the daemon flag
         * @return a reference to this {@code Builder}
         */
        public Builder daemon(final boolean daemon) {
            this.daemon = Boolean.valueOf(daemon);
            return this;
        }

        /**
         * Sets the priority for the threads created by the new {@code
         * BasicThreadFactory}.
         *
         * @param priority the priority
         * @return a reference to this {@code Builder}
         */
        public Builder priority(final int priority) {
            this.priority = Integer.valueOf(priority);
            return this;
        }

        /**
         * Sets the uncaught exception handler for the threads created by the
         * new {@code BasicThreadFactory}.
         *
         * @param handler the {@code UncaughtExceptionHandler} (must not be
         * <b>null</b>)
         * @return a reference to this {@code Builder}
         * @throws NullPointerException if the exception handler is <b>null</b>
         */
        public Builder uncaughtExceptionHandler(
                final Thread.UncaughtExceptionHandler handler) {
            if (null == handler) {
                throw new NullPointerException("Uncaught exception handler must not be null!");
            }

            exceptionHandler = handler;
            return this;
        }

        /**
         * Resets this builder. All configuration options are set to default
         * values. Note: If the {@link #build()} method was called, it is not
         * necessary to call {@code reset()} explicitly because this is done
         * automatically.
         */
        public void reset() {
            wrappedFactory = null;
            exceptionHandler = null;
            namingPattern = null;
            priority = null;
            daemon = null;
        }

        /**
         * Creates a new {@code BasicThreadFactory} with all configuration
         * options that have been specified by calling methods on this builder.
         * After creating the factory {@link #reset()} is called.
         *
         * @return the new {@code BasicThreadFactory}
         */
        public ThreadFactory build() {
            final BasicThreadFactory factory = new BasicThreadFactory(this);
            reset();
            return factory;
        }
    }
}

