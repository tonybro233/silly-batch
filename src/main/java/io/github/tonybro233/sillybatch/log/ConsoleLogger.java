package io.github.tonybro233.sillybatch.log;

import org.slf4j.Logger;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

import java.io.PrintStream;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import static org.slf4j.spi.LocationAwareLogger.*;

/**
 * Simple implementation of {@link Logger} that sends all enabled log messages
 * to the console({@code System.out}). This is the backup implementation so
 * that it has no related {@link org.slf4j.LoggerFactory}, only available when
 * no slf4j impl is provided.
 *
 * <p>Sample output follows.
 * <pre>
 *  17:50:00 INFO  - Message content is what ...
 *  17:50:00 INFO  - Opening record writer ...
 *  17:50:01 INFO  - Opening record processor ...
 *  17:50:02 ERROR - Error reading records
 *  java.lang.RuntimeException: What the hell?
 *      at TestReader.read(Testor.java:39)
 *      at TestReader.read(Testor.java:30)
 *      ...
 *  17:50:01 INFO  - Exit process ...
 * </pre>
 *
 * @author tony
 */
public class ConsoleLogger extends MarkerIgnoringBase {

    private volatile int currentLogLevel = INFO_INT;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final PrintStream targetStream = System.out;

    public ConsoleLogger() {
    }

    public ConsoleLogger(int currentLogLevel) {
        this.currentLogLevel = currentLogLevel;
    }

    public void setCurrentLogLevel(int currentLogLevel) {
        this.currentLogLevel = currentLogLevel;
    }

    private void log(int level, String message, Throwable t) {
        if (!isLevelEnabled(level)) {
            return;
        }

        StringBuilder buf = new StringBuilder(32);
        buf.append(FORMATTER.format(LocalTime.now())).append(' ')
                .append(renderLevel(level)).append(" - ")
                .append(message);

        write(buf.toString(), t);
    }

    private boolean isLevelEnabled(int logLevel) {
        return (logLevel >= currentLogLevel);
    }

    private void formatAndLog(int level, String format, Object arg1, Object arg2) {
        FormattingTuple tp = MessageFormatter.format(format, arg1, arg2);
        log(level, tp.getMessage(), tp.getThrowable());
    }

    private void formatAndLog(int level, String format, Object... arguments) {
        FormattingTuple tp = MessageFormatter.arrayFormat(format, arguments);
        log(level, tp.getMessage(), tp.getThrowable());
    }

    private String renderLevel(int level) {
        switch (level) {
            case TRACE_INT:
                return "TRACE";
            case DEBUG_INT:
                return ("DEBUG");
            case INFO_INT:
                return "INFO ";
            case WARN_INT:
                return "WARN ";
            case ERROR_INT:
                return "ERROR";
            default:
                throw new IllegalStateException("Unrecognized level [" + level + "]");
        }
    }

    // ensure the message and stacktrace print at the same time
    private synchronized void write(String str, Throwable t) {
        targetStream.println(str);
        if (t != null) {
            t.printStackTrace(targetStream);
        }
        targetStream.flush();
    }

    @Override
    public boolean isTraceEnabled() {
        return true;
    }

    @Override
    public void trace(String msg) {
        log(TRACE_INT, msg, null);
    }

    @Override
    public void trace(String format, Object arg) {
        formatAndLog(TRACE_INT, format, arg, null);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        formatAndLog(TRACE_INT, format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        formatAndLog(TRACE_INT, format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        log(TRACE_INT, msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public void debug(String msg) {
        log(DEBUG_INT, msg, null);
    }

    @Override
    public void debug(String format, Object arg) {
        formatAndLog(DEBUG_INT, format, arg, null);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        formatAndLog(DEBUG_INT, format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        formatAndLog(DEBUG_INT, format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        log(DEBUG_INT, msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public void info(String msg) {
        log(INFO_INT, msg, null);
    }

    @Override
    public void info(String format, Object arg) {
        formatAndLog(INFO_INT, format, arg, null);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        formatAndLog(INFO_INT, format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        formatAndLog(INFO_INT, format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        log(INFO_INT, msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public void warn(String msg) {
        log(WARN_INT, msg, null);
    }

    @Override
    public void warn(String format, Object arg) {
        formatAndLog(WARN_INT, format, arg, null);
    }

    @Override
    public void warn(String format, Object... arguments) {
        formatAndLog(WARN_INT, format, arguments);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        formatAndLog(WARN_INT, format, arg1, arg2);
    }

    @Override
    public void warn(String msg, Throwable t) {
        log(WARN_INT, msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public void error(String msg) {
        log(ERROR_INT, msg, null);
    }

    @Override
    public void error(String format, Object arg) {
        formatAndLog(ERROR_INT, format, arg, null);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        formatAndLog(ERROR_INT, format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        formatAndLog(ERROR_INT, format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        log(ERROR_INT, msg, t);
    }
}
