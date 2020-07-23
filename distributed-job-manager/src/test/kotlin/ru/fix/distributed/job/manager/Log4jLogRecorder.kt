package ru.fix.distributed.job.manager

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.WriterAppender
import org.apache.logging.log4j.core.layout.PatternLayout
import java.io.CharArrayWriter

class Log4jLogRecorder : AutoCloseable{
    private val target = CharArrayWriter()
    private val logger = LogManager.getRootLogger() as org.apache.logging.log4j.core.Logger
    private val appender = WriterAppender.createAppender(
            PatternLayout.newBuilder().withPattern("%level %msg").build(),
            null,
            target,
            "test-appender",
            false,
            false)
    init {
        appender.start()
        logger.addAppender(appender)
    }

    override fun close() {
        logger.removeAppender(appender)
        appender.stop()
    }

    fun getContent() = target.toString()
}