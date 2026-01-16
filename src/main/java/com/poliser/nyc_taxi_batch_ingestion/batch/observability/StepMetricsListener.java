package com.poliser.nyc_taxi_batch_ingestion.batch.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.ChunkListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepExecution;

import java.time.Duration;

public class StepMetricsListener implements StepExecutionListener, ChunkListener {

    private static final Logger log =
            LoggerFactory.getLogger(StepMetricsListener.class);

    private final MeterRegistry meterRegistry;
    private Timer.Sample timerSample;

    private long startTime;
    private long lastReadCount;
    private long lastWriteCount;
    private long lastProcessSkipCount;

    public StepMetricsListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        startTime = System.currentTimeMillis();
        timerSample = Timer.start(meterRegistry);
        lastReadCount = 0;
        lastWriteCount = 0;
        lastProcessSkipCount = 0;

        log.info("Step [{}] started", stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        var duration = Duration.ofMillis(System.currentTimeMillis() - startTime);

        timerSample.stop(
                Timer.builder("batch.step.duration")
                        .tag("step", stepExecution.getStepName())
                        .register(meterRegistry)
        );

        log.info("""
                Step [{}] finished
                Read: {}
                Written: {}
                Filtered: {}
                Process Skipped: {}
                Write Skipped: {}
                Rollbacks: {}
                Duration: {} ms
                Throughput: {} records/sec
                """,
                stepExecution.getStepName(),
                stepExecution.getReadCount(),
                stepExecution.getWriteCount(),
                stepExecution.getFilterCount(),
                stepExecution.getProcessSkipCount(),
                stepExecution.getWriteSkipCount(),
                stepExecution.getRollbackCount(),
                duration.toMillis(),
                calculateThroughput(stepExecution, duration)
        );

        return stepExecution.getExitStatus();
    }

    @Override
    public void beforeChunk(ChunkContext context) {
        // No-op
    }

    @Override
    public void afterChunk(ChunkContext context) {
        StepExecution stepExecution = context.getStepContext().getStepExecution();

        long readDelta = stepExecution.getReadCount() - lastReadCount;
        long writeDelta = stepExecution.getWriteCount() - lastWriteCount;
        long skipDelta = stepExecution.getProcessSkipCount() - lastProcessSkipCount;

        if (readDelta > 0) {
            meterRegistry.counter(
                    "batch.step.read",
                    "step", stepExecution.getStepName()
            ).increment(readDelta);
        }

        if (writeDelta > 0) {
            meterRegistry.counter(
                    "batch.step.write",
                    "step", stepExecution.getStepName()
            ).increment(writeDelta);
        }

        if (skipDelta > 0) {
            meterRegistry.counter(
                    "batch.step.skip",
                    "step", stepExecution.getStepName()
            ).increment(skipDelta);
        }

        lastReadCount = stepExecution.getReadCount();
        lastWriteCount = stepExecution.getWriteCount();
        lastProcessSkipCount = stepExecution.getProcessSkipCount();
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        // No-op
    }

    private long calculateThroughput(StepExecution stepExecution, Duration duration) {
        if (duration.isZero()) {
            return 0;
        }
        return stepExecution.getWriteCount() * 1000 / duration.toMillis();
    }
}
