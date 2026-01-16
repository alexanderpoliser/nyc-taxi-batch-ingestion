package com.poliser.nyc_taxi_batch_ingestion.batch.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;

import org.springframework.batch.core.listener.ChunkListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.Chunk;
import java.time.Duration;

public class StepMetricsListener implements StepExecutionListener, ChunkListener<Object, Object> {

    private static final Logger log =
            LoggerFactory.getLogger(StepMetricsListener.class);

    private final MeterRegistry meterRegistry;
    private Timer.Sample timerSample;

    private long startTime;
    private long lastReadCount;
    private long lastWriteCount;
    private long lastProcessSkipCount;
    private StepExecution currentStepExecution;

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
        currentStepExecution = stepExecution;

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
    public void beforeChunk(Chunk<Object> chunk) {
        // No-op
    }

    @Override
    public void afterChunk(Chunk<Object> chunk) {
        if (currentStepExecution == null) {
            return;
        }

        long readDelta = currentStepExecution.getReadCount() - lastReadCount;
        long writeDelta = currentStepExecution.getWriteCount() - lastWriteCount;
        long skipDelta = currentStepExecution.getProcessSkipCount() - lastProcessSkipCount;

        if (readDelta > 0) {
            meterRegistry.counter(
                    "batch.step.read",
                    "step", currentStepExecution.getStepName()
            ).increment(readDelta);
        }

        if (writeDelta > 0) {
            meterRegistry.counter(
                    "batch.step.write",
                    "step", currentStepExecution.getStepName()
            ).increment(writeDelta);
        }

        if (skipDelta > 0) {
            meterRegistry.counter(
                    "batch.step.skip",
                    "step", currentStepExecution.getStepName()
            ).increment(skipDelta);
        }

        lastReadCount = currentStepExecution.getReadCount();
        lastWriteCount = currentStepExecution.getWriteCount();
        lastProcessSkipCount = currentStepExecution.getProcessSkipCount();
    }

    @Override
    public void onChunkError(Exception exception, Chunk<Object> chunk) {
        // No-op
    }

    private long calculateThroughput(StepExecution stepExecution, Duration duration) {
        if (duration.isZero()) {
            return 0;
        }
        return stepExecution.getWriteCount() * 1000 / duration.toMillis();
    }
}
