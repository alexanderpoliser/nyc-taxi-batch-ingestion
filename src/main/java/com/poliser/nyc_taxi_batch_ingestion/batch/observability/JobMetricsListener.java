package com.poliser.nyc_taxi_batch_ingestion.batch.observability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;

import java.time.Duration;

public class JobMetricsListener implements JobExecutionListener {

    private static final Logger log =
            LoggerFactory.getLogger(JobMetricsListener.class);

    private long startTime;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        startTime = System.currentTimeMillis();
        log.info("Job [{}] started", jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {

        var duration = Duration.ofMillis(System.currentTimeMillis() - startTime);

        log.info("""
                Job [{}] finished
                Status: {}
                Exit Status: {}
                Duration: {} ms
                """,
                jobExecution.getJobInstance().getJobName(),
                jobExecution.getStatus(),
                jobExecution.getExitStatus(),
                duration.toMillis()
        );
    }
}
