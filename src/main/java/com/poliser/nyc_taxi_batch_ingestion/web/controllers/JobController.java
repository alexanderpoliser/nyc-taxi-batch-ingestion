package com.poliser.nyc_taxi_batch_ingestion.web.controllers;


import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.function.ToLongFunction;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobOperator jobOperator;
    private final JobRepository jobRepository;
    private final Job taxiIngestionJob;

    public JobController(
            JobOperator jobOperator,
            JobRepository jobRepository,
            Job taxiIngestionJob
    ) {
        this.jobOperator = jobOperator;
        this.jobRepository = jobRepository;
        this.taxiIngestionJob = taxiIngestionJob;
    }

    @PostMapping("/run")
    public ResponseEntity<?> run() throws Exception {

        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobOperator.start(taxiIngestionJob, params);

        return ResponseEntity.ok(
                Map.of(
                        "jobExecutionId", execution.getId(),
                        "status", execution.getStatus().toString()
                )
        );
    }

    @PostMapping("/stop/{executionId}")
    public ResponseEntity<?> stop(@PathVariable Long executionId) throws Exception {

        JobExecution jobExecution = jobRepository.getJobExecution(executionId);

        if (jobExecution == null) {
            return ResponseEntity.notFound().build();
        }

        jobOperator.stop(jobExecution);

        return ResponseEntity.ok(
                Map.of(
                        "message", "Stop signal sent to execution " + executionId,
                        "status", jobExecution.getStatus().toString()
                )
        );
    }


    @PostMapping("/restart/{executionId}")
    public ResponseEntity<?> restart(@PathVariable Long executionId) throws Exception {

        JobExecution jobExecution = jobRepository.getJobExecution(executionId);

        if (jobExecution == null) {
            return ResponseEntity.notFound().build();
        }

        JobExecution restarted = jobOperator.restart(jobExecution);

        return ResponseEntity.ok(
                Map.of(
                        "originalExecutionId", executionId,
                        "newExecutionId", restarted.getId(),
                        "status", restarted.getStatus().toString()
                )
        );
    }

    @GetMapping("/status/{executionId}")
    public ResponseEntity<?> status(@PathVariable Long executionId) {

        JobExecution execution =
                jobRepository.getJobExecution(executionId);

        if (execution == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(
                Map.of(
                        "status", execution.getStatus(),
                        "startTime", execution.getStartTime() != null
                                ? execution.getStartTime() : "not started",
                        "endTime", execution.getEndTime() != null
                                ? execution.getEndTime() : "running",
                        "exitStatus", execution.getExitStatus().getExitCode(),
                        "readCount", getStepMetric(execution, StepExecution::getReadCount),
                        "writeCount", getStepMetric(execution, StepExecution::getWriteCount),
                        "skipCount", getStepMetric(execution, StepExecution::getProcessSkipCount)
                )
        );
    }

    private long getStepMetric(JobExecution execution,
                               ToLongFunction<StepExecution> extractor) {
        return execution.getStepExecutions().stream()
                .mapToLong(extractor)
                .sum();
    }
}

