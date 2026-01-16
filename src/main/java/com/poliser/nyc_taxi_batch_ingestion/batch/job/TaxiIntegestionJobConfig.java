package com.poliser.nyc_taxi_batch_ingestion.batch.job;

import com.poliser.nyc_taxi_batch_ingestion.batch.observability.JobMetricsListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class TaxiIntegestionJobConfig {

    @Bean
    public Job taxiIngestionJob(
            JobRepository jobRepository,
            Step taxiIngestionStep
    ) {
        return new JobBuilder("taxiIngestionJob", jobRepository)
                .listener(new JobMetricsListener())
                .start(taxiIngestionStep)
                .build();
    }
}
