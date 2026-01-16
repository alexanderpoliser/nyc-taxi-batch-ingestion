package com.poliser.nyc_taxi_batch_ingestion.batch.step;

import com.poliser.nyc_taxi_batch_ingestion.batch.observability.StepMetricsListener;
import com.poliser.nyc_taxi_batch_ingestion.batch.reader.LineTrackingItemReader;
import com.poliser.nyc_taxi_batch_ingestion.domain.model.TaxiCsvRow;
import com.poliser.nyc_taxi_batch_ingestion.domain.model.TaxiTripRaw;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class TaxiStepConfig {

    private static final int CHUNK_SIZE = 1_000;

    @Bean
    public Step taxiTripIngestionStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            LineTrackingItemReader<?> reader,
            ItemProcessor<TaxiCsvRow, TaxiTripRaw> processor,
            JdbcBatchItemWriter<TaxiTripRaw> writer,
            MeterRegistry meterRegistry
    ) {
        return new StepBuilder("taxiTripIngestionStep", jobRepository)
                .<TaxiCsvRow, TaxiTripRaw>chunk(CHUNK_SIZE)
                .transactionManager(transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(new StepMetricsListener(meterRegistry))
                .faultTolerant()
                .skip(IllegalArgumentException.class)
                .skipLimit(100_000)
                .build();
    }
}
