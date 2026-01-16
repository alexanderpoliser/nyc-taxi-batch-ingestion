package com.poliser.nyc_taxi_batch_ingestion.batch.reader;

import com.poliser.nyc_taxi_batch_ingestion.domain.model.TaxiCsvRow;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Configuration
public class TaxiCsvReaderConfig {

    private static final DateTimeFormatter DATE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Bean
    public LineTrackingItemReader<TaxiCsvRow> taxiCsvReader(
            @Value("${taxi.ingestion.source-file:data/yellow_tripdata.csv}") String sourceFile
    ) {

        var flatFileReader = new FlatFileItemReader<TaxiCsvRow>(
                new FileSystemResource(sourceFile),
                lineMapper()
        );

        flatFileReader.setLinesToSkip(1);
        flatFileReader.setStrict(true);

        return new LineTrackingItemReader<TaxiCsvRow>(flatFileReader);

    }

    private DefaultLineMapper<TaxiCsvRow> lineMapper() {

        var tokenizer = getDelimitedLineTokenizer();

        var mapper = new DefaultLineMapper<TaxiCsvRow>();
        mapper.setLineTokenizer(tokenizer);
        mapper.setFieldSetMapper(fieldSet -> {

            var pickup = fieldSet.readString("pickupDatetime");
            var dropoff = fieldSet.readString("dropoffDatetime");

            return new TaxiCsvRow(
                    null, // lineNumber will be set by LineTrackingItemReader
                    fieldSet.readInt("vendorId"),
                    pickup == null || pickup.isBlank()
                            ? null
                            : LocalDateTime.parse(pickup, DATE_TIME),
                    dropoff == null || dropoff.isBlank()
                            ? null
                            : LocalDateTime.parse(dropoff, DATE_TIME),
                    fieldSet.readInt("passengerCount"),
                    fieldSet.readBigDecimal("tripDistance"),
                    fieldSet.readBigDecimal("pickupLongitude"),
                    fieldSet.readBigDecimal("pickupLatitude"),
                    fieldSet.readInt("rateCodeId"),
                    fieldSet.readString("storeAndFwdFlag"),
                    fieldSet.readBigDecimal("dropoffLongitude"),
                    fieldSet.readBigDecimal("dropoffLatitude"),
                    fieldSet.readInt("paymentType"),
                    fieldSet.readBigDecimal("fareAmount"),
                    fieldSet.readBigDecimal("extra"),
                    fieldSet.readBigDecimal("mtaTax"),
                    fieldSet.readBigDecimal("tipAmount"),
                    fieldSet.readBigDecimal("tollsAmount"),
                    fieldSet.readBigDecimal("improvementSurcharge"),
                    fieldSet.readBigDecimal("totalAmount")
            );
        });

        return mapper;
    }

    private static DelimitedLineTokenizer getDelimitedLineTokenizer() {
        var tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setStrict(false);

        tokenizer.setNames(
                "vendorId",
                "pickupDatetime",
                "dropoffDatetime",
                "passengerCount",
                "tripDistance",
                "pickupLongitude",
                "pickupLatitude",
                "rateCodeId",
                "storeAndFwdFlag",
                "dropoffLongitude",
                "dropoffLatitude",
                "paymentType",
                "fareAmount",
                "extra",
                "mtaTax",
                "tipAmount",
                "tollsAmount",
                "improvementSurcharge",
                "totalAmount"
        );
        return tokenizer;
    }
}
