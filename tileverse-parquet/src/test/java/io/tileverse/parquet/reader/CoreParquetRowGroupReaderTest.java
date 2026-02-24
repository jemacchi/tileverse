/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.parquet.CloseableIterator;
import io.tileverse.parquet.ParquetDataset;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoreParquetRowGroupReaderTest {

    private Path sampleFile;

    @BeforeEach
    void setUp() throws Exception {
        sampleFile = resourcePath("geoparquet/sample-geoparquet.parquet");
    }

    @Test
    void read_fixtureFile() throws Exception {
        ParquetDataset dataset = ParquetDataset.open(new LocalInputFile(sampleFile));
        List<GenericRecord> records = readAll(dataset.read());
        assertThat(records).hasSize(3);
    }

    @Test
    void read_withFilter_onFixtureFile() throws Exception {
        FilterPredicate filter =
                FilterApi.eq(FilterApi.binaryColumn("id"), org.apache.parquet.io.api.Binary.fromString("fid-2"));
        ParquetDataset dataset = ParquetDataset.open(new LocalInputFile(sampleFile));
        List<GenericRecord> records = readAll(dataset.read(filter));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).get("id").toString()).isEqualTo("fid-2");
    }

    @Test
    void privateDecompress_throwsForUnsupportedCodec() throws Exception {
        Method decompress = CoreParquetRowGroupReader.class.getDeclaredMethod(
                "decompress", CompressionCodec.class, byte[].class, int.class);
        decompress.setAccessible(true);

        assertThatThrownBy(() -> decompress.invoke(null, CompressionCodec.BROTLI, new byte[] {1, 2, 3}, 3))
                .hasRootCauseInstanceOf(java.io.IOException.class)
                .hasRootCauseMessage("Unsupported compression codec in core reader: BROTLI");
    }

    private static <T> List<T> readAll(CloseableIterator<T> iterator) throws Exception {
        try (iterator) {
            List<T> result = new ArrayList<>();
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
            return result;
        }
    }

    private static Path resourcePath(String resource) throws Exception {
        URL url = CoreParquetRowGroupReaderTest.class.getClassLoader().getResource(resource);
        if (url == null) {
            throw new IllegalStateException("Missing resource: " + resource);
        }
        return Path.of(url.toURI());
    }
}
