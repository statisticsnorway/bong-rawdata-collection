package no.ssb.dc.collection.api.source;

import no.ssb.dc.collection.api.csv.CsvParser;

import java.util.function.Function;

public interface CsvHandler<T extends RepositoryKey> extends AutoCloseable {

    void prepare(Function<CsvParser.Record, T> produceSortableKey);

    void produce();
}
