package no.ssb.dc.bong.commons.rawdata;

import java.util.Map;
import java.util.function.Consumer;

abstract public class BaseCsvRepository<T extends RepositoryKey> {

    abstract public String csvHeader();

    abstract public void prepare();

    abstract public void consume(Consumer<Map<T, String>> entrySetCallback);

    abstract public void produce();
}
