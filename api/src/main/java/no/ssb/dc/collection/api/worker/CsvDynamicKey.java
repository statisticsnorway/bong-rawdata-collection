package no.ssb.dc.collection.api.worker;

import no.ssb.dc.collection.api.source.GenericKey;
import no.ssb.dc.collection.api.utils.ULIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CsvDynamicKey extends GenericKey {

    private static final Logger LOG = LoggerFactory.getLogger(CsvDynamicKey.class);
    private static final String FUNCTION_SPECIFIER = "FUNCTION::";

    private final CsvSpecification specification;

    public CsvDynamicKey(CsvSpecification specification) {
        super();
        this.specification = specification;
    }

    public CsvDynamicKey(CsvSpecification specification, Map<String, Object> values) {
        super(values);
        this.specification = specification;
    }

    public static CsvDynamicKey create(CsvSpecification specification, Map<String, Object> values) {
        return new CsvDynamicKey(specification, values);
    }

    @Override
    public Map<String, Class<?>> keys() {
        return specification.groupByColumns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().type, (e1, e2) -> e2, LinkedHashMap::new));
    }

    @Override
    public List<String> positionKeys() {
        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, CsvSpecification.Position> entry : specification.positionKeys.entrySet()) {
            if (entry.getValue() instanceof CsvSpecification.PositionColumnKey) {
                keys.add(entry.getKey());

            } else if (entry.getValue() instanceof CsvSpecification.PositionColumnFunction) {
                keys.add(FUNCTION_SPECIFIER + entry.getKey());

            } else {
                throw new UnsupportedOperationException();
            }
        }
        return keys;
    }

    @Override
    public String toPosition() {
        Objects.requireNonNull(positionKeys());
        List<String> positionKeys = new ArrayList<>();
        for (String key : positionKeys()) {
            if (key.startsWith(FUNCTION_SPECIFIER)) {
                CsvSpecification.PositionColumnFunction function = getFunction(key);
                Object generatedValue;
                switch (function.generator) {
                    case SEQUENCE -> generatedValue = SequenceGenerator.next();
                    case ULID -> generatedValue = ULIDGenerator.toUUID(ULIDGenerator.generate());
                    case UUID -> generatedValue = UUIDGenerator.generate();
                    default -> throw new RuntimeException("Function type " + function.generator + " NOT supported!");
                }
                positionKeys.add(generatedValue.toString());
            } else {
                Object value = values.get(key);
                positionKeys.add(value.toString());
            }
        }
        return String.join(".", positionKeys);
    }

    public CsvSpecification.PositionColumnFunction getFunction(String key) {
        return (CsvSpecification.PositionColumnFunction) specification.positionKeys.get(key.replace(FUNCTION_SPECIFIER, ""));
    }
}
