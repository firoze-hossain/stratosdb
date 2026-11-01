package com.stratosdb.sql.statistics;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class MostCommonValuesTest {

    @Test
    public void testEmptyMcv() {
        MostCommonValues mcv = MostCommonValues.empty();
        assertTrue(mcv.isEmpty());
        assertEquals(0, mcv.size());
        assertEquals(0.0, mcv.getFrequency("val"));
        assertFalse(mcv.contains("val"));
    }

    @Test
    public void testMcvLookupAndFrequencies() {
        List<Object> values = List.of("US", "CA", "GB");
        List<Double> freqs = List.of(0.70, 0.15, 0.10);
        MostCommonValues mcv = new MostCommonValues(values, freqs);

        assertEquals(3, mcv.size());
        assertEquals(0.95, mcv.getSumFrequency(), 1e-6);

        assertTrue(mcv.contains("US"));
        assertTrue(mcv.contains("ca")); // case-insensitive match
        assertFalse(mcv.contains("FR"));

        assertEquals(0.70, mcv.getFrequency("US"), 1e-6);
        assertEquals(0.15, mcv.getFrequency("ca"), 1e-6);
        assertEquals(0.0, mcv.getFrequency("FR"), 1e-6);
    }

    @Test
    public void testNumericRangeFrequency() {
        List<Object> values = List.of(10, 20, 30, 40);
        List<Double> freqs = List.of(0.40, 0.25, 0.15, 0.10);
        MostCommonValues mcv = new MostCommonValues(values, freqs);

        // Range [15, 35] includes 20 (0.25) and 30 (0.15) -> 0.40
        assertEquals(0.40, mcv.rangeFrequency(15.0, 35.0), 1e-6);
        // Range <= 20 includes 10 (0.40) and 20 (0.25) -> 0.65
        assertEquals(0.65, mcv.rangeFrequency(null, 20.0), 1e-6);
        // Range >= 30 includes 30 (0.15) and 40 (0.10) -> 0.25
        assertEquals(0.25, mcv.rangeFrequency(30.0, null), 1e-6);
    }

    @Test
    public void testJsonSerialization() {
        List<Object> values = List.of("alpha", "beta", "gamma");
        List<Double> freqs = List.of(0.5, 0.3, 0.15);
        MostCommonValues mcv = new MostCommonValues(values, freqs);

        String valJson = mcv.toJsonValues();
        String freqJson = mcv.toJsonFrequencies();
        assertNotNull(valJson);
        assertNotNull(freqJson);

        MostCommonValues restored = MostCommonValues.fromJson(valJson, freqJson);
        assertEquals(3, restored.size());
        assertEquals(0.5, restored.getFrequency("alpha"), 1e-6);
        assertEquals(0.3, restored.getFrequency("beta"), 1e-6);
        assertEquals(0.15, restored.getFrequency("gamma"), 1e-6);
    }
}
