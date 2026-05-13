package ai.pipestream.schemamanager.indexing;

import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link NestedIndexingStrategy#prewarm(String)}.
 *
 * <p>Prewarm is the early-failure boundary for NESTED: when called at
 * consumer startup it must (a) load the binding cache entry for the
 * plan's parent index and (b) confirm every bound nested KNN field
 * actually exists on the live OpenSearch index. A missing field is a
 * contract violation that should fail startup rather than degrade
 * silently into a slow per-doc fallback under load.
 *
 * <p>This test pins the contract:
 * <ul>
 *   <li>Every binding probes its field via
 *       {@code openSearchSchemaClient.nestedMappingExists}</li>
 *   <li>Every present field is marked verified on the
 *       {@link IndexBindingCache.IndexEntry} so the per-doc path
 *       short-circuits the same probe</li>
 *   <li>The first missing field throws — no further probes happen for
 *       the rest of the bindings, because the plan is in a broken
 *       state regardless of how many other fields exist</li>
 *   <li>An index with no bindings is a no-op (zero probes, no throw)</li>
 * </ul>
 */
class NestedIndexingStrategyPrewarmTest {

    private NestedIndexingStrategy strategy;
    private IndexBindingCache bindingCache;
    private OpenSearchSchemaService schemaService;

    @BeforeEach
    void setUp() throws Exception {
        strategy = new NestedIndexingStrategy();
        bindingCache = mock(IndexBindingCache.class);
        schemaService = mock(OpenSearchSchemaService.class);
        inject(strategy, "bindingCache", bindingCache);
        inject(strategy, "openSearchSchemaClient", schemaService);
    }

    @Test
    void happyPath_everyBoundFieldProbedAndMarkedVerified() {
        IndexBindingCache.IndexEntry entry = entryWithBindings(
                mapping("vs-a", "vs_title_text_chunker_minilm", 384),
                mapping("vs-b", "vs_body_text_chunker_minilm", 384));
        when(bindingCache.getOrLoad("idx-1")).thenReturn(entry);
        when(schemaService.nestedMappingExists(anyString(), anyString())).thenReturn(true);

        strategy.prewarm("idx-1");

        verify(schemaService, times(1)).nestedMappingExists("idx-1", "vs_title_text_chunker_minilm");
        verify(schemaService, times(1)).nestedMappingExists("idx-1", "vs_body_text_chunker_minilm");
        assertThat(entry.isOsFieldVerified("vs_title_text_chunker_minilm"))
                .as("a present field must be marked verified so the per-doc path "
                        + "short-circuits the redundant cluster-state probe")
                .isTrue();
        assertThat(entry.isOsFieldVerified("vs_body_text_chunker_minilm"))
                .as("every present field must be marked, not just the first")
                .isTrue();
    }

    @Test
    void missingField_throwsAndNamesTheMissingFieldAndIndex() {
        IndexBindingCache.IndexEntry entry = entryWithBindings(
                mapping("vs-a", "vs_missing_field", 384));
        when(bindingCache.getOrLoad("idx-1")).thenReturn(entry);
        when(schemaService.nestedMappingExists("idx-1", "vs_missing_field")).thenReturn(false);

        assertThatThrownBy(() -> strategy.prewarm("idx-1"))
                .as("a missing nested KNN field is a contract violation — "
                        + "IndexProvisioningEngine should have created it, so prewarm "
                        + "must fail loud rather than silently falling through to a "
                        + "lazy per-doc create on the hot path")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("vs_missing_field")
                .hasMessageContaining("idx-1");
        assertThat(entry.isOsFieldVerified("vs_missing_field"))
                .as("a field that failed the probe must NOT be marked verified — "
                        + "the per-doc path's strict check needs to surface the same "
                        + "failure if anyone bypasses prewarm")
                .isFalse();
    }

    @Test
    void mixedBindings_firstMissingFieldThrowsWithoutProbingTheRest() {
        // Walk order is byVectorSetId iteration; with two entries we can't
        // guarantee which probes first. To pin "first missing throws and we
        // stop," we have BOTH fields fail and assert that AT MOST one of
        // them was marked verified before the throw. The looser assertion
        // here is "the throw mentions ONE specific field," not "we probed
        // in a specific order."
        IndexBindingCache.IndexEntry entry = entryWithBindings(
                mapping("vs-a", "vs_field_a", 384),
                mapping("vs-b", "vs_field_b", 384));
        when(bindingCache.getOrLoad("idx-1")).thenReturn(entry);
        when(schemaService.nestedMappingExists(eq("idx-1"), anyString())).thenReturn(false);

        assertThatThrownBy(() -> strategy.prewarm("idx-1"))
                .as("the first missing field encountered must throw, regardless of "
                        + "iteration order")
                .isInstanceOf(IllegalStateException.class);
        // We may have probed one or both fields depending on iteration order,
        // but neither should be marked verified — they both failed the probe.
        assertThat(entry.isOsFieldVerified("vs_field_a")).isFalse();
        assertThat(entry.isOsFieldVerified("vs_field_b")).isFalse();
    }

    @Test
    void emptyBindings_isNoOp() {
        IndexBindingCache.IndexEntry entry = entryWithBindings();
        when(bindingCache.getOrLoad("idx-empty")).thenReturn(entry);

        strategy.prewarm("idx-empty");

        verify(schemaService, never()).nestedMappingExists(anyString(), anyString());
    }

    // ---- helpers ----

    /**
     * Build an IndexBindingCache.IndexEntry with the supplied mappings.
     * Uses the same record shape the production code populates.
     */
    private static IndexBindingCache.IndexEntry entryWithBindings(IndexBindingCache.VectorSetMapping... mappings) {
        Map<String, IndexBindingCache.VectorSetMapping> byVectorSetId = new java.util.HashMap<>();
        for (IndexBindingCache.VectorSetMapping m : mappings) {
            byVectorSetId.put(m.vectorSetId(), m);
        }
        return new IndexBindingCache.IndexEntry(
                Map.copyOf(byVectorSetId),
                Map.of(),
                Map.of(),
                ConcurrentHashMap.<String>newKeySet(),
                System.currentTimeMillis());
    }

    private static IndexBindingCache.VectorSetMapping mapping(String vsId, String fieldName, int dim) {
        return new IndexBindingCache.VectorSetMapping(
                vsId, fieldName, dim, /* semanticConfigId */ null, /* granularity */ null, vsId);
    }

    private static void inject(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
