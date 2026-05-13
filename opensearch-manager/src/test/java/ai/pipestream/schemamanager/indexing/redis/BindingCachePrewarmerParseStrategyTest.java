package ai.pipestream.schemamanager.indexing.redis;

import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.schemamanager.entity.IndexPlanEntity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BindingCachePrewarmer#parseStrategy(IndexPlanEntity)}.
 *
 * <p>The parser is the boundary between {@code IndexPlanEntity.indexingStrategy}
 * (a free-form String column) and the proto {@link IndexingStrategy} enum.
 * Real-world writers have stamped that column in two forms historically:
 *
 * <ul>
 *   <li><b>Short form</b> &mdash; {@code "NESTED"}, {@code "CHUNK_COMBINED"},
 *       {@code "SEPARATE_INDICES"}</li>
 *   <li><b>Full enum-name form</b> &mdash; {@code "INDEXING_STRATEGY_NESTED"},
 *       etc.</li>
 * </ul>
 *
 * <p>Both forms MUST resolve to the same {@link IndexingStrategy} value.
 * The earlier prewarm-walk-skips-every-plan bug surfaced precisely
 * because the parser prepended {@code "INDEXING_STRATEGY_"} unconditionally,
 * producing {@code "INDEXING_STRATEGY_INDEXING_STRATEGY_NESTED"} for the
 * full-form rows and failing {@code IndexingStrategy.valueOf}.
 */
class BindingCachePrewarmerParseStrategyTest {

    @Test
    void shortForm_NESTED_resolves() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("NESTED")))
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_NESTED);
    }

    @Test
    void shortForm_CHUNK_COMBINED_resolves() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("CHUNK_COMBINED")))
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);
    }

    @Test
    void shortForm_SEPARATE_INDICES_resolves() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("SEPARATE_INDICES")))
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES);
    }

    @Test
    void fullForm_INDEXING_STRATEGY_NESTED_resolves() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("INDEXING_STRATEGY_NESTED")))
                .as("rows persisted in proto-enum-name form must resolve the same as the short form; "
                        + "regression here is what made every plan get skipped in dev")
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_NESTED);
    }

    @Test
    void fullForm_INDEXING_STRATEGY_CHUNK_COMBINED_resolves() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("INDEXING_STRATEGY_CHUNK_COMBINED")))
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);
    }

    @Test
    void fullForm_INDEXING_STRATEGY_SEPARATE_INDICES_resolves() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("INDEXING_STRATEGY_SEPARATE_INDICES")))
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES);
    }

    @Test
    void lowerCaseInput_isAcceptedAndCaseFolded() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("nested")))
                .as("the parser is tolerant of casing — the DB column has no case discipline")
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_NESTED);
    }

    @Test
    void whitespaceAroundValue_isTrimmed() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("  NESTED  ")))
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_NESTED);
    }

    @Test
    void blankValue_returnsNull() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("")))
                .as("blank strategy is a contract violation surfaced as a skipped plan, "
                        + "not a crash that aborts the whole prewarm walk")
                .isNull();
    }

    @Test
    void nullValue_returnsNull() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan(null))).isNull();
    }

    @Test
    void unknownValue_returnsNull() {
        assertThat(BindingCachePrewarmer.parseStrategy(plan("INDEXING_STRATEGY_GIBBERISH"))).isNull();
        assertThat(BindingCachePrewarmer.parseStrategy(plan("GIBBERISH"))).isNull();
    }

    private static IndexPlanEntity plan(String strategy) {
        IndexPlanEntity p = new IndexPlanEntity();
        p.indexingStrategy = strategy;
        return p;
    }
}
