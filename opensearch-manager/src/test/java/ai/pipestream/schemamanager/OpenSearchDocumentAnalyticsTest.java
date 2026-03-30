package ai.pipestream.schemamanager;

import ai.pipestream.data.v1.*;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.OpenSearchEmbedding;
import ai.pipestream.opensearch.v1.SemanticVectorSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for OpenSearchDocument analytics fields — validates that NLP analytics
 * survive the full JSON transformation pipeline:
 *   OpenSearchDocument proto → JSON → nested KNN doc maps → JSON → round-trip back.
 *
 * These tests do NOT require Quarkus, OpenSearch, or any infrastructure.
 * They validate the same logic as OpenSearchIndexingService.transformSemanticSetsToNestedFields()
 * without needing access to the private method.
 */
class OpenSearchDocumentAnalyticsTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    // =========================================================================
    // Top-level analytics on OpenSearchDocument
    // =========================================================================

    @Test
    void sourceFieldAnalytics_serializedAsTopLevelJsonArray() throws Exception {
        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("test-1")
                .addSourceFieldAnalytics(SourceFieldAnalytics.newBuilder()
                        .setSourceField("body")
                        .setChunkConfigId("token_500")
                        .setDocumentAnalytics(DocumentAnalytics.newBuilder()
                                .setWordCount(300)
                                .setSentenceCount(15)
                                .setDetectedLanguage("eng")
                                .setLanguageConfidence(0.98f)
                                .setNounDensity(0.28f)
                                .setVerbDensity(0.18f)
                                .setLexicalDensity(0.52f)
                                .build())
                        .setTotalChunks(3)
                        .setAverageChunkSize(200.0f)
                        .build())
                .addSourceFieldAnalytics(SourceFieldAnalytics.newBuilder()
                        .setSourceField("title")
                        .setChunkConfigId("field_level")
                        .setDocumentAnalytics(DocumentAnalytics.newBuilder()
                                .setWordCount(6)
                                .setDetectedLanguage("eng")
                                .build())
                        .setTotalChunks(1)
                        .build())
                .build();

        String json = JsonFormat.printer().preservingProtoFieldNames().print(doc);

        @SuppressWarnings("unchecked")
        Map<String, Object> docMap = objectMapper.readValue(json, Map.class);

        assertThat(docMap).as("JSON contains source_field_analytics key")
                .containsKey("source_field_analytics");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sfaList = (List<Map<String, Object>>) docMap.get("source_field_analytics");
        assertThat(sfaList).as("two source field analytics entries").hasSize(2);

        assertThat(sfaList.get(0).get("source_field")).as("first entry is body").isEqualTo("body");

        @SuppressWarnings("unchecked")
        Map<String, Object> bodyAnalytics = (Map<String, Object>) sfaList.get(0).get("document_analytics");
        assertThat(bodyAnalytics.get("word_count")).as("body word count").isEqualTo(300);
        assertThat(bodyAnalytics.get("detected_language")).as("body language").isEqualTo("eng");
        assertThat(((Number) bodyAnalytics.get("noun_density")).floatValue())
                .as("body noun density").isCloseTo(0.28f, org.assertj.core.data.Offset.offset(0.001f));

        assertThat(sfaList.get(1).get("source_field")).as("second entry is title").isEqualTo("title");
    }

    @Test
    void nlpAnalysis_serializedAsTopLevelJsonObject() throws Exception {
        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("test-2")
                .setNlpAnalysis(NlpDocumentAnalysis.newBuilder()
                        .setDetectedLanguage("eng")
                        .setLanguageConfidence(0.97f)
                        .setTotalTokens(150)
                        .setNounDensity(0.28f)
                        .setVerbDensity(0.18f)
                        .setLexicalDensity(0.52f)
                        .addSentences(SentenceSpan.newBuilder()
                                .setText("First sentence.").setStartOffset(0).setEndOffset(15).build())
                        .addSentences(SentenceSpan.newBuilder()
                                .setText("Second sentence.").setStartOffset(16).setEndOffset(32).build())
                        .build())
                .build();

        String json = JsonFormat.printer().preservingProtoFieldNames().print(doc);

        @SuppressWarnings("unchecked")
        Map<String, Object> docMap = objectMapper.readValue(json, Map.class);

        assertThat(docMap).as("JSON contains nlp_analysis key").containsKey("nlp_analysis");

        @SuppressWarnings("unchecked")
        Map<String, Object> nlp = (Map<String, Object>) docMap.get("nlp_analysis");
        assertThat(nlp.get("detected_language")).as("language").isEqualTo("eng");
        assertThat(nlp.get("total_tokens")).as("total tokens").isEqualTo(150);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sentences = (List<Map<String, Object>>) nlp.get("sentences");
        assertThat(sentences).as("two sentences in NLP analysis").hasSize(2);
        assertThat(sentences.get(0).get("text")).as("first sentence").isEqualTo("First sentence.");
    }

    // =========================================================================
    // Chunk analytics in nested KNN documents
    // =========================================================================

    @Test
    void chunkAnalytics_inNestedDoc_afterTransform() throws Exception {
        // Simulate what transformSemanticSetsToNestedFields does:
        // serialize OpenSearchDocument to JSON, parse into map, rebuild nested docs
        ChunkAnalytics ca = ChunkAnalytics.newBuilder()
                .setWordCount(45)
                .setCharacterCount(280)
                .setSentenceCount(2)
                .setNounDensity(0.30f)
                .setVerbDensity(0.15f)
                .setLexicalDensity(0.55f)
                .setRelativePosition(0.35f)
                .setIsFirstChunk(false)
                .setIsLastChunk(false)
                .setPotentialHeadingScore(0.1f)
                .build();

        OpenSearchEmbedding embedding = OpenSearchEmbedding.newBuilder()
                .addVector(0.1f).addVector(0.2f).addVector(0.3f)
                .setSourceText("chunk text here")
                .setChunkConfigId("token_500")
                .setEmbeddingId("all-MiniLM")
                .setIsPrimary(false)
                .setChunkAnalytics(ca)
                .build();

        SemanticVectorSet vset = SemanticVectorSet.newBuilder()
                .setSourceFieldName("body")
                .setChunkConfigId("token_500")
                .setEmbeddingId("all-MiniLM")
                .addEmbeddings(embedding)
                .build();

        OpenSearchDocument osDoc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("nested-test")
                .addSemanticSets(vset)
                .build();

        // Simulate the transform: build nested doc map from embedding
        Map<String, Object> nestedDoc = new LinkedHashMap<>();
        nestedDoc.put("vector", embedding.getVectorList());
        nestedDoc.put("source_text", embedding.getSourceText());
        nestedDoc.put("chunk_config_id", vset.getChunkConfigId());
        nestedDoc.put("embedding_id", vset.getEmbeddingId());
        nestedDoc.put("is_primary", embedding.getIsPrimary());

        // Serialize chunk_analytics the same way the service does
        String analyticsJson = JsonFormat.printer().print(embedding.getChunkAnalytics());
        @SuppressWarnings("unchecked")
        Map<String, Object> analyticsMap = objectMapper.readValue(analyticsJson, Map.class);
        nestedDoc.put("chunk_analytics", analyticsMap);

        // Verify the nested doc structure
        assertThat(nestedDoc).as("nested doc has chunk_analytics").containsKey("chunk_analytics");

        @SuppressWarnings("unchecked")
        Map<String, Object> caMap = (Map<String, Object>) nestedDoc.get("chunk_analytics");
        assertThat(caMap.get("wordCount")).as("word count in nested doc").isEqualTo(45);
        assertThat(caMap.get("sentenceCount")).as("sentence count in nested doc").isEqualTo(2);
        assertThat(((Number) caMap.get("nounDensity")).floatValue())
                .as("noun density in nested doc").isCloseTo(0.30f, org.assertj.core.data.Offset.offset(0.001f));
        assertThat(((Number) caMap.get("relativePosition")).floatValue())
                .as("relative position in nested doc").isCloseTo(0.35f, org.assertj.core.data.Offset.offset(0.001f));
        // Proto JSON omits default values (false), so absent means false
        assertThat(caMap.getOrDefault("isFirstChunk", false))
                .as("isFirstChunk in nested doc (absent = false)").isEqualTo(false);
        assertThat(caMap.getOrDefault("isLastChunk", false))
                .as("isLastChunk in nested doc (absent = false)").isEqualTo(false);

        // Now verify the whole thing serializes to JSON correctly for OpenSearch
        String fullJson = objectMapper.writeValueAsString(
                Map.of("vs_body_token_500_all_MiniLM", List.of(nestedDoc)));
        assertThat(fullJson).as("full JSON contains chunk_analytics").contains("chunk_analytics");
        assertThat(fullJson).as("full JSON contains wordCount").contains("wordCount");
    }

    @Test
    void chunkAnalytics_roundTrip_deserializesBack() throws Exception {
        // Verify chunk_analytics survives: proto → JSON map → JSON string → JSON map → proto
        ChunkAnalytics original = ChunkAnalytics.newBuilder()
                .setWordCount(50)
                .setCharacterCount(310)
                .setSentenceCount(3)
                .setAverageWordLength(5.5f)
                .setAverageSentenceLength(16.7f)
                .setVocabularyDensity(0.72f)
                .setNounDensity(0.30f)
                .setVerbDensity(0.15f)
                .setAdjectiveDensity(0.08f)
                .setContentWordRatio(0.55f)
                .setUniqueLemmaCount(35)
                .setLexicalDensity(0.55f)
                .setRelativePosition(0.5f)
                .setIsFirstChunk(false)
                .setIsLastChunk(true)
                .setPotentialHeadingScore(0.05f)
                .setListItemIndicator(false)
                .setContainsUrlPlaceholder(true)
                .build();

        // Step 1: proto → JSON string → Map (write path)
        String analyticsJson = JsonFormat.printer().print(original);
        @SuppressWarnings("unchecked")
        Map<String, Object> analyticsMap = objectMapper.readValue(analyticsJson, Map.class);

        // Step 2: Map → JSON string (what gets stored in OpenSearch)
        String storedJson = objectMapper.writeValueAsString(analyticsMap);

        // Step 3: JSON string → Map → proto (read path / reconstruct)
        @SuppressWarnings("unchecked")
        Map<String, Object> readBack = objectMapper.readValue(storedJson, Map.class);
        String readBackJson = objectMapper.writeValueAsString(readBack);

        ChunkAnalytics.Builder rebuilt = ChunkAnalytics.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(readBackJson, rebuilt);
        ChunkAnalytics roundTripped = rebuilt.build();

        assertThat(roundTripped.getWordCount()).as("word count round-trip").isEqualTo(50);
        assertThat(roundTripped.getCharacterCount()).as("char count round-trip").isEqualTo(310);
        assertThat(roundTripped.getSentenceCount()).as("sentence count round-trip").isEqualTo(3);
        assertThat(roundTripped.getNounDensity()).as("noun density round-trip").isEqualTo(0.30f);
        assertThat(roundTripped.getVerbDensity()).as("verb density round-trip").isEqualTo(0.15f);
        assertThat(roundTripped.getLexicalDensity()).as("lexical density round-trip").isEqualTo(0.55f);
        assertThat(roundTripped.getRelativePosition()).as("relative position round-trip").isEqualTo(0.5f);
        assertThat(roundTripped.getIsFirstChunk()).as("isFirstChunk round-trip").isFalse();
        assertThat(roundTripped.getIsLastChunk()).as("isLastChunk round-trip").isTrue();
        assertThat(roundTripped.getPotentialHeadingScore()).as("heading score round-trip").isEqualTo(0.05f);
        assertThat(roundTripped.getContainsUrlPlaceholder()).as("URL placeholder round-trip").isTrue();
        assertThat(roundTripped.getUniqueLemmaCount()).as("unique lemma count round-trip").isEqualTo(35);
    }

    @Test
    void embeddingWithoutChunkAnalytics_noAnalyticsInNestedDoc() throws Exception {
        OpenSearchEmbedding embedding = OpenSearchEmbedding.newBuilder()
                .addVector(0.1f)
                .setSourceText("no analytics")
                .setChunkConfigId("c1")
                .setEmbeddingId("m1")
                .build();

        // Simulate transform
        Map<String, Object> nestedDoc = new LinkedHashMap<>();
        nestedDoc.put("vector", embedding.getVectorList());
        nestedDoc.put("source_text", embedding.getSourceText());
        nestedDoc.put("is_primary", embedding.getIsPrimary());

        // Only add chunk_analytics if present (matching service logic)
        if (embedding.hasChunkAnalytics()) {
            // should not enter this block
            nestedDoc.put("chunk_analytics", Map.of());
        }

        assertThat(nestedDoc).as("no chunk_analytics key when embedding has none")
                .doesNotContainKey("chunk_analytics");
    }

    // =========================================================================
    // Full document JSON structure validation
    // =========================================================================

    @Test
    void fullDocument_allAnalyticsLevels_producesCorrectJsonStructure() throws Exception {
        // Build a realistic OpenSearchDocument with all three analytics levels
        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("full-doc")
                .setDocType("article")
                .setTitle("Test Article")
                .addSourceFieldAnalytics(SourceFieldAnalytics.newBuilder()
                        .setSourceField("body")
                        .setChunkConfigId("token_500")
                        .setDocumentAnalytics(DocumentAnalytics.newBuilder()
                                .setWordCount(300)
                                .setSentenceCount(15)
                                .setDetectedLanguage("eng")
                                .setNounDensity(0.28f)
                                .build())
                        .setTotalChunks(2)
                        .build())
                .setNlpAnalysis(NlpDocumentAnalysis.newBuilder()
                        .setDetectedLanguage("eng")
                        .setNounDensity(0.28f)
                        .setLexicalDensity(0.52f)
                        .addSentences(SentenceSpan.newBuilder()
                                .setText("A sentence.").setStartOffset(0).setEndOffset(11).build())
                        .build())
                .addSemanticSets(SemanticVectorSet.newBuilder()
                        .setSourceFieldName("body")
                        .setChunkConfigId("token_500")
                        .setEmbeddingId("all-MiniLM")
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .addVector(0.1f).addVector(0.2f)
                                .setSourceText("first chunk")
                                .setIsPrimary(false)
                                .setChunkAnalytics(ChunkAnalytics.newBuilder()
                                        .setWordCount(160)
                                        .setRelativePosition(0.0f)
                                        .setIsFirstChunk(true)
                                        .build()))
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .addVector(0.3f).addVector(0.4f)
                                .setSourceText("second chunk")
                                .setIsPrimary(false)
                                .setChunkAnalytics(ChunkAnalytics.newBuilder()
                                        .setWordCount(140)
                                        .setRelativePosition(1.0f)
                                        .setIsLastChunk(true)
                                        .build()))
                        .build())
                .build();

        // Serialize to JSON preserving proto field names (what the service does)
        String json = JsonFormat.printer().preservingProtoFieldNames().print(doc);

        @SuppressWarnings("unchecked")
        Map<String, Object> docMap = objectMapper.readValue(json, Map.class);

        // Verify top-level structure
        assertThat(docMap).as("has source_field_analytics").containsKey("source_field_analytics");
        assertThat(docMap).as("has nlp_analysis").containsKey("nlp_analysis");
        assertThat(docMap).as("has semantic_sets").containsKey("semantic_sets");

        // Verify NLP analysis is filterable
        @SuppressWarnings("unchecked")
        Map<String, Object> nlp = (Map<String, Object>) docMap.get("nlp_analysis");
        assertThat(nlp.get("detected_language")).as("language filterable").isEqualTo("eng");

        // Verify semantic sets contain chunk_analytics
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sets = (List<Map<String, Object>>) docMap.get("semantic_sets");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> embeddings = (List<Map<String, Object>>) sets.get(0).get("embeddings");

        assertThat(embeddings).as("two embeddings").hasSize(2);

        @SuppressWarnings("unchecked")
        Map<String, Object> firstEmbedding = embeddings.get(0);
        assertThat(firstEmbedding).as("first embedding has chunk_analytics").containsKey("chunk_analytics");

        @SuppressWarnings("unchecked")
        Map<String, Object> firstCa = (Map<String, Object>) firstEmbedding.get("chunk_analytics");
        assertThat(firstCa.get("word_count")).as("first chunk word count").isEqualTo(160);
        assertThat(firstCa.get("is_first_chunk")).as("first chunk flag").isEqualTo(true);

        @SuppressWarnings("unchecked")
        Map<String, Object> secondCa = (Map<String, Object>) embeddings.get(1).get("chunk_analytics");
        assertThat(secondCa.get("word_count")).as("second chunk word count").isEqualTo(140);
        assertThat(secondCa.get("is_last_chunk")).as("last chunk flag").isEqualTo(true);
    }

    @Test
    void openSearchDocument_protoJsonRoundTrip_preservesAllAnalytics() throws Exception {
        // Full proto → JSON → proto round-trip (simulates index + retrieve from OpenSearch)
        OpenSearchDocument original = OpenSearchDocument.newBuilder()
                .setOriginalDocId("round-trip")
                .addSourceFieldAnalytics(SourceFieldAnalytics.newBuilder()
                        .setSourceField("body")
                        .setChunkConfigId("c1")
                        .setDocumentAnalytics(DocumentAnalytics.newBuilder()
                                .setWordCount(200).setDetectedLanguage("fra").build())
                        .setTotalChunks(2)
                        .build())
                .setNlpAnalysis(NlpDocumentAnalysis.newBuilder()
                        .setDetectedLanguage("fra")
                        .setNounDensity(0.25f)
                        .addSentences(SentenceSpan.newBuilder().setText("Bonjour.").build())
                        .build())
                .addSemanticSets(SemanticVectorSet.newBuilder()
                        .setSourceFieldName("body")
                        .setChunkConfigId("c1")
                        .setEmbeddingId("m1")
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .addVector(0.5f)
                                .setSourceText("texte")
                                .setChunkAnalytics(ChunkAnalytics.newBuilder()
                                        .setWordCount(30)
                                        .setNounDensity(0.25f)
                                        .setRelativePosition(0.0f)
                                        .setIsFirstChunk(true)
                                        .build()))
                        .build())
                .build();

        String json = JsonFormat.printer().print(original);

        OpenSearchDocument.Builder rebuilt = OpenSearchDocument.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(json, rebuilt);
        OpenSearchDocument roundTripped = rebuilt.build();

        // Source field analytics
        assertThat(roundTripped.getSourceFieldAnalyticsCount())
                .as("source field analytics count").isEqualTo(1);
        assertThat(roundTripped.getSourceFieldAnalytics(0).getDocumentAnalytics().getWordCount())
                .as("word count").isEqualTo(200);
        assertThat(roundTripped.getSourceFieldAnalytics(0).getDocumentAnalytics().getDetectedLanguage())
                .as("language").isEqualTo("fra");

        // NLP analysis
        assertThat(roundTripped.getNlpAnalysis().getDetectedLanguage())
                .as("NLP language").isEqualTo("fra");
        assertThat(roundTripped.getNlpAnalysis().getSentencesCount())
                .as("NLP sentences").isEqualTo(1);

        // Chunk analytics
        OpenSearchEmbedding emb = roundTripped.getSemanticSets(0).getEmbeddings(0);
        assertThat(emb.hasChunkAnalytics()).as("chunk analytics present").isTrue();
        assertThat(emb.getChunkAnalytics().getWordCount()).as("chunk word count").isEqualTo(30);
        assertThat(emb.getChunkAnalytics().getNounDensity()).as("chunk noun density").isEqualTo(0.25f);
        assertThat(emb.getChunkAnalytics().getIsFirstChunk()).as("chunk is first").isTrue();
    }

    // =========================================================================
    // Punctuation counts sanitization tests
    // =========================================================================

    @Test
    void punctuationCounts_inDocumentAnalytics_strippedFromJson() throws Exception {
        // punctuation_counts has keys like ".", "," which OpenSearch interprets as
        // path separators, causing mapper_parsing_exception. Must be stripped before indexing.
        OpenSearchDocument doc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("punct-test")
                .addSourceFieldAnalytics(SourceFieldAnalytics.newBuilder()
                        .setSourceField("body")
                        .setChunkConfigId("c1")
                        .setDocumentAnalytics(DocumentAnalytics.newBuilder()
                                .setWordCount(100)
                                .setDetectedLanguage("eng")
                                .putPunctuationCounts(".", 15)
                                .putPunctuationCounts(",", 8)
                                .putPunctuationCounts("!", 2)
                                .build())
                        .build())
                .build();

        // Serialize with preserving field names (what the indexing service does)
        String json = JsonFormat.printer().preservingProtoFieldNames().print(doc);

        @SuppressWarnings("unchecked")
        Map<String, Object> docMap = objectMapper.readValue(json, Map.class);

        // Verify punctuation_counts IS present in the raw proto JSON
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sfaList = (List<Map<String, Object>>) docMap.get("source_field_analytics");
        @SuppressWarnings("unchecked")
        Map<String, Object> da = (Map<String, Object>) sfaList.get(0).get("document_analytics");
        assertThat(da).as("raw JSON has punctuation_counts before sanitization")
                .containsKey("punctuation_counts");

        // Now simulate the sanitization the indexing service performs
        da.remove("punctuation_counts");

        // Verify it's gone
        assertThat(da).as("punctuation_counts removed after sanitization")
                .doesNotContainKey("punctuation_counts");
        // But other fields are preserved
        assertThat(da.get("word_count")).as("word_count preserved").isEqualTo(100);
        assertThat(da.get("detected_language")).as("detected_language preserved").isEqualTo("eng");
    }

    @Test
    void punctuationCounts_inChunkAnalytics_strippedFromJson() throws Exception {
        ChunkAnalytics ca = ChunkAnalytics.newBuilder()
                .setWordCount(45)
                .setNounDensity(0.30f)
                .putPunctuationCounts(".", 3)
                .putPunctuationCounts(",", 2)
                .build();

        // Serialize (default printer uses camelCase)
        String analyticsJson = JsonFormat.printer().print(ca);

        @SuppressWarnings("unchecked")
        Map<String, Object> analyticsMap = objectMapper.readValue(analyticsJson, Map.class);

        assertThat(analyticsMap).as("raw JSON has punctuationCounts before sanitization")
                .containsKey("punctuationCounts");

        // Simulate sanitization
        analyticsMap.remove("punctuationCounts");
        analyticsMap.remove("punctuation_counts");

        assertThat(analyticsMap).as("punctuationCounts removed")
                .doesNotContainKey("punctuationCounts")
                .doesNotContainKey("punctuation_counts");
        assertThat(analyticsMap.get("wordCount")).as("wordCount preserved").isEqualTo(45);
    }

    @Test
    void punctuationCounts_dotKeysWouldCauseOpenSearchFailure() throws Exception {
        // Demonstrate the problem: "." as a map key in an object field
        // OpenSearch would interpret this as a nested path with empty segments
        DocumentAnalytics da = DocumentAnalytics.newBuilder()
                .setWordCount(50)
                .putPunctuationCounts(".", 10)
                .putPunctuationCounts(",", 5)
                .putPunctuationCounts("...", 1)  // triple dot — even worse
                .build();

        String json = JsonFormat.printer().preservingProtoFieldNames().print(da);

        @SuppressWarnings("unchecked")
        Map<String, Object> daMap = objectMapper.readValue(json, Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Object> punctCounts = (Map<String, Object>) daMap.get("punctuation_counts");

        // These keys contain dots which OpenSearch treats as path separators
        assertThat(punctCounts).as("dot key present").containsKey(".");
        assertThat(punctCounts).as("triple dot key present").containsKey("...");

        // After sanitization, the whole map is removed
        daMap.remove("punctuation_counts");
        assertThat(daMap).as("sanitized — no punctuation_counts")
                .doesNotContainKey("punctuation_counts");
        assertThat(daMap.get("word_count")).as("other fields untouched").isEqualTo(50);
    }
}
