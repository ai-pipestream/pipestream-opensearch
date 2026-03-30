package ai.pipestream.schemamanager.util;

import ai.pipestream.common.descriptor.ClasspathDescriptorLoader;
import ai.pipestream.common.descriptor.DescriptorRegistry;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import com.google.protobuf.Any;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class AnyDocumentMapperTest {

    private AnyDocumentMapper mapper;

    @BeforeEach
    void setUp() {
        // Manually assemble the mapper with a real registry and classpath loader
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.addLoader(new ClasspathDescriptorLoader());
        
        mapper = new AnyDocumentMapper();
        mapper.descriptorRegistry = registry;
    }

    @Test
    void testMapSearchMetadata() {
        SearchMetadata meta = SearchMetadata.newBuilder()
                .setTitle("Descriptor Title")
                .setBody("Descriptor Body")
                .setAuthor("Descriptor Author")
                .setDocumentType("PDF")
                .build();

        // Any.pack uses the type name which ClasspathDescriptorLoader will use 
        // to find the class and get the descriptor.
        Any any = Any.pack(meta);
        OpenSearchDocument doc = mapper.mapToOpenSearchDocument(any, "id-1");

        assertThat(doc.getOriginalDocId(), equalTo("id-1"));
        assertThat(doc.getDocType(), containsString("SearchMetadata"));
        assertThat(doc.getTitle(), equalTo("Descriptor Title"));
        assertThat(doc.getBody(), equalTo("Descriptor Body"));
        assertThat(doc.getCreatedBy(), equalTo("Descriptor Author"));
        
        // Verify custom_fields Struct has the data too
        assertThat(doc.getCustomFields().getFieldsMap(), hasKey("title"));
        assertThat(doc.getCustomFields().getFieldsMap().get("title").getStringValue(), equalTo("Descriptor Title"));
    }
}
