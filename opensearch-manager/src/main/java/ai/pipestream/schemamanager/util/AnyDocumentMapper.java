package ai.pipestream.schemamanager.util;

import ai.pipestream.common.descriptor.DescriptorRegistry;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Strictly maps Protobuf 'Any' messages to the canonical OpenSearchDocument schema.
 * Relies entirely on the DescriptorRegistry (Apicurio + Classpath).
 * If a descriptor is missing, it fails fast to prevent "JSON bleed" or data loss.
 */
@ApplicationScoped
public class AnyDocumentMapper {

    private static final Logger LOG = Logger.getLogger(AnyDocumentMapper.class);

    @Inject
    DescriptorRegistry descriptorRegistry;

    /** CDI; {@link #descriptorRegistry} is injected after construction. */
    public AnyDocumentMapper() {
    }

    /**
     * Parses a protobuf {@link Any} into {@link OpenSearchDocument} using registered descriptors.
     *
     * @param anyDocument packed user document
     * @param documentId  explicit id, or blank to assign a random UUID
     * @return normalized OpenSearch document
     */
    public OpenSearchDocument mapToOpenSearchDocument(Any anyDocument, String documentId) {
        String id = (documentId != null && !documentId.isBlank()) ? documentId : UUID.randomUUID().toString();
        
        // 1. Resolve the Descriptor strictly
        String typeUrl = anyDocument.getTypeUrl();
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);

        Descriptor descriptor = descriptorRegistry.findDescriptorByFullName(typeName);
        if (descriptor == null) {
            throw new RuntimeException("Protocol Buffer Descriptor not found for type: " + typeName + 
                ". Ensure the schema is registered in Apicurio.");
        }

        try {
            // 2. Parse into a DynamicMessage
            DynamicMessage message = DynamicMessage.parseFrom(descriptor, anyDocument.getValue());
            
            OpenSearchDocument.Builder builder = OpenSearchDocument.newBuilder()
                    .setOriginalDocId(id)
                    .setDocType(descriptor.getFullName());

            // 1. Heuristic Mapping of standard fields from the DynamicMessage
            for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
                FieldDescriptor field = entry.getKey();
                Object value = entry.getValue();
                String fieldName = field.getName().toLowerCase();

                if (value instanceof String strValue) {
                    if (fieldName.contains("title") || fieldName.equals("headline")) {
                        builder.setTitle(strValue);
                    } else if (fieldName.equals("body") || fieldName.equals("content") || fieldName.equals("text")) {
                        builder.setBody(strValue);
                    } else if (fieldName.equals("author") || fieldName.equals("creator")) {
                        builder.setCreatedBy(strValue);
                    }
                } else if (value instanceof DynamicMessage msgValue) {
                    // Check for Timestamp promotion
                    if (msgValue.getDescriptorForType().getFullName().equals("google.protobuf.Timestamp")) {
                        Timestamp ts = Timestamp.newBuilder()
                                .setSeconds((Long) msgValue.getField(msgValue.getDescriptorForType().findFieldByName("seconds")))
                                .setNanos((Integer) msgValue.getField(msgValue.getDescriptorForType().findFieldByName("nanos")))
                                .build();

                        if (fieldName.contains("created")) {
                            builder.setCreatedAt(ts);
                        } else if (fieldName.contains("modified") || fieldName.contains("updated")) {
                            builder.setLastModifiedAt(ts);
                        }
                    }
                }
            }


            // 4. Map the entire structure to custom_fields Struct for indexing
            builder.setCustomFields(toStruct(message));

            return builder.build();
        } catch (InvalidProtocolBufferException e) {
            LOG.errorf(e, "Failed to parse Any document of type %s", typeName);
            throw new RuntimeException("Failed to parse strictly typed Any document: " + typeName, e);
        }
    }

    /**
     * Recursively converts a DynamicMessage to a Google Protobuf Struct.
     */
    private Struct toStruct(DynamicMessage message) {
        Struct.Builder builder = Struct.newBuilder();
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            builder.putFields(entry.getKey().getName(), toValue(entry.getValue()));
        }
        return builder.build();
    }

    private Value toValue(Object value) {
        Value.Builder builder = Value.newBuilder();
        if (value instanceof String s) {
            builder.setStringValue(s);
        } else if (value instanceof Number n) {
            builder.setNumberValue(n.doubleValue());
        } else if (value instanceof Boolean b) {
            builder.setBoolValue(b);
        } else if (value instanceof DynamicMessage m) {
            builder.setStructValue(toStruct(m));
        } else if (value instanceof List<?> list) {
            ListValue.Builder listBuilder = ListValue.newBuilder();
            for (Object item : list) {
                listBuilder.addValues(toValue(item));
            }
            builder.setListValue(listBuilder.build());
        } else if (value instanceof Descriptors.EnumValueDescriptor ev) {
            builder.setStringValue(ev.getName());
        } else if (value == null) {
            builder.setNullValue(NullValue.NULL_VALUE);
        } else {
            // Fallback for types that are not explicitly handled but exist in the descriptor
            builder.setStringValue(value.toString());
        }
        return builder.build();
    }
}
