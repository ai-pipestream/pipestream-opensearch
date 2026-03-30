package ai.pipestream.schemamanager.opensearch;

/**
 * Constants for OpenSearch indices and field names
 */
public class IndexConstants {
    
    /**
     * Index names for different entity types
     */
    public enum Index {
        // Filesystem indices
        FILESYSTEM_DRIVES("filesystem-drives"),
        FILESYSTEM_NODES("filesystem-nodes"),
        
        // Repository service indices
        REPOSITORY_MODULES("repository-modules"),
        REPOSITORY_PIPEDOCS("repository-pipedocs"),
        REPOSITORY_PROCESS_REQUESTS("repository-process-requests"),
        REPOSITORY_PROCESS_RESPONSES("repository-process-responses"),
        REPOSITORY_GRAPHS("repository-graphs"),
        REPOSITORY_GRAPH_NODES("repository-graph-nodes"),
        REPOSITORY_GRAPH_EDGES("repository-graph-edges"),
        REPOSITORY_DOCUMENT_UPLOADS("repository-document-uploads"),

        // Catalog and history indices
        REPOSITORY_CATALOG("repository-catalog"),
        REPOSITORY_HISTORY("repository-history");
        
        private final String indexName;
        
        Index(String indexName) {
            this.indexName = indexName;
        }
        
        public String getIndexName() {
            return indexName;
        }
    }
    
    /**
     * Common field names across all indices
     */
    public enum CommonFields {
        ID("id"),
        INDEXED_AT("indexed_at"),
        CREATED_AT("created_at"),
        UPDATED_AT("updated_at"),
        MODIFIED_AT("modified_at"),
        NAME("name"),
        DESCRIPTION("description"),
        METADATA("metadata"),
        TAGS("tags");
        
        private final String fieldName;
        
        CommonFields(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Drive-specific field names
     */
    public enum DriveFields {
        DRIVE_NAME("drive_name"),
        DRIVE_NAME_TEXT("drive_name_text"),
        ENDPOINT("endpoint"),
        BUCKET("bucket"),
        ACCESS_KEY("access_key"),
        REGION("region"),
        TYPE("type"),
        STATUS("status"),
        TOTAL_OBJECTS("total_objects"),
        TOTAL_SIZE("total_size"),
        S3_METADATA("s3_metadata"),
        IS_PUBLIC("is_public"),
        ENCRYPTION_STATUS("encryption_status"),
        VERSIONING_ENABLED("versioning_enabled");
        
        private final String fieldName;
        
        DriveFields(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Node-specific field names
     */
    public enum NodeFields {
        NODE_ID("node_id"),
        DRIVE("drive"),
        PARENT_ID("parent_id"),
        NODE_TYPE("node_type"),
        PATH("path"),
        PATH_COMPONENTS("path_components"),
        PATH_DEPTH("path_depth"),
        PARENT_PATH("parent_path"),
        RELATIVE_PATH("relative_path"),
        
        // Size fields
        FILE_SIZE("file_size"),
        PAYLOAD_SIZE("payload_size"),
        METADATA_SIZE("metadata_size"),
        TOTAL_SIZE("total_size"),
        
        // S3/Storage fields
        BUCKET_NAME("bucket_name"),
        S3_KEY("s3_key"),
        S3_ENDPOINT("s3_endpoint"),
        S3_METADATA("s3_metadata"),
        ETAG("etag"),
        VERSION_ID("version_id"),
        STORAGE_CLASS("storage_class"),
        CONTENT_TYPE("content_type"),
        CHECKSUM("checksum"),
        
        // File metadata
        MIME_TYPE("mime_type"),
        MIME_TYPE_TEXT("mime_type_text"),
        FILE_EXTENSION("file_extension"),
        MIME_TYPE_CATEGORY("mime_type_category"),
        
        // Payload metadata
        HAS_PAYLOAD("has_payload"),
        PAYLOAD_TYPE_URL("payload_type_url"),
        PAYLOAD_CLASS_NAME("payload_class_name"),
        
        // UI/Display fields
        ICON_SVG("icon_svg"),
        SERVICE_TYPE("service_type"),
        
        // Search duplicates for text analysis
        NAME_TEXT("name_text"),
        PATH_TEXT("path_text");
        
        private final String fieldName;
        
        NodeFields(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * PipeDoc-specific field names
     */
    public enum PipeDocFields {
        STORAGE_ID("storage_id"),
        DOC_ID("doc_id"),
        TITLE("title"),
        AUTHOR("author"),
        CONTENT_SUMMARY("content_summary"),
        ENTITY_COUNT("entity_count"),
        DOCUMENT_TYPE("document_type");
        
        private final String fieldName;
        
        PipeDocFields(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Process Request/Response field names
     */
    public enum ProcessFields {
        STORAGE_ID("storage_id"),
        REQUEST_ID("request_id"),
        RESPONSE_ID("response_id"),
        MODULE_ID("module_id"),
        PROCESSOR_ID("processor_id"),
        STATUS("status"),
        STREAM_ID("stream_id");
        
        private final String fieldName;
        
        ProcessFields(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Graph-specific field names
     */
    public enum GraphFields {
        GRAPH_ID("graph_id"),
        CLUSTER_ID("cluster_id"),
        NODE_ID("node_id"),
        EDGE_ID("edge_id"),
        FROM_NODE_ID("from_node_id"),
        TO_NODE_ID("to_node_id"),
        TO_CLUSTER_ID("to_cluster_id"),
        NODE_TYPE("node_type"),
        MODULE_ID("module_id"),
        CONDITION("condition"),
        PRIORITY("priority"),
        IS_CROSS_CLUSTER("is_cross_cluster"),
        MODE("mode"),
        VISIBILITY("visibility");
        
        private final String fieldName;
        
        GraphFields(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Module-specific field names
     */
    public enum ModuleFields {
        MODULE_ID("module_id"),
        IMPLEMENTATION_NAME("implementation_name"),
        GRPC_SERVICE_NAME("grpc_service_name"),
        VISIBILITY("visibility"),
        CONFIG_SCHEMA("config_schema"),
        DEFAULT_CONFIG("default_config");
        
        private final String fieldName;
        
        ModuleFields(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public String getFieldName() {
            return fieldName;
        }
    }
}