package ai.pipestream.schemamanager.opensearch;

/**
 * Constants for OpenSearch index names and canonical field name strings used in mappings and queries.
 */
public final class IndexConstants {

    private IndexConstants() {
    }

    /**
     * Logical OpenSearch index names for platform entity types.
     */
    public enum Index {
        /** Index for filesystem drive metadata. */
        FILESYSTEM_DRIVES("filesystem-drives"),
        /** Index for filesystem node metadata. */
        FILESYSTEM_NODES("filesystem-nodes"),

        /** Index for repository module definitions. */
        REPOSITORY_MODULES("repository-modules"),
        /** Index for PipeDoc summaries. */
        REPOSITORY_PIPEDOCS("repository-pipedocs"),
        /** Index for pipeline process requests. */
        REPOSITORY_PROCESS_REQUESTS("repository-process-requests"),
        /** Index for pipeline process responses. */
        REPOSITORY_PROCESS_RESPONSES("repository-process-responses"),
        /** Index for repository graphs. */
        REPOSITORY_GRAPHS("repository-graphs"),
        /** Index for graph node rows. */
        REPOSITORY_GRAPH_NODES("repository-graph-nodes"),
        /** Index for graph edge rows. */
        REPOSITORY_GRAPH_EDGES("repository-graph-edges"),
        /** Index for document upload tracking. */
        REPOSITORY_DOCUMENT_UPLOADS("repository-document-uploads"),

        /** Index for catalog entries. */
        REPOSITORY_CATALOG("repository-catalog"),
        /** Index for repository history events. */
        REPOSITORY_HISTORY("repository-history");

        private final String indexName;

        /**
         * @param indexName physical OpenSearch index name
         */
        Index(String indexName) {
            this.indexName = indexName;
        }

        /**
         * Returns the physical OpenSearch index name for this logical index.
         *
         * @return index name string
         */
        public String getIndexName() {
            return indexName;
        }
    }

    /**
     * Field names shared across many Pipestream indices.
     */
    public enum CommonFields {
        /** Document or entity id. */
        ID("id"),
        /** When the document was indexed into OpenSearch. */
        INDEXED_AT("indexed_at"),
        /** Creation timestamp from the source system. */
        CREATED_AT("created_at"),
        /** Last update timestamp from the source system. */
        UPDATED_AT("updated_at"),
        /** Last modification timestamp. */
        MODIFIED_AT("modified_at"),
        /** Display name. */
        NAME("name"),
        /** Human-readable description. */
        DESCRIPTION("description"),
        /** Arbitrary JSON metadata blob. */
        METADATA("metadata"),
        /** Tag list or tag string. */
        TAGS("tags");

        private final String fieldName;

        /**
         * @param fieldName OpenSearch field name
         */
        CommonFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the OpenSearch field name for this constant.
         *
         * @return field name
         */
        public String getFieldName() {
            return fieldName;
        }
    }

    /**
     * Field names for filesystem drive documents.
     */
    public enum DriveFields {
        /** Drive display name. */
        DRIVE_NAME("drive_name"),
        /** Analyzed text form of drive name. */
        DRIVE_NAME_TEXT("drive_name_text"),
        /** Storage endpoint URL or host. */
        ENDPOINT("endpoint"),
        /** Bucket or container name. */
        BUCKET("bucket"),
        /** Access key identifier (never the secret). */
        ACCESS_KEY("access_key"),
        /** Cloud region code. */
        REGION("region"),
        /** Drive or storage type discriminator. */
        TYPE("type"),
        /** Operational status string. */
        STATUS("status"),
        /** Count of stored objects. */
        TOTAL_OBJECTS("total_objects"),
        /** Total stored bytes. */
        TOTAL_SIZE("total_size"),
        /** S3-specific metadata map. */
        S3_METADATA("s3_metadata"),
        /** Whether the drive is publicly readable. */
        IS_PUBLIC("is_public"),
        /** Server-side encryption status. */
        ENCRYPTION_STATUS("encryption_status"),
        /** Whether object versioning is enabled. */
        VERSIONING_ENABLED("versioning_enabled");

        private final String fieldName;

        /**
         * @param fieldName OpenSearch field name
         */
        DriveFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the OpenSearch field name for this constant.
         *
         * @return field name
         */
        public String getFieldName() {
            return fieldName;
        }
    }

    /**
     * Field names for filesystem node (file/folder) documents.
     */
    public enum NodeFields {
        /** Stable node id. */
        NODE_ID("node_id"),
        /** Owning drive id or key. */
        DRIVE("drive"),
        /** Parent folder node id. */
        PARENT_ID("parent_id"),
        /** FILE, FOLDER, or similar type label. */
        NODE_TYPE("node_type"),
        /** Full logical path string. */
        PATH("path"),
        /** Path split into ordered path components. */
        PATH_COMPONENTS("path_components"),
        /** Directory depth (root = 0). */
        PATH_DEPTH("path_depth"),
        /** Parent directory path string. */
        PARENT_PATH("parent_path"),
        /** Path relative to some anchor (implementation-defined). */
        RELATIVE_PATH("relative_path"),

        /** Logical file size in bytes. */
        FILE_SIZE("file_size"),
        /** Stored payload size in bytes. */
        PAYLOAD_SIZE("payload_size"),
        /** User metadata size in bytes. */
        METADATA_SIZE("metadata_size"),
        /** Combined size fields for sorting. */
        TOTAL_SIZE("total_size"),

        /** Bucket backing this node when stored in object storage. */
        BUCKET_NAME("bucket_name"),
        /** Object key within the bucket. */
        S3_KEY("s3_key"),
        /** Resolved S3 endpoint for this object. */
        S3_ENDPOINT("s3_endpoint"),
        /** Raw S3 metadata map. */
        S3_METADATA("s3_metadata"),
        /** HTTP ETag for the stored object. */
        ETAG("etag"),
        /** S3 version id when versioning is enabled. */
        VERSION_ID("version_id"),
        /** S3 storage class (STANDARD, GLACIER, etc.). */
        STORAGE_CLASS("storage_class"),
        /** MIME type of the payload. */
        CONTENT_TYPE("content_type"),
        /** Content checksum string. */
        CHECKSUM("checksum"),

        /** MIME type of the file payload. */
        MIME_TYPE("mime_type"),
        /** Analyzed MIME type text. */
        MIME_TYPE_TEXT("mime_type_text"),
        /** File extension without dot. */
        FILE_EXTENSION("file_extension"),
        /** Coarse MIME category for faceting. */
        MIME_TYPE_CATEGORY("mime_type_category"),

        /** Whether a binary payload exists for this node. */
        HAS_PAYLOAD("has_payload"),
        /** URL identifying the payload type. */
        PAYLOAD_TYPE_URL("payload_type_url"),
        /** Java class or proto type name for the payload. */
        PAYLOAD_CLASS_NAME("payload_class_name"),

        /** Optional SVG icon identifier. */
        ICON_SVG("icon_svg"),
        /** Service that produced this node (ingest pipeline id, etc.). */
        SERVICE_TYPE("service_type"),

        /** Analyzed file or folder name for full-text search. */
        NAME_TEXT("name_text"),
        /** Analyzed path for full-text search. */
        PATH_TEXT("path_text");

        private final String fieldName;

        /**
         * @param fieldName OpenSearch field name
         */
        NodeFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the OpenSearch field name for this constant.
         *
         * @return field name
         */
        public String getFieldName() {
            return fieldName;
        }
    }

    /**
     * Field names for PipeDoc index documents.
     */
    public enum PipeDocFields {
        /** Storage id owning this PipeDoc. */
        STORAGE_ID("storage_id"),
        /** Canonical document id. */
        DOC_ID("doc_id"),
        /** Title text. */
        TITLE("title"),
        /** Author string. */
        AUTHOR("author"),
        /** Short summary of body content. */
        CONTENT_SUMMARY("content_summary"),
        /** Count of extracted entities. */
        ENTITY_COUNT("entity_count"),
        /** Document type discriminator. */
        DOCUMENT_TYPE("document_type");

        private final String fieldName;

        /**
         * @param fieldName OpenSearch field name
         */
        PipeDocFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the OpenSearch field name for this constant.
         *
         * @return field name
         */
        public String getFieldName() {
            return fieldName;
        }
    }

    /**
     * Field names for pipeline process request/response indices.
     */
    public enum ProcessFields {
        /** Storage id for the originating document. */
        STORAGE_ID("storage_id"),
        /** Unique process request id. */
        REQUEST_ID("request_id"),
        /** Unique process response id. */
        RESPONSE_ID("response_id"),
        /** Module id that handled the request. */
        MODULE_ID("module_id"),
        /** Processor id within the module. */
        PROCESSOR_ID("processor_id"),
        /** Processing status string. */
        STATUS("status"),
        /** Optional stream id for streaming processors. */
        STREAM_ID("stream_id");

        private final String fieldName;

        /**
         * @param fieldName OpenSearch field name
         */
        ProcessFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the OpenSearch field name for this constant.
         *
         * @return field name
         */
        public String getFieldName() {
            return fieldName;
        }
    }

    /**
     * Field names for repository graph indices.
     */
    public enum GraphFields {
        /** Graph id. */
        GRAPH_ID("graph_id"),
        /** Cluster owning this graph. */
        CLUSTER_ID("cluster_id"),
        /** Graph node id. */
        NODE_ID("node_id"),
        /** Graph edge id. */
        EDGE_ID("edge_id"),
        /** Edge tail node id. */
        FROM_NODE_ID("from_node_id"),
        /** Edge head node id. */
        TO_NODE_ID("to_node_id"),
        /** Remote cluster id for cross-cluster edges. */
        TO_CLUSTER_ID("to_cluster_id"),
        /** Node type label. */
        NODE_TYPE("node_type"),
        /** Referenced module id. */
        MODULE_ID("module_id"),
        /** Edge condition expression. */
        CONDITION("condition"),
        /** Edge evaluation priority. */
        PRIORITY("priority"),
        /** Whether the edge spans clusters. */
        IS_CROSS_CLUSTER("is_cross_cluster"),
        /** Graph or edge mode flag. */
        MODE("mode"),
        /** Visibility scope for the graph artifact. */
        VISIBILITY("visibility");

        private final String fieldName;

        /**
         * @param fieldName OpenSearch field name
         */
        GraphFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the OpenSearch field name for this constant.
         *
         * @return field name
         */
        public String getFieldName() {
            return fieldName;
        }
    }

    /**
     * Field names for pipeline module catalog documents.
     */
    public enum ModuleFields {
        /** Module id. */
        MODULE_ID("module_id"),
        /** Java or native implementation class name. */
        IMPLEMENTATION_NAME("implementation_name"),
        /** gRPC fully-qualified service name. */
        GRPC_SERVICE_NAME("grpc_service_name"),
        /** Module visibility (public, private, etc.). */
        VISIBILITY("visibility"),
        /** JSON Schema or descriptor id for configuration. */
        CONFIG_SCHEMA("config_schema"),
        /** Default configuration JSON blob. */
        DEFAULT_CONFIG("default_config");

        private final String fieldName;

        /**
         * @param fieldName OpenSearch field name
         */
        ModuleFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the OpenSearch field name for this constant.
         *
         * @return field name
         */
        public String getFieldName() {
            return fieldName;
        }
    }
}
