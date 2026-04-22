package ai.pipestream.schemamanager.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Feature flags for semantic vector set resolution and validation limits.
 */
@ConfigMapping(prefix = "vectorset.semantic")
public interface SemanticVectorSetConfig {

    /**
     * When true, allows resolving vector sets from inline document directives without a prior DB row.
     *
     * @return whether inline resolution is enabled
     */
    @WithName("inline-resolution.enabled")
    @WithDefault("true")
    boolean inlineResolutionEnabled();

    /**
     * Maximum allowed length (in characters) for {@code source_cel} strings.
     *
     * @return character limit
     */
    @WithName("source-cel.max-chars")
    @WithDefault("1048576")
    int sourceCelMaxChars();
}
