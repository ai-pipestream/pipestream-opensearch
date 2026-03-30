package ai.pipestream.schemamanager.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "vectorset.semantic")
public interface SemanticVectorSetConfig {

    @WithName("inline-resolution.enabled")
    @WithDefault("true")
    boolean inlineResolutionEnabled();

    @WithName("source-cel.max-chars")
    @WithDefault("1048576")
    int sourceCelMaxChars();
}
