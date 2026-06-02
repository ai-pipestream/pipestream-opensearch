package ai.pipestream.schemamanager.indexing;

import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceResponse;
import ai.pipestream.repository.pipedoc.v1.PipeDocServiceGrpc;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RepoClient {

    @Inject
    @GrpcClient("repository")
    PipeDocServiceGrpc.PipeDocServiceBlockingStub repositoryStub;

    public GetPipeDocByReferenceResponse getPipeDocByReference(GetPipeDocByReferenceRequest request) {
        return repositoryStub.getPipeDocByReference(request);
    }
}
