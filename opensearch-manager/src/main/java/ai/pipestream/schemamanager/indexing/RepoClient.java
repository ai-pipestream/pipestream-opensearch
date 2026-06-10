package ai.pipestream.schemamanager.indexing;

import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceResponse;
import ai.pipestream.repository.pipedoc.v1.PipeDocServiceGrpc;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class RepoClient {

    /**
     * Per-call deadline for claim-check dereferences. WITHOUT a deadline, a
     * wedged repo connection blocks the indexing consumer's batch forever —
     * and on a single partition that freezes the entire indexing pipeline
     * (observed live: the separate_indices drain froze with ~700 events left,
     * twice). 30s is generous for a single S3-backed read; past it, fail the
     * event terminally and keep the partition moving.
     */
    private static final long DEREF_DEADLINE_SECONDS = 30;

    @Inject
    @GrpcClient("repository")
    PipeDocServiceGrpc.PipeDocServiceBlockingStub repositoryStub;

    public GetPipeDocByReferenceResponse getPipeDocByReference(GetPipeDocByReferenceRequest request) {
        return repositoryStub
                .withDeadlineAfter(DEREF_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .getPipeDocByReference(request);
    }
}
