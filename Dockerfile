# Runtime image for rine: a prebuilt, stripped release binary on
# distroless. The binary is compiled by CI (see release.yml) so image
# builds stay fast and bit-identical to the attached release tarballs;
# multi-arch works without emulation because this stage runs nothing.
FROM gcr.io/distroless/base-debian12:nonroot
ARG TARGETARCH
COPY rine-${TARGETARCH} /rine
EXPOSE 2379
# The base image already defaults to nonroot; the explicit numeric USER
# satisfies trivy's DS-0002 (names may not resolve, DL3066) without
# changing the runtime identity.
USER 65532:65532
# No shell or curl exists in distroless, so no in-image probe is possible.
# Health is owned by the orchestrator via the same-port /health endpoint
# (see the ci.yml smoke test); explicitly disable the Dockerfile-level
# check instead of shipping a fake one.
HEALTHCHECK NONE
ENTRYPOINT ["/rine"]
