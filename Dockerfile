# Runtime image for rine: a prebuilt, stripped release binary on
# distroless. The binary is compiled by CI (see release.yml) so image
# builds stay fast and bit-identical to the attached release tarballs;
# multi-arch works without emulation because this stage runs nothing.
#
# Pinned by digest (hadolint DL3006; renovate bumps it): `:latest` here
# is only the tag the digest was taken from, the digest itself wins.
# Same glibc floor caveat as the build runners (see the release workflow):
# debian13 ships glibc 2.41, comfortably above the 22.04-built 2.35.
FROM gcr.io/distroless/cc-debian13:latest@sha256:dac90a25ffe03ebaace90cda886b081e61ab6cd77ac47eda51f7bcc15c320765
ARG TARGETARCH
COPY rine-${TARGETARCH} /rine
EXPOSE 2379
# Explicit numeric non-root user (trivy DS-0002; hadolint DL3066 wants
# numeric). Distroless `cc` has no `nonroot` variant, but a numeric UID
# needs no passwd entry to run.
USER 65532:65532
# No shell or curl exists in distroless, so no in-image probe is possible.
# Health is owned by the orchestrator via the same-port /health endpoint
# (see the ci.yml smoke test); explicitly disable the Dockerfile-level
# check instead of shipping a fake one.
HEALTHCHECK NONE
ENTRYPOINT ["/rine"]
