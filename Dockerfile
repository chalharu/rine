# Runtime image for rine: a prebuilt, stripped release binary on
# distroless. The binary is compiled by CI (see release.yml) so image
# builds stay fast and bit-identical to the attached release tarballs;
# multi-arch works without emulation because this stage runs nothing.
FROM gcr.io/distroless/base-debian12:nonroot
ARG TARGETARCH
COPY rine-${TARGETARCH} /rine
EXPOSE 2379
ENTRYPOINT ["/rine"]
