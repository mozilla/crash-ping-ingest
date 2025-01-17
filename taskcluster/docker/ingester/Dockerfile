# Build the ingester in a separate stage
FROM rust:1.84-alpine AS builder
WORKDIR /workdir
ENV CARGO_HOME=/workdir/.cargo
# %include ingester
COPY topsrcdir/ingester/ ./
RUN apk update && apk add --no-cache gcc libressl-dev musl-dev pkgconfig
RUN cargo build --release

# Start the main image
FROM alpine:latest
LABEL maintainer="Mozilla Release Engineering <release+docker@mozilla.com>"

# Add worker user
RUN mkdir -p /builds && \
    adduser -h /builds/worker -s /bin/ash -D worker && \
    mkdir /builds/worker/artifacts && \
    chown worker:worker /builds/worker/artifacts

# Update repositories
RUN apk update

# Setup Python
# %include requirements.txt
COPY topsrcdir/requirements.txt /requirements.txt
RUN apk add --no-cache python3 py3-pip && \
    python3 -m pip install --no-cache --upgrade --break-system-packages pip setuptools && \
    python3 -m pip install --no-cache --break-system-packages -r /requirements.txt

# Copy in the ingester from the previous stage
COPY --from=builder /workdir/target/release/ingester /usr/local/bin

# Setup other dependencies
RUN apk add bash coreutils curl git gzip jq

# %include-run-task

ENV SHELL=/bin/ash \
    HOME=/builds/worker \
    PATH=/builds/worker/.local/bin:$PATH

VOLUME /builds/worker/checkouts
VOLUME /builds/worker/.cache

# Set a default command useful for debugging
CMD ["/bin/ash"]
