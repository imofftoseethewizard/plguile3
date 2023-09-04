FROM pgxn/pgxn-tools:latest

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        guile-3.0 \
        guile-3.0-dev \
        unzip \
        wget && \
    apt-get clean && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/*

RUN mkdir -p /opt/pgtap/ &&
    cd /opt/pgtap && \
    wget https://api.pgxn.org/dist/pgtap/1.3.0/pgtap-1.3.0.zip && \
    unzip pgtap-1.3.0.zip && \
    cd pgtap-1.3.0 && \
    make
