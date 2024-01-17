FROM pgxn/pgxn-tools:latest

ARG PGVERSION

ENV PGTAP_VERSION=1.3.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        guile-3.0 \
        guile-3.0-dev \
        inotify-tools \
        unzip \
        wget \
        xxd && \
    mkdir -p /opt/src/pgtap/ && \
    cd /opt/src/pgtap && \
    wget https://api.pgxn.org/dist/pgtap/$PGTAP_VERSION/pgtap-$PGTAP_VERSION.zip && \
    unzip pgtap-$PGTAP_VERSION.zip && \
    apt-get remove -y unzip wget && \
    apt-get clean -y && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/*

ADD pg-install.sh /usr/local/bin/pg-install.sh
RUN bash pg-install.sh $PGVERSION

CMD pg_ctlcluster $PGVERSION test start
