FROM python:2.7-alpine
MAINTAINER dataflow-endprod dataflow-engprod@google.com
RUN echo test test! && python --version
RUN apk add --update openssl

RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz -O gcloud.tar.gz
RUN tar xf gcloud.tar.gz
RUN ./google-cloud-sdk/install.sh --quiet
RUN . ./google-cloud-sdk/path.bash.inc
RUN gcloud components update --quiet || echo 'gcloud components update failed'
RUN gcloud -v
