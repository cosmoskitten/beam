
FROM python:2.7-alpine
MAINTAINER dataflow-engprod dataflow-engprod@google.com

RUN pip install virtualenv
RUN apk update && apk add bash && apk add openssl
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz -O gcloud.tar.gz
RUN tar xf gcloud.tar.gz
RUN ./google-cloud-sdk/install.sh --quiet
ENV PATH="/google-cloud-sdk/bin:/usr/sbin:${PATH}"
RUN gcloud components update --quiet || echo 'gcloud components update failed'
RUN echo $PATH && gcloud --version
