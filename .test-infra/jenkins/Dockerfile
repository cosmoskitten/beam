
FROM python:2.7-alpine
MAINTAINER dataflow-engprod dataflow-engprod@google.com

RUN pip install virtualenv

RUN apk add --update openssl
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz -O gcloud.tar.gz
RUN tar xf gcloud.tar.gz
RUN ./google-cloud-sdk/install.sh --quiet
ENV PATH="/google-cloud-sdk/bin:${PATH}"
RUN gcloud components update --quiet || echo 'gcloud components update failed'
RUN gcloud -v

#ENV USER=user \
#    UID=9999 \
#    HOME=/HOME/user
#RUN groupadd --system --gid=$UID $USER; \
#    useradd --system --uid=$UID --gid $USER $USER
#RUN mkdir -p $HOME; \
#    chown -R $USER:$USER $HOME;
#USER $USER
#WORKDIR $HOME
