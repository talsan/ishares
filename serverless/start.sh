#!/bin/sh

export _HANDLER="etf_downloader.lambda_handler"
export _HANDLER="queue_etf_downloader.lambda_handler"

RUNTIME_ENTRYPOINT=/var/runtime/bootstrap

if [ -z "${AWS_LAMBDA_RUNTIME_API}" ]; then

  exec /usr/local/bin/aws-lambda-rie $RUNTIME_ENTRYPOINT

else

  exec $RUNTIME_ENTRYPOINT

fi
