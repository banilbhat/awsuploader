#!/bin/bash

set -e

cat <<EOF> ~/.dockercfg
{
  "https://openthoughts.artifactoryonline.com" : {
    "auth" : "$ARTIFACTORY_DOCKER_AUTH",
    "email" : "noreply@openthoughts.in"
  }
}
EOF