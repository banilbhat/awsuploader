#!/bin/bash

set -e

cat <<EOF> ~/.gradle/gradle.properties
artifactory_user=$ARTIFACTORY_USER
artifactory_password=$ARTIFACTORY_ENCRYPTED_PASSWORD
artifactory_contextUrl=http://openthoughts.artifactoryonline.com/openthoughts
org.gradle.daemon=false
EOF