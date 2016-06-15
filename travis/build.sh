#! /bin/bash
set -e

openssl aes-256-cbc -k "$key_password" -in ./travis/inn-oss-public.enc -out ./inn-oss-public.asc -d
openssl aes-256-cbc -k "$key_password" -in ./travis/inn-oss-private.enc -out ./inn-oss-private.asc -d
gpg --dearmor ./inn-oss-public.asc
gpg --dearmor ./inn-oss-private.asc

ls -la

if [[ "$TRAVIS_PULL_REQUEST" == "false" && "$TRAVIS_BRANCH" == "master" ]]; then
    mvn clean install --settings=./settings-deploy.xml
    mvn deploy -DskipTests=true --settings=./settings-deploy.xml
else
    mvn clean install --settings=./settings-deploy.xml
fi
