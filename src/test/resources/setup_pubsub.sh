#!/bin/sh

# Wait for pubsub-emulator to come up
bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' '$PUBSUB_SETUP_HOST')" != "200" ]]; do sleep 1; done'

curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/topics/event_new-case
curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/subscriptions/event_new-case_rm-case-processor -H 'Content-Type: application/json' -d '{"topic": "projects/our-project/topics/event_new-case"}'

curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/topics/event_refusal
curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/subscriptions/event_refusal_rm-case-processor -H 'Content-Type: application/json' -d '{"topic": "projects/our-project/topics/event_refusal"}'

curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/topics/event_invalid-case
curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/subscriptions/event_invalid-case_rm-case-processor -H 'Content-Type: application/json' -d '{"topic": "projects/our-project/topics/event_invalid-case"}'

curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/topics/event_update-sample-sensitive
curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/subscriptions/event_update-sample-sensitive_rm-case-processor -H 'Content-Type: application/json' -d '{"topic": "projects/our-project/topics/event_update-sample-sensitive"}'

curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/topics/event_update-sample
curl -X PUT http://$PUBSUB_SETUP_HOST/v1/projects/our-project/subscriptions/event_update-sample_rm-case-processor -H 'Content-Type: application/json' -d '{"topic": "projects/our-project/topics/event_update-sample"}'
