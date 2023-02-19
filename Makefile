protoco_gen: 
	# protoc proto/*.proto
build_push_to_kind:
	docker build . -t outbox-service
	kind load docker-image outbox-service --name micro-service
