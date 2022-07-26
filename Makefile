gen_sdk_metrics:
	protoc --java_out=src/main/java -I=src/main/resources src/main/resources/sdk_metrics.proto