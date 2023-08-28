gen_sdk_metrics:
	protoc --java_out=src/main/java -I=src/main/resources src/main/resources/byteplus_rec_sdk_metrics.proto