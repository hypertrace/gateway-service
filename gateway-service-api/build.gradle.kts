import com.google.protobuf.gradle.*

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.8"
  id("org.hypertrace.publish-plugin")
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.6.1"
  }
  plugins {
    // Optional: an artifact spec for a protoc plugin, with "grpc" as
    // the identifier, which can be referred to in the "plugins"
    // container of the "generateProtoTasks" closure.
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.15.1"
    }
  }
  generateProtoTasks {
    ofSourceSet("main").forEach {
      it.plugins {
        // Apply the "grpc" plugin whose spec is defined above, without options.
        id("grpc_java")
      }
      it.builtins {
        java
      }
    }
  }
}

sourceSets {
  main {
    java {
      srcDirs("src/main/java", "build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }
  }
}

dependencies {
  api("io.grpc:grpc-protobuf:1.30.2")
  api("com.google.api.grpc:proto-google-common-protos:1.18.0")
  api("io.grpc:grpc-stub:1.30.2")
  api("javax.annotation:javax.annotation-api:1.3.2")
}
