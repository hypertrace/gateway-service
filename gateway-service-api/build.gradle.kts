import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.13"
  id("org.hypertrace.publish-plugin")
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.17.3"
  }
  plugins {
    // Optional: an artifact spec for a protoc plugin, with "grpc" as
    // the identifier, which can be referred to in the "plugins"
    // container of the "generateProtoTasks" closure.
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.43.1"
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
  api("io.grpc:grpc-protobuf:1.43.1")
  api("com.google.api.grpc:proto-google-common-protos:2.3.2")
  api("io.grpc:grpc-stub:1.43.1")
  api("javax.annotation:javax.annotation-api:1.3.2")

  constraints {
    api("com.google.guava:guava:30.1.1-jre") {
      because("Information Disclosure [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415] in com.google.guava:guava@29.0-android")
    }
  }
}
