import com.google.protobuf.gradle.id

plugins {
  `java-library`
  id("com.google.protobuf") version "0.9.4"
  id("org.hypertrace.publish-plugin")
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.21.12"
  }
  plugins {
    // Optional: an artifact spec for a protoc plugin, with "grpc" as
    // the identifier, which can be referred to in the "plugins"
    // container of the "generateProtoTasks" closure.
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.60.0"
    }
  }
  generateProtoTasks {
    ofSourceSet("main").forEach {
      it.plugins {
        // Apply the "grpc" plugin whose spec is defined above, without options.
        id("grpc_java")
      }
    }
  }
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }
  }
}

dependencies {
  api(platform("io.grpc:grpc-bom:1.60.0"))
  api("io.grpc:grpc-protobuf")
  api("io.grpc:grpc-stub")
  api("javax.annotation:javax.annotation-api:1.3.2")
  constraints {
    implementation("com.google.guava:guava:32.1.2-jre") {
      because("https://nvd.nist.gov/vuln/detail/CVE-2023-2976")
    }
  }
}
