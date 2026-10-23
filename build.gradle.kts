import java.util.zip.ZipFile

plugins {
    java
    application
    id("com.gradleup.shadow") version "9.0.2"
}

group = "io.alerting"
version = "0.1.0"

repositories {
    mavenCentral()
}

val beamVersion = "2.61.0"
val protobufVersion = "3.25.5"

dependencies {
    implementation("org.apache.beam:beam-sdks-java-core:$beamVersion")
    implementation("org.apache.beam:beam-sdks-java-io-kafka:$beamVersion") {
        exclude(group = "io.confluent") // Avro/Schema Registry support is not used
    }
    implementation("org.apache.beam:beam-runners-flink-1.19:$beamVersion")
    implementation("org.apache.beam:beam-runners-direct-java:$beamVersion")
    implementation("org.apache.kafka:kafka-clients:3.7.1")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.hamcrest:hamcrest:2.2")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

// Targets the self-hosted Flink 1.19 / Java 17 runtime (flink:1.19-java17 image).
tasks.compileJava { options.release.set(17); options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-processing,-options")) }
tasks.compileTestJava { options.release.set(17) }

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "2g"
    testLogging { events("passed", "failed", "skipped"); exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
}

application {
    mainClass.set("io.alerting.pipeline.AlertingPipelineMain")
}

// Beam registers coders, runners and IOs through META-INF/services, and many jars ship the same service
// file. Shadow 9 keeps only the first copy unless duplicates are included wholesale (which also duplicates
// thousands of classes), so the merged service files are generated here and win as the first copy.
val mergeServiceFiles by tasks.registering {
    val runtime = configurations.runtimeClasspath
    val outDir = layout.buildDirectory.dir("generated/merged-services")
    inputs.files(runtime)
    outputs.dir(outDir)
    doLast {
        val merged = sortedMapOf<String, LinkedHashSet<String>>()
        runtime.get().files.filter { it.name.endsWith(".jar") }.forEach { jar ->
            ZipFile(jar).use { zip ->
                zip.entries().asSequence()
                    .filter { !it.isDirectory && it.name.startsWith("META-INF/services/") }
                    .forEach { entry ->
                        val lines = zip.getInputStream(entry).bufferedReader().readLines()
                            .map { it.substringBefore('#').trim() }.filter { it.isNotEmpty() }
                        merged.getOrPut(entry.name) { linkedSetOf() }.addAll(lines)
                    }
            }
        }
        val root = outDir.get().asFile
        root.deleteRecursively()
        merged.forEach { (name, lines) ->
            val f = root.resolve(name)
            f.parentFile.mkdirs()
            f.writeText(lines.joinToString("\n", postfix = "\n"))
        }
    }
}

tasks.shadowJar {
    archiveClassifier.set("all")
    // The Flink cluster provides Flink itself; bundling a second copy causes classloading conflicts.
    // Local embedded runs use `./gradlew run`, which has the full classpath.
    dependencies {
        exclude(dependency("org.apache.flink:.*:.*"))
    }
    dependsOn(mergeServiceFiles)
    from(mergeServiceFiles)
    isZip64 = true
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
