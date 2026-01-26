plugins {
    kotlin("jvm") version "1.9.22"
    `maven-publish`
    id("org.jetbrains.dokka") version "1.9.10"
}

group = "com.molo17.parquetkt"
version = "1.0.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Compression libraries (for encoding/decoding support)
    implementation("org.xerial.snappy:snappy-java:1.1.10.5")
    implementation("org.apache.commons:commons-compress:1.26.0")
    implementation("com.github.luben:zstd-jni:1.5.5-11")
    
    // Kotlin standard library
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    
    // Coroutines for async operations (future use)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    
    // Logging
    implementation("org.slf4j:slf4j-api:2.0.12")
    implementation("ch.qos.logback:logback-classic:1.5.0")
    
    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("io.mockk:mockk:1.13.10")
    testImplementation("org.assertj:assertj-core:3.25.3")
    testImplementation("org.json:json:20240303")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
    maxHeapSize = "4g"
    jvmArgs = listOf("-Xms2g", "-Xmx4g")
}

kotlin {
    jvmToolchain(17)
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "17"
    }
}
