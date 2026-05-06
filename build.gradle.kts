plugins {
    kotlin("jvm") version "1.9.22"
    `maven-publish`
    signing
    id("org.jetbrains.dokka") version "1.9.10"
    id("io.github.gradle-nexus.publish-plugin") version "1.3.0"
}

group = "com.molo17"
val releaseVersion = providers.environmentVariable("RELEASE_VERSION")
val defaultVersion = providers.gradleProperty("projectVersion").orElse("1.0.1-SNAPSHOT")
version = releaseVersion.orElse(defaultVersion).get()

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
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
    systemProperty("parquetkt.version", project.version.toString())
    maxHeapSize = "4g"
    jvmArgs = listOf("-Xms2g", "-Xmx4g")
}

kotlin {
    jvmToolchain(21)
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "21"
    }
}

tasks.withType<Jar> {
    manifest {
        attributes(
            mapOf(
                "Implementation-Title" to "parquetkt",
                "Implementation-Version" to project.version.toString()
            )
        )
    }
}

// Create sources jar
val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

// Create javadoc jar using Dokka
val dokkaHtml by tasks.getting(org.jetbrains.dokka.gradle.DokkaTask::class)
val javadocJar by tasks.registering(Jar::class) {
    dependsOn(dokkaHtml)
    archiveClassifier.set("javadoc")
    from(dokkaHtml.outputDirectory)
}

publishing {
    publications {
        create<MavenPublication>("mavenKotlin") {
            from(components["java"])
            artifact(sourcesJar)
            artifact(javadocJar)
            
            groupId = "com.molo17"
            artifactId = "parquetkt"
            version = project.version.toString()
            
            pom {
                name.set("ParquetKT")
                description.set("A pure Kotlin library for reading and writing Apache Parquet files")
                url.set("https://github.com/molo17/parquetkt")
                
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                
                developers {
                    developer {
                        id.set("danieleangeli")
                        name.set("Daniele Angeli")
                        email.set("daniele@molo17.com")
                        organization.set("MOLO17")
                        organizationUrl.set("https://www.molo17.com")
                    }
                }
                
                scm {
                    connection.set("scm:git:git://github.com/molo17/parquetkt.git")
                    developerConnection.set("scm:git:ssh://github.com/molo17/parquetkt.git")
                    url.set("https://github.com/molo17/parquetkt")
                }
            }
        }
    }
}

signing {
    useGpgCmd()
    sign(publishing.publications["mavenKotlin"])
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
            username.set(project.findProperty("sonatypeUsername") as String? ?: System.getenv("SONATYPE_USERNAME"))
            password.set(project.findProperty("sonatypePassword") as String? ?: System.getenv("SONATYPE_PASSWORD"))
        }
    }
}
