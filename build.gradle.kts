import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.41"
    application
}

application {
    mainClassName = "timeestimator.MainKt"
}

group = "time-estimator"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(group = "org.jgrapht", name = "jgrapht-core", version = "1.3.1")
    implementation(group = "org.jgrapht", name = "jgrapht-io", version = "1.3.1")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}