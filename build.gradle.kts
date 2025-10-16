plugins {
    java
}

group = "com.bethibande.messaging"
version = "1.0"

repositories {
    mavenCentral()
    maven {
        name = "bethibande-releases"
        url = uri("https://pckg.bethibande.com/repository/maven-releases")
    }
}

dependencies {
    implementation("com.bethibande.memory:core:1.7")

    implementation("org.openjdk.jmh:jmh-core:1.37")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}