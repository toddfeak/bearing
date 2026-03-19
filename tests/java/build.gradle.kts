plugins {
    java
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.lucene:lucene-core:10.3.2")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.register<JavaExec>("verifyIndex") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "VerifyIndex"
    val indexDir = project.findProperty("indexDir") as? String
    val docCount = project.findProperty("docCount") as? String
    args = listOfNotNull(indexDir, docCount)
}

tasks.register<JavaExec>("verifyImpacts") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "VerifyImpacts"
    val indexDir = project.findProperty("indexDir") as? String
    args = listOfNotNull(indexDir)
}

tasks.register<JavaExec>("verifyTimCompression") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "VerifyTimCompression"
    jvmArgs("--add-opens", "org.apache.lucene.core/org.apache.lucene.codecs.lucene103.blocktree=ALL-UNNAMED")
    val indexDir = project.findProperty("indexDir") as? String
    args = listOfNotNull(indexDir)
}

tasks.register<JavaExec>("indexDocValues") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "IndexDocValues"
    val indexDir = project.findProperty("indexDir") as? String
    args = listOfNotNull(indexDir)
}

tasks.register<JavaExec>("indexTermVectors") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "IndexTermVectors"
    val indexDir = project.findProperty("indexDir") as? String
    args = listOfNotNull(indexDir)
}

tasks.register<JavaExec>("indexAllFields") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "IndexAllFields"
    val docsDir = project.findProperty("docsDir") as? String
    val indexDir = project.findProperty("indexDir") as? String
    val threads = project.findProperty("threads") as? String
    val compound = project.findProperty("compound") as? String
    args = listOfNotNull(docsDir, indexDir) +
        (if (threads != null) listOf("--threads", threads) else emptyList()) +
        (if (compound != null) listOf("--compound") else emptyList())
}
