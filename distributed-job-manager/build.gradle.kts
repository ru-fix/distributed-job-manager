plugins {
    java
    kotlin("jvm")
}

dependencies {

    compile(Libs.kotlin_jdk8)

    // JFIX
    compile(Libs.aggregating_profiler)
    compile(Libs.jfix_zookeeper) {
        exclude("org.apache.curator", "curator-recipes")
    }
    compile(Libs.jfix_concurrency)

    // Common
    compile(Libs.slf4j)
    compile(Libs.commons_io)
    compile(Libs.validation_api)
    compile(Libs.curator) {
        exclude("org.slf4j", "slf4j-api")
    }

    // JFIX Test
    testCompile(Libs.jfix_socket)

    // Test
    testCompile(Libs.junit_jupiter)
    testCompile(Libs.junit_jupiter_api)
    testCompile(Libs.mockito)
    testCompile(Libs.hamkrest)
    testCompile(Libs.curator_test)
    testCompile(Libs.log4j_core)
    testCompile(Libs.slf4j_over_log4j)
}