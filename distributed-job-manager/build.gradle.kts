plugins {
    java
    kotlin("jvm")
}

dependencies {

    // JFIX
    compile(Libs.aggregating_profiler)
    compile(Libs.jfix_zookeeper)
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
    testCompile(Libs.logback)
}