plugins {
    java
    kotlin("jvm")
}

dependencies {

    compile(Libs.aggregating_profiler)
    compile(Libs.jfix_zookeeper)
    compile(Libs.jfix_concurrency)

    compile(Libs.slf4j)
    compile(Libs.commons_io)
    compile(Libs.validation_api)
    compile(Libs.curator) {
        exclude("org.slf4j", "slf4j-api")
    }

    compileOnly(Libs.lombok)
    annotationProcessor(Libs.lombok)

    testCompile(Libs.junit_jupiter)
    testCompile(Libs.junit_jupiter_api)
    testCompile(Libs.mockito)
    testCompile(Libs.hamkrest)
    testCompile(Libs.curator_test)

    testCompile(Libs.jfix_socket)

    testCompile(Libs.logback)
}