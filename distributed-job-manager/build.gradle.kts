plugins {
    java
    kotlin("jvm")
}

dependencies {

    compile(Libs.aggregating_profiler)
    compile(Libs.jfix_zookeeper)
    compile(Libs.jfix_concurrency)
    compile(Libs.lombok)
    compile(Libs.validation_api)
    compile(Libs.slf4j)
    compile(Libs.curator) {
        exclude("org.slf4j", "slf4j-api")
    }
    compile(Libs.commons_io)
    compileOnly("org.projectlombok:lombok:1.18.8")
    annotationProcessor("org.projectlombok:lombok:1.18.8")

    testCompile(Libs.jfix_socket)
    testCompile(Libs.logback)
    testCompile(Libs.curator_test)
    testCompile(Libs.mockito)
    testCompile(Libs.hamkrest)
    testCompile(Libs.junit_jupiter)
    testCompile(Libs.junit_vintage)

}