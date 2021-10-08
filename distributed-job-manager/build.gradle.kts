plugins {
    java
    kotlin("jvm")
}

dependencies {

    implementation(Libs.kotlin_jdk8)

    // JFIX
    api(Libs.aggregating_profiler)
    api(Libs.jfix_zookeeper) {
        exclude("org.apache.curator", "curator-recipes")
    }
    api(Libs.jfix_concurrency)
    api(Libs.jfix_dynamic_property_api)

    // Common
    implementation(Libs.slf4j)
    implementation(Libs.log4j_kotlin)
    implementation(Libs.validation_api)
    api(Libs.curator) {
        exclude("org.slf4j", "slf4j-api")
    }

    // JFIX Test
    testImplementation(Libs.jfix_zookeeper_test)

    // Test
    testImplementation(Libs.kotlinx_coroutines_core)
    testImplementation(Libs.junit_jupiter)
    testImplementation(Libs.mockk)
    testImplementation(Libs.mockito_kotlin)
    testImplementation(Libs.curator_test)
    testImplementation(Libs.log4j_core)
    testImplementation(Libs.slf4j_over_log4j)
    testImplementation(Libs.awaitility)
    testImplementation(Libs.kotest_assertions)
    testImplementation(Libs.netcrusher)
}
