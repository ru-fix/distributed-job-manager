plugins {
    java
    kotlin("jvm")
}

dependencies {

    implementation(Libs.kotlin_jdk8)

    // JFIX
    implementation(Libs.aggregating_profiler)
    implementation(Libs.jfix_zookeeper) {
        exclude("org.apache.curator", "curator-recipes")
    }
    implementation(Libs.jfix_concurrency)
    implementation(Libs.jfix_dynamic_property_api)

    // Common
    implementation(Libs.slf4j)
    implementation(Libs.log4j_kotlin)
    implementation(Libs.validation_api)
    implementation(Libs.curator) {
        exclude("org.slf4j", "slf4j-api")
    }

    // JFIX Test
    implementation(Libs.jfix_socket)

    // Test
    testImplementation(Libs.jfix_zookeeper_test)
    testImplementation(Libs.junit_jupiter)
    testImplementation(Libs.junit_jupiter_api)
    testImplementation(Libs.junit_jupiter_params)
    testImplementation(Libs.mockito)
    testImplementation(Libs.mockito_kotlin)
    testImplementation(Libs.curator_test)
    testImplementation(Libs.log4j_core)
    testImplementation(Libs.slf4j_over_log4j)
    testImplementation(Libs.awaitility)
    testImplementation(Libs.netcrusher)
    testImplementation(Libs.kotest_assertions)
}
