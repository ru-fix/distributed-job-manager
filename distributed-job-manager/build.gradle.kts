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
    implementation(Libs.jfix_zookeeper_test)
    implementation(Libs.jfix_concurrency)
    implementation(Libs.jfix_dynamic_property_api)

    // Common
    implementation(Libs.slf4j)
    implementation(Libs.commons_io)
    implementation(Libs.validation_api)
    implementation(Libs.curator) {
        exclude("org.slf4j", "slf4j-api")
    }

    // JFIX Test
    implementation(Libs.jfix_socket)

    // Test
    implementation(Libs.junit_jupiter)
    implementation(Libs.junit_jupiter_api)
    implementation(Libs.junit_jupiter_params)
    implementation(Libs.mockito)
    implementation(Libs.hamkrest)
    implementation(Libs.curator_test)
    implementation(Libs.log4j_core)
    implementation(Libs.slf4j_over_log4j)
    implementation(Libs.awaitility)
}
