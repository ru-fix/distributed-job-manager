object Vers {
    //Plugins
    const val dokkav = "0.10.1"
    const val gradle_release_plugin = "1.3.17"
    const val asciidoctor = "1.6.0"

    //Dependencies
    const val kotlin = "1.3.72"

    const val slf4j = "1.7.30"

    const val aggregating_profiler = "1.6.6"
    const val jfix_zookeeper = "1.1.7"
    const val jfix_stdlib = "3.0.12"
    const val jfix_dynamic_property = "2.0.7"

    const val validation_api = "2.0.1.Final"
    const val curator = "5.0.0"

    const val junit = "5.6.2"
    const val mockito = "3.3.3"
    const val mockito_kotlin = "2.2.0"
    const val awaitility = "4.0.3"

    const val log4j = "2.13.3"
}

object Libs {
    //Plugins
    const val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    const val dokka_gradle_plugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"
    const val nexus_staging_plugin = "io.codearte.nexus-staging"
    const val nexus_publish_plugin = "de.marcphilipp.nexus-publish"
    const val asciidoctor = "org.asciidoctor:asciidoctor-gradle-plugin:${Vers.asciidoctor}"

    //Dependencies

    // Kotlin
    const val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    const val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    const val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"

    // JFIX
    const val aggregating_profiler = "ru.fix:aggregating-profiler:${Vers.aggregating_profiler}"
    const val jfix_zookeeper = "ru.fix:jfix-zookeeper:${Vers.jfix_zookeeper}"
    const val jfix_zookeeper_test = "ru.fix:jfix-zookeeper-test:${Vers.jfix_zookeeper}"
    const val jfix_concurrency = "ru.fix:jfix-stdlib-concurrency:${Vers.jfix_stdlib}"
    const val jfix_dynamic_property_api = "ru.fix:dynamic-property-api:${Vers.jfix_dynamic_property}"

    // Common
    const val validation_api = "javax.validation:validation-api:${Vers.validation_api}"
    const val slf4j = "org.slf4j:slf4j-api:${Vers.slf4j}"
    const val curator = "org.apache.curator:curator-recipes:${Vers.curator}"

    const val log4j_core = "org.apache.logging.log4j:log4j-core:${Vers.log4j}"
    const val log4j_kotlin = "org.apache.logging.log4j:log4j-api-kotlin:1.0.0"
    const val slf4j_over_log4j = "org.apache.logging.log4j:log4j-slf4j-impl:${Vers.log4j}"

    // JFIX Test
    const val jfix_socket = "ru.fix:jfix-stdlib-socket:${Vers.jfix_stdlib}"

    // Test
    const val junit_jupiter = "org.junit.jupiter:junit-jupiter:${Vers.junit}"
    const val curator_test = "org.apache.curator:curator-test:${Vers.curator}"
    const val mockito = "org.mockito:mockito-core:${Vers.mockito}"
    const val mockito_kotlin = "com.nhaarman.mockitokotlin2:mockito-kotlin:${Vers.mockito_kotlin}"
    const val awaitility = "org.awaitility:awaitility:${Vers.awaitility}"
    const val kotest_assertions = "io.kotest:kotest-assertions-core:4.1.1"
    const val netcrusher = "com.github.netcrusherorg:netcrusher-core:0.10"
}
