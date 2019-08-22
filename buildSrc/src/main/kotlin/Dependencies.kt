
object Vers {
    //Plugins
    const val dokkav = "0.9.18"
    const val gradle_release_plugin = "1.3.9"

    //Dependencies
    const val kotlin = "1.3.41"

    const val slf4j = "1.7.28"
    const val logback = "1.2.3"

    const val aggregating_profiler = "1.4.13"
    const val jfix_zookeeper = "1.0.4"
    const val jfix_concurrency = "1.0.42"
    const val jfix_socket = "1.0.42"

    const val validation_api = "2.0.1.Final"
    const val curator = "2.10.0"
    const val commons_io = "2.6"

    const val junit = "5.5.1"
    const val hamkrest = "1.3"
    const val mockito = "3.0.0"
}

object Libs {
    //Plugins
    val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    val dokka_gradle_plugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"
    val nexus_staging_plugin = "io.codearte.nexus-staging"
    val nexus_publish_plugin = "de.marcphilipp.nexus-publish"

    //Dependencies

    // Kotlin
    const val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    const val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    const val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"

    // JFIX
    const val aggregating_profiler = "ru.fix:aggregating-profiler:${Vers.aggregating_profiler}"
    const val jfix_zookeeper = "ru.fix:jfix-zookeeper:${Vers.jfix_zookeeper}"
    const val jfix_concurrency = "ru.fix:jfix-stdlib-concurrency:${Vers.jfix_concurrency}"

    // Common
    const val commons_io = "commons-io:commons-io:${Vers.commons_io}"
    const val validation_api = "javax.validation:validation-api:${Vers.validation_api}"
    const val slf4j = "org.slf4j:slf4j-api:${Vers.slf4j}"
    const val curator = "org.apache.curator:curator-recipes:${Vers.curator}"

    // JFIX Test
    const val jfix_socket = "ru.fix:jfix-stdlib-socket:${Vers.jfix_socket}"

    // Test
    const val junit_jupiter_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit}"
    const val junit_jupiter = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit}"
    const val logback = "ch.qos.logback:logback-classic:${Vers.logback}"
    const val curator_test = "org.apache.curator:curator-test:${Vers.curator}"
    const val mockito = "org.mockito:mockito-core:${Vers.mockito}"
    const val hamkrest = "org.hamcrest:hamcrest-all:${Vers.hamkrest}"
}