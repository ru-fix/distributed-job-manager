
object Vers {
    val kotlin = "1.3.41"

    val slf4j = "1.7.25"
    val logback = "1.1.11"

    val dokka = "0.9.18"
    val gradle_release_plugin = "1.3.8"

    val aggregating_profiler = "1.4.13"
    val jfix_zookeeper = "1.0.4"
    val jfix_concurrency = "1.0.22"
    val jfix_socket = "1.0.30"

    val validation_api = "2.0.1.Final"
    val curator = "2.10.0"
    val commons_io = "2.6"

    val lombok = "1.18.8"
    val junit = "5.5.1"
    val hamkrest = "1.3"
    val mockito = "2.21.0"
}

object Libs {

    // Kotlin
    val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"

    // JFIX
    val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    val aggregating_profiler = "ru.fix:aggregating-profiler:${Vers.aggregating_profiler}"
    val jfix_zookeeper = "ru.fix:jfix-zookeeper:${Vers.jfix_zookeeper}"
    val jfix_concurrency = "ru.fix:jfix-stdlib-concurrency:${Vers.jfix_concurrency}"

    // Common
    val commons_io = "commons-io:commons-io:${Vers.commons_io}"
    val lombok = "org.projectlombok:lombok:${Vers.lombok}"
    val validation_api = "javax.validation:validation-api:${Vers.validation_api}"
    val slf4j = "org.slf4j:slf4j-api:${Vers.slf4j}"
    val curator = "org.apache.curator:curator-recipes:${Vers.curator}"
    val dokka_gradle_plugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokka}"

    // JFIX Test
    val jfix_socket = "ru.fix:jfix-stdlib-socket:${Vers.jfix_socket}"

    // Test
    val junit_jupiter_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit}"
    val junit_jupiter = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit}"
    val logback = "ch.qos.logback:logback-classic:${Vers.logback}"
    val curator_test = "org.apache.curator:curator-test:${Vers.curator}"
    val mockito = "org.mockito:mockito-core:${Vers.mockito}"
    val hamkrest = "org.hamcrest:hamcrest-all:${Vers.hamkrest}"
}