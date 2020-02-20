plugins {
    kotlin("jvm") version "1.3.60"
}


dependencies {

    implementation(kotlin("stdlib-jdk8"))

    // Gradle plugin
    implementation(gradleApi())

    // Align versions of all Kotlin components
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))

    // Use the Kotlin JDK 8 standard library.
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("org.junit.jupiter:junit-jupiter:5.5.2")
    testImplementation("junit:junit:4.12")

    implementation("org.apache.arrow:arrow-memory:0.16.0")
    implementation("org.apache.arrow:arrow-vector:0.16.0")
    implementation("org.apache.hadoop:hadoop-common:3.1.0")
    implementation("org.apache.parquet:parquet-arrow:1.11.0")
    implementation("org.apache.parquet:parquet-common:1.11.0")
    implementation("org.apache.parquet:parquet-column:1.11.0")
    implementation("org.apache.parquet:parquet-hadoop:1.11.0")

    implementation("com.github.doyaaaaaken:kotlin-csv-jvm:0.7.3")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines:0.19.2")

}

//
//tasks.test {
//    useJUnitPlatform()
//    testLogging {
//        events("passed", "skipped", "failed")
//    }
//}
