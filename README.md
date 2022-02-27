# kafka-practice

## Run

```shell
./gradlew bootJar
java -cp build/libs/kafka-practice-0.0.1-SNAPSHOT.jar -Dloader.main="${mainClass}" org.springframework.boot.loader.PropertiesLauncher
```