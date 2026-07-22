FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace
COPY gradlew settings.gradle build.gradle gradle.properties ./
COPY gradle ./gradle
COPY core ./core
COPY cli ./cli
RUN chmod +x gradlew && ./gradlew :cli:installDist --no-daemon

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN useradd -r -u 10001 syncmgr
COPY --from=build /workspace/cli/build/install/syncmgr/ /app/
RUN mkdir -p /data/offsets /data/schema-history /data/dlq && chown -R syncmgr:syncmgr /data /app
USER syncmgr
ENV JAVA_OPTS="-XX:+UseG1GC -XX:MaxRAMPercentage=75"
EXPOSE 9404
ENTRYPOINT ["/app/bin/syncmgr"]
CMD ["--help"]
