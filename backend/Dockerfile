FROM maven:3.8-openjdk-11 as build

WORKDIR /app
COPY pom.xml .
COPY src ./src

# 构建应用
RUN mvn clean package -DskipTests

# 运行阶段使用较小的基础镜像
FROM openjdk:11-jre-slim

WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

# 暴露应用端口
EXPOSE 8888

# 启动应用
ENTRYPOINT ["java", "-jar", "app.jar"] 