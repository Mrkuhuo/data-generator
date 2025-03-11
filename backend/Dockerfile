# 使用多阶段构建
# 第一阶段：构建阶段
FROM maven:3.8.4-openjdk-17 AS builder

# 设置工作目录
WORKDIR /app

# 复制pom.xml
COPY pom.xml .

# 复制源代码
COPY src ./src

# 构建应用
RUN mvn clean package -DskipTests

# 第二阶段：运行阶段
FROM openjdk:17-slim

WORKDIR /app

# 从构建阶段复制jar文件
COPY --from=builder /app/target/*.jar app.jar

# 暴露端口
EXPOSE 8080

# 设置启动命令
ENTRYPOINT ["java","-jar","app.jar"] 