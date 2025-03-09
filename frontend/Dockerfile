FROM node:16 as build

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build

# 使用nginx来提供静态文件服务
FROM nginx:stable-alpine

# 复制构建产物到nginx目录
COPY --from=build /app/dist /usr/share/nginx/html

# 复制nginx配置
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"] 