FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ARG VITE_ANALYTICS_API_BASE
ARG VITE_FORECASTING_API_BASE
ENV VITE_ANALYTICS_API_BASE=$VITE_ANALYTICS_API_BASE
ENV VITE_FORECASTING_API_BASE=$VITE_FORECASTING_API_BASE
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80