# 构建上下文是仓库根（见 docker-compose.yaml 的 build.context），
# 这样才能把 producer 与 layering 契约包一起拷进镜像。
FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/producer.py .
# 契约包必须随镜像一起发布：producer 与 Flink 作业 import 同一份规则实现
COPY layering ./layering

#(optional, for debugging)
EXPOSE 5000

# 固定产出 50 条样例后退出；未设置 KAFKA_BROKER 或缺少 confluent_kafka 时自动降级为纯 JSONL sink
CMD ["python", "producer.py"]
