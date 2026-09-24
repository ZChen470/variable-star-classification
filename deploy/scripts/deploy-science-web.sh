#!/usr/bin/env bash
set -Eeuo pipefail

NAME="science-classifier-web"
BACKUP="science-classifier-web-deploy-backup"
NAMESPACE="variable-star"
GATEWAY_SERVICE="triton-gateway"

MODEL_BUNDLE_VERSION="variable-classifier-2026-07-003"
MODEL_BUNDLE_MANIFEST_PATH="/app/models/bundles/model-bundle-manifest-v2.yaml"
SCIENCE_WEB_LISTEN_ADDR="0.0.0.0:8088"

# 每次部署时读取当前 Kubernetes Service IP，避免使用迁移前的 Docker 地址。
GATEWAY_IP="$(kubectl -n "$NAMESPACE" get svc "$GATEWAY_SERVICE" -o jsonpath='{.spec.clusterIP}')"

if [[ -z "$GATEWAY_IP" || "$GATEWAY_IP" == "None" ]]; then
    echo "ERROR: 无法获取 Triton Gateway ClusterIP" >&2
    exit 1
fi

TRITON_BASE_URL="http://${GATEWAY_IP}:8000"

# 启动前验证 Gateway 和实际模型版本。
curl -fsS --max-time 5 \
    "${TRITON_BASE_URL}/v2/models/variable_star_classifier/versions/1/ready" \
    >/dev/null

echo "TRITON_GATEWAY_READY=PASS"
echo "TRITON_BASE_URL=${TRITON_BASE_URL}"

# 禁止覆盖此前部署失败时可能留下的备份。
if docker container inspect "$BACKUP" >/dev/null 2>&1; then
    echo "ERROR: 备份容器已经存在，请先检查" >&2
    exit 1
fi

# 默认复用当前正在使用的镜像；首次部署时使用本地镜像标签。
# 如需部署新镜像，可以显式指定 SCIENCE_WEB_IMAGE。
if [[ -n "${SCIENCE_WEB_IMAGE:-}" ]]; then
    IMAGE="$SCIENCE_WEB_IMAGE"
elif docker container inspect "$NAME" >/dev/null 2>&1; then
    IMAGE="$(docker inspect -f '{{.Image}}' "$NAME")"
else
    IMAGE="variable-star-science-web:latest"
fi

docker image inspect "$IMAGE" >/dev/null

rollback() {
    trap - ERR
    echo "ERROR: 新前端部署失败，尝试恢复原容器" >&2

    # 只有备份已经存在，才能删除新容器并恢复旧容器。
    # 旧容器停止或改名失败时，绝不能删除仍名为 $NAME 的原容器。
    if docker container inspect "$BACKUP" >/dev/null 2>&1; then
        if docker container inspect "$NAME" >/dev/null 2>&1; then
            docker rm -f "$NAME" >/dev/null || true
        fi
        docker rename "$BACKUP" "$NAME" &&
        docker update --restart unless-stopped "$NAME" >/dev/null &&
        docker start "$NAME" >/dev/null || true
    elif docker container inspect "$NAME" >/dev/null 2>&1; then
        docker update --restart unless-stopped "$NAME" >/dev/null || true
        docker start "$NAME" >/dev/null || true
    fi

    exit 1
}

# 在停止或改名旧容器之前启用失败回滚。
trap rollback ERR

# 如果已有前端，停止并保留为临时回滚备份。
if docker container inspect "$NAME" >/dev/null 2>&1; then
    docker update --restart=no "$NAME" >/dev/null
    docker stop -t 10 "$NAME" >/dev/null
    docker rename "$NAME" "$BACKUP"
fi

# 仅创建科学测试前端；不复制旧容器中可能过时的 Triton 地址。
docker run -d \
    --name "$NAME" \
    --network host \
    --restart unless-stopped \
    --user 65532:65532 \
    --workdir /app \
    --entrypoint /app/science-classifier-web \
    -e "MODEL_BUNDLE_VERSION=${MODEL_BUNDLE_VERSION}" \
    -e "MODEL_BUNDLE_MANIFEST_PATH=${MODEL_BUNDLE_MANIFEST_PATH}" \
    -e "SCIENCE_WEB_LISTEN_ADDR=${SCIENCE_WEB_LISTEN_ADDR}" \
    -e "TRITON_BASE_URL=${TRITON_BASE_URL}" \
    "$IMAGE" >/dev/null

# 验证前端成功启动，而不只是 Docker 容器处于 Running 状态。
READY=0

for i in $(seq 1 15); do
    if curl -fsS --max-time 2 http://127.0.0.1:8088/ >/dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 1
done

if [[ "$READY" != 1 ]]; then
    echo "ERROR: 科学测试前端未通过 HTTP 启动检查" >&2
    false
fi

trap - ERR

# HTTP 200 仅证明页面可访问；真实分类通过之前保留旧容器。
echo "BACKUP_CONTAINER=${BACKUP}"
echo "BACKUP_RETAINED=YES"

echo "SCIENCE_WEB_DEPLOY=PASS"
echo "WEB_URL=http://127.0.0.1:8088/"
echo "TRITON_BASE_URL=${TRITON_BASE_URL}"
