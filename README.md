```
swag init -g ./cmd/server/main.go  -o ./docs --parseDependency --parseInternal

```
kubectl get node -L kubernetes.io/hostname
```
{
    "constraints": [
        {
            "type": "node",
            "key": "hostname",
            "operator": "In",
            "values": [
                "ld0davo3ht3wb6w"
            ]
        }
    ]
}
```

```
      location /c/analysis/ {
        proxy_pass http://192.168.3.61:8084/;
        proxy_http_version 1.1;

        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host $http_host;
        proxy_set_header X-Forwarded-Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Port $server_port;
        proxy_set_header X-Forwarded-Prefix /c/analysis;

        proxy_redirect off;
        proxy_buffering off;
        proxy_cache off;
        add_header Cache-Control no-cache;
    }
```

```
2026-06-05T14:30:38Z ERR Provider error, retrying in 983.06358ms error="Error response from daemon: client version 1.24 is too old. Minimum supported API version is 1.40, please upgrade your client to a newer version" providerName=docker
^C2026-06-05T14:30:39Z ERR Cannot retrieve data error="context canceled" providerName=docker
```
`vim  /etc/docker/daemon.json`
```
{
  "min-api-version": "1.24"
}
```




docker run --rm \
    -p 8089:80 \
    -p 8087:8080 \
    -v /home/admin/workspace/go-project/gobrave/traefik/dynamic:/home/admin/workspace/go-project/gobrave/traefik/dynamic \
      -v /var/run/docker.sock:/var/run/docker.sock:ro \
   registry.cn-hangzhou.aliyuncs.com/wybioinfo/traefik:v3.5 \
    --api.insecure=true  \
    --providers.rest=true \
    --providers.docker=true  \
    --log.level=DEBUG  \
    --entrypoints.web.address=:80  \
    --providers.file.directory=/home/admin/workspace/go-project/gobrave/traefik/dynamic \
    --providers.file.watch=true


    docker run --rm \
  -p 8089:80 \
  -p 8087:8080 \
  -v /home/admin/workspace/go-project/gobrave/traefik/dynamic:/etc/traefik/dynamic \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
   registry.cn-hangzhou.aliyuncs.com/wybioinfo/traefik:v3.5 \
  --api.dashboard=true \
  --api.insecure=true \
  --log.level=DEBUG \
  --entrypoints.web.address=:80 \
  --entrypoints.dashboard.address=:8080 \
  --providers.docker=true \
  --providers.docker.exposedbydefault=false \
  --providers.file.directory=/etc/traefik/dynamic \
  --providers.file.watch=true





  我可以继续把 DockerExecutor 对接你现有 ContainerManager/AppSession 启动逻辑，做到真实容器执行节点。
我可以补 DAG 运行状态查询与停止接口（running registry + stop API + recent records）。
我可以补一组 DAG 调度单元测试（ready 计算、输出传播、失败分支、终止条件）。


