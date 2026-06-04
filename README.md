```
swag init -g ./cmd/server/main.go  -o ./docs --parseDependency --parseInternal

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