# Public companion reads never forward visitor sessions or request bodies.
proxy_http_version 1.1;
proxy_set_header Host $http_host;
proxy_set_header Cookie "";
proxy_set_header Authorization "";
proxy_set_header X-Forwarded-Host "";
proxy_set_header X-Forwarded-For "";
proxy_set_header X-Real-IP "";
proxy_set_header Content-Length "";
proxy_pass_request_body off;
proxy_hide_header Set-Cookie;
proxy_connect_timeout 2s;
proxy_send_timeout 5s;
proxy_redirect off;
