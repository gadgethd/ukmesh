# Used only by the isolated app preview. No visitor session headers cross it.
proxy_http_version 1.1;
proxy_set_header Host $http_host;
proxy_set_header Cookie "";
proxy_set_header Authorization "";
proxy_set_header X-Forwarded-Host "";
proxy_set_header X-Forwarded-For "";
proxy_set_header X-Real-IP "";
proxy_hide_header Set-Cookie;
proxy_connect_timeout 2s;
proxy_send_timeout 5s;
proxy_read_timeout 10s;
proxy_redirect off;
