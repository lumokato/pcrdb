"""
自定义 HTTP 服务器
为 ES Module (.js) 文件设置正确的 MIME 类型
"""
import http.server
import socketserver

PORT = 8433

class ESModuleHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="frontend", **kwargs)
    
    def guess_type(self, path):
        if path.endswith('.js'):
            return 'application/javascript'
        return super().guess_type(path)

if __name__ == "__main__":
    with socketserver.TCPServer(("", PORT), ESModuleHandler) as httpd:
        print(f"Serving at http://localhost:{PORT}")
        httpd.serve_forever()
