import http.server
import sys


def handler_class(static_content):
    class DummyHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Length', len(static_content))
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(content.encode())

    return DummyHandler

if __name__ == '__main__':
    port = int(sys.argv[1])
    content = sys.argv[2]
    server = http.server.HTTPServer(
            ('localhost', port),
            handler_class(content))
    server.serve_forever()
