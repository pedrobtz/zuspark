import socket, time, threading
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("127.0.0.1", 16099)); s.listen(16)
conns = []
while True:
    c, _ = s.accept(); conns.append(c)
