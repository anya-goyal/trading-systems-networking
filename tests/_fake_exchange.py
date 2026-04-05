"""Fake exchange: TCP server that prints FIX messages from the IOG."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import socket

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", 6001))
srv.listen(1)
print("[EXCH1] Listening on :6001")

conn, addr = srv.accept()
print(f"[EXCH1] IOG connected from {addr}")

try:
    while True:
        data = conn.recv(4096)
        if not data:
            print("[EXCH1] IOG disconnected")
            break
        # Split on SOH and print readable FIX fields
        fields = data.replace(b"\x01", b"|").decode("ascii", errors="replace")
        print(f"[EXCH1] {fields}")
except KeyboardInterrupt:
    print("\n[EXCH1] Shutting down")
finally:
    conn.close()
    srv.close()
