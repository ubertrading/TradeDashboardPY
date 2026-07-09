import socket
import ssl

host = 'live-api.dukascopy.com'
port = 10543

print('Connecting to', host, port, 'with SSL (No SNI)...')
try:
    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_sock.settimeout(5)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    # Enable older TLS versions if needed
    try:
        ctx.options &= ~ssl.OP_NO_TLSv1
        ctx.options &= ~ssl.OP_NO_TLSv1_1
    except:
        pass
        
    # No server_hostname to avoid SNI
    sock = ctx.wrap_socket(raw_sock)
    sock.connect((host, port))
    print('SSL Handshake successful!')
    
    # Send logon with 141=Y
    logon = b'8=FIX.4.4\x019=122\x0135=A\x0149=Sanantonio272_LIVEFIX\x0156=LIVEDUKASCOPYFIX\x0134=1\x0152=20260709-12:00:00.000\x0198=0\x01108=30\x01141=Y\x01553=Sanantonio272\x01554=Pass1234!\x0110=123\x01'
    sock.sendall(logon)
    print('Sent logon with 141=Y')
    
    data = sock.recv(1024)
    if data:
        print('Received:', data)
    else:
        print('Socket closed by remote')
except Exception as e:
    print('Error:', e)
