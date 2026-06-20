"""Standalone script to fetch cTrader trading accounts via raw Protobuf/SSL.
No Twisted dependency — uses raw SSL sockets with proper ProtoMessage framing.

Usage: python fetch_ctrader_accounts.py <client_id> <client_secret> <access_token>
Output: JSON on stdout: {"accounts": [...]} or {"error": "..."}
"""
import sys
import json
import ssl
import socket
import struct


def main():
    if len(sys.argv) != 4:
        print(json.dumps({"error": "Usage: fetch_ctrader_accounts.py <client_id> <client_secret> <access_token>"}))
        sys.exit(1)

    client_id, client_secret, access_token = sys.argv[1], sys.argv[2], sys.argv[3]

    try:
        from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoMessage
        from ctrader_open_api.messages.OpenApiMessages_pb2 import (
            ProtoOAApplicationAuthReq,
            ProtoOAApplicationAuthRes,
            ProtoOAGetAccountListByAccessTokenReq,
            ProtoOAGetAccountListByAccessTokenRes,
        )
    except ImportError as e:
        print(json.dumps({"error": f"Import failed: {e}"}))
        sys.exit(1)

    host = "demo.ctraderapi.com"
    port = 5035
    timeout = 15

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    def send_msg(ssock, inner_msg, msg_id="1"):
        """Wrap inner protobuf message in ProtoMessage and send with length prefix."""
        wrapper = ProtoMessage()
        wrapper.payloadType = inner_msg.payloadType
        wrapper.payload = inner_msg.SerializeToString()
        if msg_id:
            wrapper.clientMsgId = msg_id
        data = wrapper.SerializeToString()
        ssock.sendall(struct.pack(">I", len(data)) + data)

    def recv_msg(ssock):
        """Receive a length-prefixed ProtoMessage."""
        header = b""
        while len(header) < 4:
            chunk = ssock.recv(4 - len(header))
            if not chunk:
                raise ConnectionError("Connection closed")
            header += chunk
        msg_len = struct.unpack(">I", header)[0]
        data = b""
        while len(data) < msg_len:
            chunk = ssock.recv(msg_len - len(data))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        wrapper = ProtoMessage()
        wrapper.ParseFromString(data)
        return wrapper

    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        ssock = ctx.wrap_socket(sock, server_hostname=host)
    except Exception as e:
        print(json.dumps({"error": f"Connection failed: {e}"}))
        sys.exit(1)

    try:
        # Step 1: App auth
        app_auth = ProtoOAApplicationAuthReq()
        app_auth.clientId = client_id
        app_auth.clientSecret = client_secret
        send_msg(ssock, app_auth, "auth")

        # Read response
        resp = recv_msg(ssock)
        app_auth_res_type = ProtoOAApplicationAuthRes().payloadType
        if resp.payloadType != app_auth_res_type:
            print(json.dumps({"error": f"App auth failed: unexpected response type {resp.payloadType}"}))
            sys.exit(1)

        # Step 2: Get account list
        acct_req = ProtoOAGetAccountListByAccessTokenReq()
        acct_req.accessToken = access_token
        send_msg(ssock, acct_req, "accts")

        # Read responses until we get account list
        acct_res_type = ProtoOAGetAccountListByAccessTokenRes().payloadType
        accounts = []
        for _ in range(10):
            resp = recv_msg(ssock)
            if resp.payloadType == acct_res_type:
                acct_res = ProtoOAGetAccountListByAccessTokenRes()
                acct_res.ParseFromString(resp.payload)
                for ta in acct_res.ctidTraderAccount:
                    accounts.append({
                        "accountId": ta.ctidTraderAccountId,
                        "traderLogin": ta.traderLogin if hasattr(ta, 'traderLogin') else "",
                        "isLive": ta.isLive if hasattr(ta, 'isLive') else False,
                    })
                break

        print(json.dumps({"accounts": accounts}))

    except Exception as e:
        print(json.dumps({"error": str(e)}))
    finally:
        ssock.close()


if __name__ == "__main__":
    main()
