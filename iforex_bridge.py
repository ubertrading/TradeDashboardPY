import asyncio
import json
import logging
import websockets
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("iforex_bridge")

active_ws = None
req_counter = 0
pending_requests = {}

async def execute_js(code, timeout=10):
    """Execute JavaScript in the iFOREX browser tab and return the result."""
    global req_counter, active_ws
    if not active_ws:
        raise RuntimeError("No active browser connection! Paste the bridge snippet into the DevTools console.")
    
    req_counter += 1
    req_id = req_counter
    future = asyncio.get_event_loop().create_future()
    pending_requests[req_id] = future
    
    payload = json.dumps({"id": req_id, "code": code})
    await active_ws.send(payload)
    
    try:
        return await asyncio.wait_for(future, timeout=timeout)
    finally:
        pending_requests.pop(req_id, None)

async def handler(websocket):
    global active_ws
    active_ws = websocket
    logger.info(">>> iFOREX Browser Tab Connected successfully! <<<")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if "type" in data and data["type"] == "event":
                    logger.info("[BROWSER EVENT] %s: %s", data.get("event"), data.get("payload"))
                elif "id" in data:
                    req_id = data["id"]
                    if req_id in pending_requests:
                        if "error" in data and data["error"]:
                            pending_requests[req_id].set_exception(Exception(data["error"]))
                        else:
                            pending_requests[req_id].set_result(data.get("result"))
                else:
                    logger.info("[BROWSER MSG] %s", str(data)[:200])
            except Exception as e:
                logger.error("Error processing message: %s", e)
    except websockets.exceptions.ConnectionClosed:
        logger.warning("Browser disconnected.")
    finally:
        if active_ws == websocket:
            active_ws = None

async def run_discovery():
    """Probe the iFOREX page environment once connected."""
    while not active_ws:
        await asyncio.sleep(0.5)
    
    logger.info("Starting automated platform discovery...")
    await asyncio.sleep(1)
    
    # 1. Check page title and URL
    info = await execute_js("""
        ({
            url: window.location.href,
            title: document.title,
            localStorageKeys: Object.keys(localStorage),
            sessionStorageKeys: Object.keys(sessionStorage)
        })
    """)
    logger.info("Page Info: %s", json.dumps(info, indent=2))
    
    # 2. Extract potential auth tokens from localStorage / sessionStorage
    tokens = await execute_js("""
        (() => {
            const res = {};
            for (let k of Object.keys(localStorage)) {
                if (/token|auth|session|user|account|jwt/i.test(k)) {
                    res['local:' + k] = localStorage.getItem(k);
                }
            }
            for (let k of Object.keys(sessionStorage)) {
                if (/token|auth|session|user|account|jwt/i.test(k)) {
                    res['session:' + k] = sessionStorage.getItem(k);
                }
            }
            return res;
        })()
    """)
    logger.info("Extracted Auth/Session Keys: %s", json.dumps(tokens, indent=2))

    # 3. Search for global app objects (Angular, React, Redux, Vue, or custom objects)
    app_globals = await execute_js("""
        (() => {
            const found = [];
            for (let k of Object.keys(window)) {
                if (/trading|trade|account|app|store|state|iforex|broker|engine/i.test(k)) {
                    found.push(k);
                }
            }
            return found;
        })()
    """)
    logger.info("Found Global Objects: %s", app_globals)

    # 4. Attach continuous Network & DOM Sniffer in the tab to stream everything to Python
    await execute_js("""
        (() => {
            if (window._iforex_bridge_hooked) return 'Already hooked';
            window._iforex_bridge_hooked = true;

            // Hook fetch
            const origFetch = window.fetch;
            window.fetch = async function(...args) {
                const [resource, config] = args;
                const url = typeof resource === 'string' ? resource : (resource && resource.url) || '';
                const method = (config && config.method) || 'GET';
                const body = (config && config.body) || null;
                const headers = (config && config.headers) || null;
                
                const response = await origFetch.apply(this, args);
                const clone = response.clone();
                clone.text().then(text => {
                    try {
                        const json = JSON.parse(text);
                        window._iforex_ws_send({
                            type: 'event',
                            event: 'FETCH_RESPONSE',
                            payload: { url, method, headers, body, response: json }
                        });
                    } catch(e) {}
                }).catch(() => {});
                return response;
            };

            // Hook XHR
            const origOpen = XMLHttpRequest.prototype.open;
            const origSend = XMLHttpRequest.prototype.send;
            XMLHttpRequest.prototype.open = function(method, url) {
                this._url = url; this._method = method;
                return origOpen.apply(this, arguments);
            };
            XMLHttpRequest.prototype.send = function(body) {
                this._body = body;
                this.addEventListener('load', function() {
                    try {
                        const json = JSON.parse(this.responseText);
                        window._iforex_ws_send({
                            type: 'event',
                            event: 'XHR_RESPONSE',
                            payload: { url: this._url, method: this._method, body: this._body, response: json }
                        });
                    } catch(e) {}
                });
                return origSend.apply(this, arguments);
            };

            return 'Sniffers hooked successfully!';
        })()
    """)
    logger.info("Continuous Fetch & XHR sniffer installed in page context!")

async def main():
    async with websockets.serve(handler, "127.0.0.1", 8765):
        logger.info("WebSocket Bridge Server listening on ws://127.0.0.1:8765")
        discovery_task = asyncio.create_task(run_discovery())
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
