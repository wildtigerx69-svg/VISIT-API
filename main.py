from flask import Flask, jsonify
import aiohttp
import asyncio
import json
import os
import sys
import time
from functools import lru_cache
from byte import encrypt_api, Encrypt_ID
from visit_count_pb2 import Info

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "status": "online",
        "endpoint": "/visit/<server>/<uid>",
        "example": "/visit/bd/14502384617",
        "target": "50,000 visits"
    }), 200

@lru_cache(maxsize=5)
def load_tokens(server_name):
    try:
        path_map = {
            "IND": "token_ind.json",
            "BR": "token_br.json",
            "US": "token_br.json",
            "SAC": "token_br.json",
            "NA": "token_br.json",
            "BD": "token_bd.json"
        }
        path = path_map.get(server_name.upper(), "token_bd.json")
        
        print(f"📂 Loading tokens from: {path}")
        sys.stdout.flush()

        with open(path, "r") as f:
            data = json.load(f)

        tokens = [item["token"] for item in data if item.get("token") not in ["", "N/A", None]]
        print(f"✅ Loaded {len(tokens)} tokens for {server_name}")
        print(f"📝 First token: {tokens[0][:20]}... (if exists)")
        sys.stdout.flush()
        return tokens
    except FileNotFoundError:
        print(f"❌ File not found: {path}")
        sys.stdout.flush()
        return []
    except Exception as e:
        print(f"❌ Token load error: {e}")
        sys.stdout.flush()
        return []

def get_url(server_name):
    server = server_name.upper()
    url_map = {
        "IND": "https://client.ind.freefiremobile.com/GetPlayerPersonalShow",
        "BR": "https://client.us.freefiremobile.com/GetPlayerPersonalShow",
        "US": "https://client.us.freefiremobile.com/GetPlayerPersonalShow",
        "SAC": "https://client.us.freefiremobile.com/GetPlayerPersonalShow",
        "NA": "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    }
    return url_map.get(server, "https://clientbp.ggblueshark.com/GetPlayerPersonalShow")

def parse_protobuf_response(response_data):
    try:
        info = Info()
        info.ParseFromString(response_data)
        return {
            "uid": info.AccountInfo.UID or 0,
            "nickname": info.AccountInfo.PlayerNickname or "",
            "likes": info.AccountInfo.Likes or 0,
            "region": info.AccountInfo.PlayerRegion or "",
            "level": info.AccountInfo.Levels or 0
        }
    except Exception as e:
        print(f"❌ Protobuf parse error: {e}")
        return None

async def visit(session, url, token, data):
    headers = {
        "ReleaseVersion": "OB53",
        "X-GA": "v1 1",
        "Authorization": f"Bearer {token}",
        "Host": url.replace("https://", "").split("/")[0],
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "*/*",
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-G973F Build/PPR1.180610.011)"
    }
    try:
        async with session.post(
            url, 
            headers=headers, 
            data=data, 
            ssl=False, 
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            status = resp.status
            if status == 200:
                response_data = await resp.read()
                return True, response_data
            else:
                print(f"⚠️ HTTP {status} for token: {token[:20]}...")
                sys.stdout.flush()
                return False, None
    except asyncio.TimeoutError:
        print(f"⏱️ Timeout for token: {token[:20]}...")
        sys.stdout.flush()
        return False, None
    except Exception as e:
        print(f"❌ Request error: {e}")
        sys.stdout.flush()
        return False, None

async def send_visits_async(tokens, uid, server_name, target=50000):
    url = get_url(server_name)
    token_len = len(tokens)
    
    print(f"🔗 URL: {url}")
    print(f"🔑 Using {token_len} tokens")
    print(f"👤 Target UID: {uid}")
    sys.stdout.flush()
    
    encrypted = encrypt_api("08" + Encrypt_ID(str(uid)) + "1801")
    data = bytes.fromhex(encrypted)
    
    print(f"🔐 Encrypted data: {encrypted[:50]}...")
    sys.stdout.flush()
    
    success = 0
    sent = 0
    fail = 0
    player_info = None
    
    connector = aiohttp.TCPConnector(
        limit=50, 
        ssl=False,
        force_close=True
    )
    
    timeout = aiohttp.ClientTimeout(total=15, connect=10)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        while success < target:
            batch = min(target - success, 100)
            tasks = []
            
            for i in range(batch):
                token = tokens[(sent + i) % token_len]
                tasks.append(visit(session, url, token, data))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            if player_info is None:
                for result in results:
                    if isinstance(result, tuple) and result[0] and result[1]:
                        player_info = parse_protobuf_response(result[1])
                        if player_info:
                            print(f"✅ Got player info: {player_info.get('nickname', 'N/A')}")
                            sys.stdout.flush()
                            break
            
            batch_success = 0
            batch_fail = 0
            
            for result in results:
                sent += 1
                if isinstance(result, tuple) and result[0]:
                    success += 1
                    batch_success += 1
                else:
                    fail += 1
                    batch_fail += 1
            
            print(f"📊 Batch: {batch_success} ok, {batch_fail} fail | Total: {success}/{target}")
            sys.stdout.flush()
            
            # If first batch completely fails, try to debug
            if success == 0 and sent >= 50:
                print(f"❌ No success after {sent} attempts!")
                print(f"🔍 Check: URL={url}, Tokens valid? Data correct?")
                sys.stdout.flush()
            
            # Stop if too many failures
            if sent > 0 and success == 0 and sent > 500:
                print(f"🛑 Stopping: No successful visits after {sent} attempts")
                sys.stdout.flush()
                break
    
    return success, sent, fail, player_info

@app.route('/visit/<string:server>/<int:uid>', methods=['GET'])
def visit_endpoint(server, uid):
    start_time = time.time()
    server = server.upper()
    target = 50000
    
    print(f"\n{'='*50}")
    print(f"📥 NEW REQUEST: server={server}, uid={uid}, target={target}")
    print(f"{'='*50}")
    sys.stdout.flush()
    
    tokens = load_tokens(server)
    if not tokens:
        return jsonify({
            "error": f"No tokens found for server: {server}",
            "hint": "Check if token file exists and has valid tokens"
        }), 500
    
    try:
        success, sent, fail, player_info = asyncio.run(
            send_visits_async(tokens, uid, server, target)
        )
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        return jsonify({"error": str(e)}), 500
    
    elapsed = round(time.time() - start_time, 2)
    
    response = {
        "status": "completed" if success >= target else "partial",
        "target": target,
        "success": success,
        "fail": fail,
        "total_sent": sent,
        "time_seconds": elapsed,
        "time_minutes": round(elapsed/60, 2),
        "success_rate": f"{round((success/sent)*100, 1)}%" if sent > 0 else "0%"
    }
    
    if player_info:
        response["player"] = {
            "uid": player_info.get("uid", uid),
            "nickname": player_info.get("nickname", ""),
            "level": player_info.get("level", 0),
            "likes": player_info.get("likes", 0),
            "region": player_info.get("region", "")
        }
    else:
        response["note"] = "Player info unavailable - tokens may be invalid"
    
    print(f"✅ Response: {json.dumps(response, indent=2)}")
    sys.stdout.flush()
    
    return jsonify(response), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5100))
    print(f"🚀 Server running on port {port}")
    print(f"📌 Try: http://localhost:{port}/visit/bd/14502384617")
    sys.stdout.flush()
    app.run(host="0.0.0.0", port=port, threaded=True, debug=True)
