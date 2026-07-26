"""
PCR 客户端模块
负责与公主连结游戏服务器的网络通信，包括加密、解密和请求处理
"""
import asyncio
import hashlib
import os
import random
from Crypto.Cipher import AES
import base64
import msgpack
import uuid
from dateutil.parser import parse
from datetime import datetime
from re import search
from json import loads
from pathlib import Path
import aiohttp


class PCRClientError(RuntimeError):
    """Base error for game API communication."""


class PCRTransportError(PCRClientError):
    """The request failed at the HTTP or network layer."""


class PCRProtocolError(PCRClientError):
    """The game server returned a response that could not be decoded."""


class PCRServerError(PCRClientError):
    """The game server returned a structured business error."""

    def __init__(self, endpoint: str, error):
        self.endpoint = endpoint
        self.error = error
        if isinstance(error, dict):
            status = error.get("status", "unknown")
            message = error.get("message") or error.get("title") or "unknown server error"
        else:
            status = "unknown"
            message = str(error) or "unknown server error"
        super().__init__(f"{endpoint} returned server error status={status}: {message}")


# 版本配置
_version_file = Path(__file__).parent.parent.parent.parent / 'version.txt'
_version = os.getenv("PCR_APP_VERSION", "").strip() or "11.4.0"
if _version_file.exists() and not os.getenv("PCR_APP_VERSION", "").strip():
    _version = _version_file.read_text(encoding='utf-8').strip()


# AES 加密初始向量
_IV = b'7Fk9Lm3Np8Qr4Sv2'


def _padding(data: bytes) -> bytes:
    """PKCS7 填充"""
    pad_len = 16 - len(data) % 16
    return data + bytes([pad_len] * pad_len)


def _unpack(data: bytes):
    """解包 msgpack 数据"""
    try:
        return msgpack.unpackb(data)
    except msgpack.ExtraData as err:
        return err.unpacked


def decrypt(encrypted: bytes) -> dict:
    """解密服务器响应"""
    try:
        data = base64.b64decode(encrypted)
        key = data[-32:]
        data = data[:-32]
        
        cryptor = AES.new(key, AES.MODE_CBC, _IV)
        plain = cryptor.decrypt(data)
        
        result = msgpack.unpackb(plain[:-plain[-1]], strict_map_key=False)
        if isinstance(result, dict):
            return result
        raise PCRProtocolError(f"unexpected decrypted response type: {type(result).__name__}")
    except msgpack.ExtraData as err:
        if isinstance(err.unpacked, dict):
            return err.unpacked
        raise PCRProtocolError(f"unexpected unpacked response type: {type(err.unpacked).__name__}") from err
    except PCRProtocolError:
        raise
    except Exception as e:
        raise PCRProtocolError(f"failed to decrypt game response: {e}") from e


def encrypt(text: str, key: bytes) -> bytes:
    """加密字符串（带 base64）"""
    cryptor = AES.new(key, AES.MODE_CBC, _IV)
    padded = _padding(text.encode())
    encrypted = cryptor.encrypt(padded)
    return base64.b64encode(encrypted + key)


def encrypt_request(data: dict, key: bytes) -> bytes:
    """加密请求数据（不带 base64）"""
    cryptor = AES.new(key, AES.MODE_CBC, _IV)
    packed = msgpack.packb(data)
    padded = _padding(packed)
    encrypted = cryptor.encrypt(padded)
    return encrypted


def create_key() -> bytes:
    """生成随机请求密钥"""
    return base64.b16encode(uuid.uuid1().bytes).lower()


def pack_request(request: dict, key: bytes) -> bytes:
    """打包请求"""
    encrypted = encrypt_request(request, key)
    return encrypted + key


class PCRClient:
    """公主连结游戏客户端"""
    
    # 服务器地址
    URL_ROOT = "https://l3-prod-uo-gs-gzlj.bilibiligame.net/"
    OFFICIAL_CONFIG_URL = "https://static.biligame.com/config/pcr.config.js"
    
    def __init__(self, viewer_id: int):
        self.viewer_id = viewer_id
        self.request_id = ""
        self.session_id = ""
        self.manifest = None
        
        self.headers = {
            "EXCEL-VER": "1.0.0",
            "SHORT-UDID": "1001341751",
            "BATTLE-LOGIC-VERSION": "4",
            "IP-ADDRESS": "10.0.2.15",
            "DEVICE-ID": "febf37270db0254b8d1f76af92f0419f",
            "DEVICE-NAME": "Google PIXEL 2 XL",
            "GRAPHICS-DEVICE-NAME": "Adreno (TM) 540",
            "APP-VER": _version,
            "RES-KEY": "d145b29050641dac2f8b19df0afe0e59",
            "RES-VER": "10002200",
            "KEYCHAIN": "",
            "CHANNEL-ID": "4",
            "PLATFORM-ID": "4",
            "REGION-CODE": "",
            "PLATFORM": "2",
            "PLATFORM-OS-VERSION": "Android OS 7.1.2 / API-25 (NOF26V/4565141)",
            "LOCALE": "Jpn",
            "X-Unity-Version": "2018.4.30f1",
            "BUNDLE_VER": "",
            "DEVICE": "2",
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 7.1.2; PIXEL 2 XL Build/NOF26V)",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "close"
        }
    
    async def call_api(self, endpoint: str, request: dict, encrypted: bool = True) -> dict:
        """调用游戏 API"""
        global _version
        
        key = create_key()
        
        if encrypted:
            request['viewer_id'] = encrypt(str(self.viewer_id), key).decode()
        else:
            request['viewer_id'] = str(self.viewer_id)
        
        data = pack_request(request, key)
        
        headers = self.headers.copy()
        if self.request_id:
            headers["REQUEST-ID"] = self.request_id
        if self.session_id:
            headers["SID"] = self.session_id
        
        timeout = aiohttp.ClientTimeout(
            total=float(os.getenv("PCR_API_TIMEOUT_TOTAL", "60")),
            connect=float(os.getenv("PCR_API_TIMEOUT_CONNECT", "10")),
            sock_read=float(os.getenv("PCR_API_TIMEOUT_READ", "45")),
        )
        url = self.URL_ROOT + endpoint.lstrip("/")
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, data=data, headers=headers) as response:
                    resp_data = await response.read()
                    if not 200 <= response.status < 300:
                        raise PCRTransportError(f"HTTP {response.status} from {endpoint}")
        except PCRClientError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise PCRTransportError(f"request failed for {endpoint}: {exc}") from exc
        
        if encrypted:
            result = decrypt(resp_data)
        else:
            result = loads(resp_data.decode())
        
        if not isinstance(result, dict):
            raise PCRProtocolError(f"invalid response type from {endpoint}: {type(result).__name__}")
        if "data_headers" not in result or "data" not in result:
            raise PCRProtocolError(f"response from {endpoint} is missing data headers or data")
        
        ret_header = result.get("data_headers", {})
        if not isinstance(ret_header, dict):
            ret_header = {}
        
        # 更新版本
        if endpoint == "check/game_start" and "store_url" in ret_header:
            new_version = ret_header["store_url"].split('_')[1][:-4]
            if new_version != _version:
                _version = new_version
                self.headers['APP-VER'] = _version
                _version_file.write_text(_version, encoding='utf-8')
        
        # 更新会话信息
        if ret_header.get("sid"):
            self.session_id = hashlib.md5((ret_header["sid"] + "c!SID!n").encode()).hexdigest()
        
        if ret_header.get("request_id") and ret_header["request_id"] != self.request_id:
            self.request_id = ret_header["request_id"]
        
        if ret_header.get("viewer_id") and ret_header["viewer_id"] != self.viewer_id:
            self.viewer_id = int(ret_header["viewer_id"])
        
        data = result.get("data", {})
        if not isinstance(data, dict):
            raise PCRProtocolError(f"response data from {endpoint} is {type(data).__name__}")
        return data

    async def _refresh_app_version(self) -> str:
        timeout = aiohttp.ClientTimeout(total=15, connect=10, sock_read=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.OFFICIAL_CONFIG_URL) as response:
                    if not 200 <= response.status < 300:
                        raise PCRTransportError(
                            f"HTTP {response.status} while checking the official app version"
                        )
                    config = await response.text()
        except PCRClientError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise PCRTransportError(f"failed to check the official app version: {exc}") from exc

        match = search(r'"latest_version"\s*:\s*"(\d+\.\d+\.\d+)"', config)
        if not match:
            raise PCRProtocolError("official app config does not contain latest_version")

        version = match.group(1)
        global _version
        _version = version
        self.headers["APP-VER"] = version
        return version
    
    async def login(self, uid: str, access_key: str) -> tuple:
        """登录游戏"""
        # 检查维护状态
        version_refreshed = False
        while True:
            self.manifest = await self.call_api('source_ini/get_maintenance_status', {}, False)
            if 'maintenance_message' in self.manifest:
                try:
                    match = search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',
                                   self.manifest['maintenance_message']).group()
                    end = parse(match)
                    print(f'服务器维护中，预计结束时间: {match}')
                    while datetime.now() < end:
                        await asyncio.sleep(60)
                except Exception:
                    print('服务器维护中，等待 60 秒后重试...')
                    await asyncio.sleep(60)
                continue

            if 'required_manifest_ver' in self.manifest:
                break

            if 'server_error' in self.manifest and not version_refreshed:
                await self._refresh_app_version()
                version_refreshed = True
                continue

            raise PCRProtocolError("maintenance response is missing required_manifest_ver")
        
        # 设置 manifest 版本
        self.headers["MANIFEST-VER"] = self.manifest["required_manifest_ver"]
        
        # 登录流程
        await self.call_api('tool/sdk_login', {
            "uid": uid, 
            "access_key": access_key, 
            "platform": self.headers["PLATFORM-ID"], 
            "channel_id": self.headers["CHANNEL-ID"]
        })
        
        await self.call_api('check/game_start', {
            "app_type": 0, 
            "campaign_data": "", 
            "campaign_user": random.randint(1, 1000000)
        })
        
        load = await self.call_api("load/index", {"carrier": "google"})
        home = await self.call_api("home/index", {
            'message_id': random.randint(1, 5000), 
            'tips_id_list': [], 
            'is_first': 1, 
            'gold_history': 0
        })
        
        # 处理登录错误
        if 'server_error' in home:
            await self.call_api('tool/sdk_login', {
                "uid": uid, 
                "access_key": access_key, 
                "platform": self.headers["PLATFORM-ID"], 
                "channel_id": self.headers["CHANNEL-ID"]
            })
            await self.call_api('check/game_start', {
                "app_type": 0, 
                "campaign_data": "", 
                "campaign_user": random.randint(1, 1000000)
            })
        
        return load, home


class ApiException(Exception):
    """API 异常"""
    def __init__(self, message: str, code: int):
        super().__init__(message)
        self.code = code
