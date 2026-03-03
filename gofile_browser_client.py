import atexit
import importlib
import json
import os
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode


def _read_proxy_from_env() -> Optional[str]:
    for env_name in (
        "GOFILE_BROWSER_PROXY",
        "GOFILE_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        value = os.environ.get(env_name)
        if value and value.strip():
            return value.strip()
    return None


class BrowserMetaTransport:
    def __init__(self, profile_dir: str) -> None:
        self.profile_dir = profile_dir
        self._driver: Any = None
        self._lock = threading.RLock()
        self._origin_ready = False
        self._session_bootstrapped = False

    def _ensure_driver(self):
        if self._driver is not None:
            return

        os.makedirs(self.profile_dir, exist_ok=True)
        self._driver = self._create_browser_driver()
        self._origin_ready = False
        self._session_bootstrapped = False
        try:
            self._bootstrap_gofile_session_state()
        except Exception:
            self.close()
            raise
        atexit.register(self.close)

    def _bootstrap_gofile_session_state(self) -> None:
        if self._driver is None:
            raise RuntimeError("Browser driver is not initialized")

        self._driver.get("https://gofile.io/")
        self._origin_ready = True

        storage_payload = self._driver.execute_script(
            """
            const readStorage = (storage) => {
                const values = {};
                for (let i = 0; i < storage.length; i++) {
                    const key = storage.key(i);
                    values[key] = storage.getItem(key);
                }
                return values;
            };

            return {
                localStorage: readStorage(window.localStorage),
                sessionStorage: readStorage(window.sessionStorage),
            };
            """
        )

        cookies = self._driver.get_cookies()
        if not isinstance(cookies, list):
            cookies = []

        if not isinstance(storage_payload, dict):
            storage_payload = {}

        local_storage = storage_payload.get("localStorage")
        if not isinstance(local_storage, dict):
            local_storage = {}

        session_storage = storage_payload.get("sessionStorage")
        if not isinstance(session_storage, dict):
            session_storage = {}

        snapshot = {
            "origin": "https://gofile.io/",
            "savedAt": int(time.time()),
            "cookies": cookies,
            "localStorage": local_storage,
            "sessionStorage": session_storage,
        }
        self._persist_session_state(snapshot)
        self._session_bootstrapped = True

    def _persist_session_state(self, snapshot: Dict[str, Any]) -> None:
        state_path = os.path.join(self.profile_dir, "gofile-session-state.json")
        temp_path = f"{state_path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as state_file:
            json.dump(snapshot, state_file, indent=2, sort_keys=True)
        os.replace(temp_path, state_path)

    def _configure_browser_options(self, options: Any) -> None:
        options.add_argument("--headless=new")
        options.add_argument(f"--user-data-dir={self.profile_dir}")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        proxy_server = _read_proxy_from_env()
        if proxy_server:
            options.add_argument(f"--proxy-server={proxy_server}")

    def _create_browser_driver(self) -> Any:
        uc_error: Optional[Exception] = None

        try:
            uc = importlib.import_module("undetected_chromedriver")
        except ImportError:
            uc = None

        if uc is not None:
            options = uc.ChromeOptions()
            self._configure_browser_options(options)
            try:
                return uc.Chrome(options=options)
            except Exception as exc:
                uc_error = exc

        try:
            webdriver_module = importlib.import_module("selenium.webdriver")
            options_module = importlib.import_module("selenium.webdriver.chrome.options")
        except ImportError as exc:
            if uc_error is not None:
                raise RuntimeError(
                    "undetected_chromedriver failed and selenium is unavailable "
                    "(install dependencies with `pip install -r requirements.txt`)"
                ) from uc_error
            raise RuntimeError(
                "BrowserMetaTransport requires undetected_chromedriver or selenium "
                "(install dependencies with `pip install -r requirements.txt`)"
            ) from exc

        options = options_module.Options()
        self._configure_browser_options(options)
        try:
            return webdriver_module.Chrome(options=options)
        except Exception as exc:
            if uc_error is not None:
                raise RuntimeError(
                    "Failed to initialize browser driver via both undetected_chromedriver "
                    "and selenium"
                ) from exc
            raise

    def _build_url(self, url: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return url
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}{urlencode(params, doseq=True)}"

    def close(self) -> None:
        with self._lock:
            if self._driver is None:
                self._origin_ready = False
                self._session_bootstrapped = False
                return

            self._driver.quit()
            self._driver = None
            self._origin_ready = False
            self._session_bootstrapped = False

    def _ensure_gofile_origin(self) -> None:
        if self._origin_ready:
            return

        if self._driver is None:
            raise RuntimeError("Browser driver is not initialized")

        self._driver.get("https://gofile.io/")
        self._origin_ready = True

    def request_text(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 10,
        credentials: str = "include",
    ) -> str:
        request_url = self._build_url(url, params)
        with self._lock:
            self._ensure_driver()
            if not self._session_bootstrapped:
                raise RuntimeError("Browser session bootstrap did not complete")
            self._ensure_gofile_origin()
            if self._driver is None:
                raise RuntimeError("Browser driver is not initialized")

            payload = self._driver.execute_async_script(
                """
                const [url, method, headers, timeoutMs, credentialsMode, done] = arguments;
                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), timeoutMs);

                fetch(url, {
                    method: method,
                    headers: headers || {},
                    credentials: credentialsMode || 'include',
                    signal: controller.signal
                })
                    .then(async (resp) => {
                        const text = await resp.text();
                        done(JSON.stringify({ ok: resp.ok, status: resp.status, text: text }));
                    })
                    .catch((err) => {
                        done(JSON.stringify({ error: String(err) }));
                    })
                    .finally(() => {
                        clearTimeout(timer);
                    });
                """,
                request_url,
                method.upper(),
                headers or {},
                int(timeout * 1000),
                credentials,
            )

        result = json.loads(payload)
        if "error" in result:
            raise RuntimeError(result["error"])
        return result["text"]

    def request_json(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 10,
        credentials: str = "include",
    ) -> Dict[str, Any]:
        return json.loads(
            self.request_text(
                method,
                url,
                headers=headers,
                params=params,
                timeout=timeout,
                credentials=credentials,
            )
        )


_CLIENT: Optional[BrowserMetaTransport] = None
_LOCK = threading.Lock()


def _get_base_runtime_dir() -> str:
    config_dir = os.environ.get("CONFIG_DIR")
    if config_dir:
        return config_dir
    return os.path.join(os.path.expanduser("~"), ".cache", "gofile-dl")


def get_browser_meta_transport() -> BrowserMetaTransport:
    global _CLIENT
    with _LOCK:
        if _CLIENT is None:
            base_dir = _get_base_runtime_dir()
            profile_dir = os.path.join(base_dir, ".gofile-chrome-profile")
            _CLIENT = BrowserMetaTransport(profile_dir=profile_dir)
        return _CLIENT


def _reset_browser_meta_transport_for_tests() -> None:
    global _CLIENT
    with _LOCK:
        if _CLIENT is not None:
            _CLIENT.close()
        _CLIENT = None
