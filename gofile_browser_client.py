import atexit
import importlib
import json
import os
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

DEFAULT_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)
DEFAULT_ACCEPT_LANGUAGE = "en-US,en;q=0.9"
STEALTH_INIT_SCRIPT = """
(() => {
  const define = (obj, key, value) => {
    try {
      Object.defineProperty(obj, key, {
        configurable: true,
        enumerable: true,
        get: () => value,
      });
    } catch (_err) {}
  };

  define(navigator, 'webdriver', undefined);
  define(navigator, 'platform', 'Win32');
  define(navigator, 'language', 'en-US');
  define(navigator, 'languages', ['en-US', 'en']);
  define(navigator, 'hardwareConcurrency', 8);
  define(navigator, 'deviceMemory', 8);

  if (!window.chrome) {
    Object.defineProperty(window, 'chrome', {
      configurable: true,
      enumerable: true,
      value: { runtime: {} },
    });
  } else if (!window.chrome.runtime) {
    Object.defineProperty(window.chrome, 'runtime', {
      configurable: true,
      enumerable: true,
      value: {},
    });
  }

  if (navigator.permissions && navigator.permissions.query) {
    const originalQuery = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = (parameters) => {
      if (parameters && parameters.name === 'notifications') {
        return Promise.resolve({ state: Notification.permission });
      }
      return originalQuery(parameters);
    };
  }
})();
"""


def _read_browser_user_agent() -> str:
    value = os.environ.get("GOFILE_BROWSER_USER_AGENT")
    if value and value.strip():
        return value.strip()
    return DEFAULT_BROWSER_USER_AGENT


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


class _PlaywrightDriverAdapter:
    def __init__(self, sync_playwright: Any, profile_dir: str, proxy_server: Optional[str]) -> None:
        self._runner = sync_playwright()
        self._playwright = self._runner.start()

        launch_kwargs: Dict[str, Any] = {
            "user_data_dir": profile_dir,
            "headless": True,
            "user_agent": _read_browser_user_agent(),
            "locale": "en-US",
            "timezone_id": "UTC",
            "color_scheme": "light",
            "device_scale_factor": 1,
            "java_script_enabled": True,
            "viewport": {"width": 1366, "height": 768},
            "extra_http_headers": {
                "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
            },
            "args": [
                "--headless=new",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--lang=en-US,en",
                "--window-size=1366,768",
            ],
        }
        if proxy_server:
            launch_kwargs["proxy"] = {"server": proxy_server}

        self._context = self._playwright.chromium.launch_persistent_context(**launch_kwargs)
        self._context.add_init_script(STEALTH_INIT_SCRIPT)
        pages = getattr(self._context, "pages", [])
        self._page = pages[0] if pages else self._context.new_page()

    def get(self, url: str) -> None:
        self._page.goto(url)

    def execute_script(self, script: str) -> Any:
        return self._page.evaluate(f"() => {{{script}}}")

    def get_cookies(self):
        return self._context.cookies("https://gofile.io/")

    def execute_async_script(self, _script: str, *args: Any) -> str:
        if len(args) < 4:
            raise RuntimeError("Missing fetch arguments for browser request")

        url = args[0]
        method = args[1]
        headers = args[2]
        timeout_ms = args[3]
        credentials_mode = args[4] if len(args) > 4 else "include"

        return self._page.evaluate(
            """
            async ({url, method, headers, timeoutMs, credentialsMode}) => {
                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), timeoutMs);

                try {
                    const resp = await fetch(url, {
                        method: method,
                        headers: headers || {},
                        credentials: credentialsMode || "include",
                        signal: controller.signal,
                    });
                    const text = await resp.text();
                    return JSON.stringify({ ok: resp.ok, status: resp.status, text: text });
                } catch (err) {
                    return JSON.stringify({ error: String(err) });
                } finally {
                    clearTimeout(timer);
                }
            }
            """,
            {
                "url": url,
                "method": method,
                "headers": headers,
                "timeoutMs": timeout_ms,
                "credentialsMode": credentials_mode,
            },
        )

    def quit(self) -> None:
        self._context.close()
        self._playwright.stop()


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

    def _create_browser_driver(self) -> Any:
        try:
            playwright_module = importlib.import_module("playwright.sync_api")
        except ImportError as exc:
            raise RuntimeError(
                "BrowserMetaTransport requires playwright "
                "(install dependencies with `pip install -r requirements.txt` and run `playwright install chromium`)"
            ) from exc

        sync_playwright = getattr(playwright_module, "sync_playwright", None)
        if sync_playwright is None:
            raise RuntimeError("playwright.sync_api is missing sync_playwright")

        return _PlaywrightDriverAdapter(
            sync_playwright=sync_playwright,
            profile_dir=self.profile_dir,
            proxy_server=_read_proxy_from_env(),
        )

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
