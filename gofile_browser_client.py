import atexit
import json
import os
import threading
from typing import Any, Dict, Optional
from urllib.parse import urlencode


class BrowserMetaTransport:
    def __init__(self, profile_dir: str) -> None:
        self.profile_dir = profile_dir
        self._driver: Any = None
        self._lock = threading.RLock()
        self._origin_ready = False

    def _ensure_driver(self):
        if self._driver is not None:
            return

        try:
            import undetected_chromedriver as uc
        except ImportError as exc:
            raise RuntimeError(
                "undetected_chromedriver is required for BrowserMetaTransport"
            ) from exc

        os.makedirs(self.profile_dir, exist_ok=True)

        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument(f"--user-data-dir={self.profile_dir}")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        self._driver = uc.Chrome(options=options)
        self._origin_ready = False
        atexit.register(self.close)

    def _build_url(self, url: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return url
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}{urlencode(params, doseq=True)}"

    def close(self) -> None:
        with self._lock:
            if self._driver is None:
                self._origin_ready = False
                return

            self._driver.quit()
            self._driver = None
            self._origin_ready = False

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
    ) -> str:
        request_url = self._build_url(url, params)
        with self._lock:
            self._ensure_driver()
            self._ensure_gofile_origin()
            if self._driver is None:
                raise RuntimeError("Browser driver is not initialized")

            payload = self._driver.execute_async_script(
                """
                const [url, method, headers, timeoutMs, done] = arguments;
                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), timeoutMs);

                fetch(url, {
                    method: method,
                    headers: headers || {},
                    credentials: 'include',
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
    ) -> Dict[str, Any]:
        return json.loads(
            self.request_text(method, url, headers=headers, params=params, timeout=timeout)
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
