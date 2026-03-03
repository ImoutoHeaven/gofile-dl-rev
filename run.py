import argparse
import base64
import binascii
import logging
import os
import sys
from pathvalidate import sanitize_filename
import requests
from curl_cffi import requests as curl_requests
import hashlib
import time
import re
import json
from typing import Dict, Any, Optional, Callable, Set, List, Tuple

try:
    from typing import Protocol
except ImportError:  # Python 3.7
    from typing_extensions import Protocol

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(funcName)20s()][%(levelname)-8s]: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("GoFile")

DEFAULT_TIMEOUT = 10  # 10 seconds
GOFILE_URL_PATTERN = re.compile(r"^https?://gofile\.io/d/([A-Za-z0-9]+)(?:/)?$")
ACCOUNT_TOKEN_PATTERN = re.compile(r"data\.token\s*[:=]\s*['\"]?([^'\"\s,}]+)")
AUTH_RETRY_EXACT_STATUSES = {
    "error-notauthenticated",
    "error-invalidtoken",
    "error-badtoken",
    "error-invalidaccounttoken",
    "error-invalidwebsitetoken",
    "error-authrequired",
}
AUTH_RETRY_SKIP_STATUSES = {
    "error-notpremium",
    "error-ratelimit",
}
DEFAULT_TOKEN_CACHE_TTL = 12 * 60 * 60  # 12 hours
DEFAULT_WT_CACHE_TTL = 60 * 60  # 1 hour
DEFAULT_CONTENTS_PAGE_SIZE = 1000
NOT_PREMIUM_STATUS = "error-notpremium"
PAYLOAD_BUNDLE_PROMPT_SENTINEL = "__GOFILE_PAYLOAD_BUNDLE_PROMPT__"


class _MetaTransportProtocol(Protocol):
    def request_json(
        self,
        method: str,
        url: str,
        headers=None,
        params=None,
        timeout=10,
        credentials: str = "include",
    ):
        raise NotImplementedError

    def request_text(
        self,
        method: str,
        url: str,
        headers=None,
        params=None,
        timeout=10,
        credentials: str = "include",
    ):
        raise NotImplementedError


def build_meta_transport() -> _MetaTransportProtocol:
    from gofile_browser_client import get_browser_meta_transport

    return get_browser_meta_transport()


def get_runtime_config_dir() -> str:
    """Return writable config directory for cache/tracker data."""
    config_dir = os.environ.get("CONFIG_DIR")
    if config_dir:
        return config_dir
    return os.path.join(os.path.expanduser("~"), ".cache", "gofile-dl")


def read_proxy_from_env() -> Optional[str]:
    """Resolve proxy URL from GoFile-specific or standard proxy environment variables."""
    for env_name in (
        "GOFILE_DOWNLOAD_PROXY",
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


def normalize_gofile_url(raw_url: str) -> Optional[str]:
    """Validate and normalize a GoFile URL to canonical form."""
    cleaned = raw_url.strip()
    if not cleaned:
        return None

    match = GOFILE_URL_PATTERN.fullmatch(cleaned)
    if not match:
        return None

    content_id = match.group(1)
    return f"https://gofile.io/d/{content_id}"


def extract_account_token(raw_token: str) -> Optional[str]:
    """Extract account token from plain text, data.token assignment, or JSON payload."""
    cleaned = raw_token.strip()
    if not cleaned:
        return None

    match = ACCOUNT_TOKEN_PATTERN.search(cleaned)
    if match:
        return match.group(1)

    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return cleaned

    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            token = data.get("token")
            if isinstance(token, str) and token.strip():
                return token.strip()

    return cleaned


def parse_api_error_details(payload: Any) -> Tuple[str, str]:
    """Extract API status and readable details from a GoFile response payload."""
    if not isinstance(payload, dict):
        return "error-unknown", str(payload)

    status = str(payload.get("status", "error-unknown"))
    details: List[str] = []

    for key in ("message", "error"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            details.append(value.strip())

    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("message", "reason", "error", "details"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                details.append(value.strip())
        if "retryAfter" in data:
            details.append(f"retryAfter={data.get('retryAfter')}")
    elif data not in (None, ""):
        details.append(f"data={data}")

    if not details:
        details.append("no additional details")

    return status, "; ".join(details)


def should_refresh_auth(status: str) -> bool:
    """Decide whether an API error likely needs credential refresh."""
    normalized = status.strip().lower()
    if not normalized:
        return False
    if normalized in AUTH_RETRY_SKIP_STATUSES:
        return False
    if normalized in AUTH_RETRY_EXACT_STATUSES:
        return True
    return "token" in normalized or "auth" in normalized


def filter_gofile_urls(raw_lines: List[str]) -> Tuple[List[str], List[str]]:
    """Trim, validate, and split URL lines into valid and invalid lists."""
    valid_urls: List[str] = []
    invalid_lines: List[str] = []

    for raw_line in raw_lines:
        cleaned = raw_line.strip()
        if not cleaned:
            continue

        normalized = normalize_gofile_url(cleaned)
        if normalized is None:
            invalid_lines.append(cleaned)
            continue

        valid_urls.append(normalized)

    return valid_urls, invalid_lines


def collect_batch_urls(input_fn: Callable[[str], str] = input) -> List[str]:
    """
    Collect URL lines from stdin and stop on two consecutive blank lines.

    A single blank line is ignored to let users separate URL groups visually.
    """
    collected: List[str] = []
    blank_streak = 0

    while True:
        try:
            line = input_fn("")
        except EOFError:
            break

        cleaned = line.strip()
        if not cleaned:
            blank_streak += 1
            if blank_streak >= 2:
                break
            continue

        blank_streak = 0
        collected.append(cleaned)

    return collected


def collect_multiline_block(input_fn: Callable[[str], str] = input) -> str:
    """Collect a multiline text block from stdin, ending on two blank lines."""
    lines: List[str] = []
    blank_streak = 0

    while True:
        try:
            line = input_fn("")
        except EOFError:
            break

        cleaned = line.strip()
        if not cleaned:
            blank_streak += 1
            if blank_streak >= 2:
                break
            lines.append("")
            continue

        blank_streak = 0
        lines.append(line)

    return "\n".join(lines).strip()


def _decode_payload_bundle_text(raw_bundle: str) -> str:
    """Decode raw payload-bundle input as JSON text or base64/base64url JSON text."""
    cleaned = raw_bundle.strip()
    if not cleaned:
        raise ValueError("Payload bundle is empty")

    if (
        (cleaned.startswith("\"") and cleaned.endswith("\""))
        or (cleaned.startswith("'") and cleaned.endswith("'"))
    ):
        cleaned = cleaned[1:-1].strip()

    if cleaned.startswith("{"):
        return cleaned

    compact = "".join(cleaned.split())
    if not compact:
        raise ValueError("Payload bundle is empty")

    candidate_values: List[str] = [compact]
    normalized = compact.replace("-", "+").replace("_", "/")
    if normalized != compact:
        candidate_values.append(normalized)

    sanitized = re.sub(r"[^A-Za-z0-9+/=_-]", "", compact)
    if sanitized and sanitized not in candidate_values:
        candidate_values.append(sanitized)
        sanitized_normalized = sanitized.replace("-", "+").replace("_", "/")
        if sanitized_normalized not in candidate_values:
            candidate_values.append(sanitized_normalized)

    decode_errors: List[str] = []
    for candidate in candidate_values:
        padded_candidate = candidate
        padding = (-len(padded_candidate)) % 4
        if padding:
            padded_candidate += "=" * padding

        try:
            decoded_bytes = base64.b64decode(padded_candidate, validate=True)
        except (binascii.Error, ValueError) as e:
            decode_errors.append(str(e))
            continue

        try:
            return decoded_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            decode_errors.append(str(e))
            continue

    if decode_errors:
        detail = decode_errors[-1]
        raise ValueError(
            "Payload bundle must be JSON text (starting with '{') or a valid base64 string "
            f"(last decode error: {detail})"
        )

    raise ValueError("Payload bundle must be JSON text (starting with '{') or a valid base64 string")


def _extract_payloads_from_bundle(bundle_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract payload object list from parsed bundle object."""
    payloads_candidate: Any = None

    if isinstance(bundle_payload.get("payloads"), list):
        payloads_candidate = bundle_payload.get("payloads")
    elif isinstance(bundle_payload.get("payload"), dict):
        payloads_candidate = [bundle_payload.get("payload")]
    elif isinstance(bundle_payload.get("payloadJsonl"), str):
        payloads_candidate = _decode_payload_stream(bundle_payload["payloadJsonl"])
    elif isinstance(bundle_payload.get("jsonl"), str):
        payloads_candidate = _decode_payload_stream(bundle_payload["jsonl"])

    if not isinstance(payloads_candidate, list) or not payloads_candidate:
        raise ValueError(
            "Payload bundle must include non-empty 'payloads' list, 'payload' object, or 'payloadJsonl' text"
        )

    payloads: List[Dict[str, Any]] = []
    for index, item in enumerate(payloads_candidate, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Payload bundle item {index} must be a JSON object")
        payloads.append(item)

    return payloads


def parse_payload_bundle(raw_bundle: str) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """Parse payload-bundle text and return (account_token, payload_objects)."""
    bundle_text = _decode_payload_bundle_text(raw_bundle)

    try:
        bundle_payload = json.loads(bundle_text)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Payload bundle JSON is invalid (line {e.lineno}, column {e.colno}): {e.msg}"
        ) from e

    if not isinstance(bundle_payload, dict):
        raise ValueError("Payload bundle root must be a JSON object")

    account_token: Optional[str] = None
    account_token_raw = bundle_payload.get("accountToken")
    if not isinstance(account_token_raw, str):
        account_token_raw = bundle_payload.get("account_token")
    if isinstance(account_token_raw, str) and account_token_raw.strip():
        account_token = extract_account_token(account_token_raw)

    payloads = _extract_payloads_from_bundle(bundle_payload)
    return account_token, payloads


def _read_payload_source(payload_source: str) -> str:
    """Read payload text from file path or stdin ('-')."""
    try:
        if payload_source == "-":
            raw_payload = sys.stdin.read()
        else:
            with open(payload_source, "r", encoding="utf-8") as payload_fp:
                raw_payload = payload_fp.read()
    except OSError as e:
        raise ValueError(f"Cannot read content payload source '{payload_source}': {e}") from e

    if not raw_payload.strip():
        raise ValueError("Content payload source is empty")

    return raw_payload


def read_payload_bundle_input(
    payload_bundle_arg: str,
    input_fn: Callable[[str], str] = input,
) -> str:
    """Resolve payload-bundle arg as direct text, file path, stdin, or interactive paste block."""
    if payload_bundle_arg != PAYLOAD_BUNDLE_PROMPT_SENTINEL:
        candidate = payload_bundle_arg.strip()
        if not candidate:
            raise ValueError("Payload bundle input is empty")

        if candidate == "-":
            raw_stdin_bundle = sys.stdin.read()
            if not raw_stdin_bundle.strip():
                raise ValueError("Payload bundle input from stdin is empty")
            return raw_stdin_bundle

        expanded_path = os.path.expanduser(candidate)
        if os.path.isfile(expanded_path):
            try:
                with open(expanded_path, "r", encoding="utf-8") as bundle_fp:
                    raw_file_bundle = bundle_fp.read()
            except OSError as e:
                raise ValueError(f"Cannot read payload bundle file '{candidate}': {e}") from e

            if not raw_file_bundle.strip():
                raise ValueError(f"Payload bundle file '{candidate}' is empty")

            return raw_file_bundle

        return payload_bundle_arg

    logger.info("Payload-bundle mode: paste JSON/base64 bundle, then press Enter twice to finish")
    raw_bundle = collect_multiline_block(input_fn=input_fn)
    if not raw_bundle.strip():
        raise ValueError("Payload bundle input is empty")

    return raw_bundle


def _decode_payload_stream(raw_payload: str) -> List[Any]:
    """Decode multiple JSON values separated by whitespace."""
    decoder = json.JSONDecoder()
    payload_values: List[Any] = []
    offset = 0
    payload_length = len(raw_payload)

    while offset < payload_length:
        while offset < payload_length and raw_payload[offset].isspace():
            offset += 1

        if offset >= payload_length:
            break

        try:
            value, offset = decoder.raw_decode(raw_payload, idx=offset)
        except json.JSONDecodeError as e:
            raise ValueError(
                "Content payload is not valid JSON payload stream "
                f"(line {e.lineno}, column {e.colno}): {e.msg}"
            ) from e

        payload_values.append(value)

    return payload_values


def load_content_payloads(payload_source: str) -> List[Dict[str, Any]]:
    """Load one or more content payload objects from JSON/JSON array/JSONL."""
    raw_payload = _read_payload_source(payload_source)

    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        payload_values = _decode_payload_stream(raw_payload)
        if not payload_values:
            raise ValueError("Content payload source is empty")

        payloads: List[Dict[str, Any]] = []
        for index, item in enumerate(payload_values, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"Content payload stream item {index} must be a JSON object")
            payloads.append(item)

        return payloads

    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        if not payload:
            raise ValueError("Content payload array is empty")
        for index, item in enumerate(payload, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"Content payload array item {index} must be a JSON object")
        return payload

    raise ValueError(
        "Content payload must be a JSON object, JSON object array, "
        "or whitespace-delimited JSON objects"
    )


def load_content_payload(payload_source: str) -> Dict[str, Any]:
    """Load exactly one GoFile content API payload from file path or stdin ('-')."""
    payloads = load_content_payloads(payload_source)
    if len(payloads) != 1:
        raise ValueError(f"Expected exactly one payload, got {len(payloads)}")

    payload = payloads[0]

    if not isinstance(payload, dict):
        raise ValueError("Content payload must be a JSON object")

    return payload


def _normalize_payload_name(name: Any, fallback: str, strip_emojis: bool) -> str:
    """Sanitize payload-provided folder/file names with fallback value."""
    normalized = str(name).strip() if name is not None else ""
    if strip_emojis and normalized:
        normalized = strip_emojis_func(normalized)
    if not normalized:
        normalized = fallback
    return normalized


def _walk_payload_node(
    node: Dict[str, Any],
    current_dir: str,
    jobs: List[Tuple[str, str]],
    strip_emojis: bool,
    node_id: str = "",
) -> None:
    """Recursively walk payload node and collect downloadable file links."""
    node_type = str(node.get("type", "file")).lower()

    if node_type == "folder":
        fallback_folder_name = f"folder_{node_id[:8]}" if node_id else "folder"
        folder_name = _normalize_payload_name(
            node.get("name"), fallback_folder_name, strip_emojis
        )
        folder_path = os.path.join(current_dir, sanitize_filename(folder_name))

        children = node.get("children")
        if not isinstance(children, dict) or not children:
            children = node.get("contents")

        if isinstance(children, dict):
            for child_id, child in children.items():
                if not isinstance(child, dict):
                    continue
                _walk_payload_node(
                    node=child,
                    current_dir=folder_path,
                    jobs=jobs,
                    strip_emojis=strip_emojis,
                    node_id=str(child_id),
                )
            return

        logger.warning(
            "Payload folder '%s' has no embedded children; skipping nested traversal",
            folder_name,
        )
        return

    link = node.get("link")
    if not isinstance(link, str) or not link.strip():
        logger.warning("Skipping payload file without direct link")
        return

    fallback_file_name = f"file_{node_id[:8]}" if node_id else "file"
    file_name = _normalize_payload_name(node.get("name"), fallback_file_name, strip_emojis)

    relative_path = node.get("relativePath")
    if isinstance(relative_path, str) and relative_path.strip():
        path_parts: List[str] = []
        for raw_part in relative_path.replace("\\", "/").split("/"):
            part = raw_part.strip()
            if not part or part in (".", ".."):
                continue
            safe_part = sanitize_filename(part)
            if safe_part:
                path_parts.append(safe_part)
        if path_parts:
            file_path = os.path.join(current_dir, *path_parts)
        else:
            file_path = os.path.join(current_dir, sanitize_filename(file_name))
    else:
        file_path = os.path.join(current_dir, sanitize_filename(file_name))

    jobs.append((link, file_path))


def _parse_optional_int(value: Any) -> Optional[int]:
    """Parse optional integer metadata values like payload `size`."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        parsed = int(value)
        return parsed if parsed >= 0 else None
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.isdigit():
            parsed = int(cleaned)
            return parsed if parsed >= 0 else None
    return None


def _walk_payload_items(
    node: Dict[str, Any],
    current_dir: str,
    items: List[Dict[str, Any]],
    strip_emojis: bool,
    node_id: str = "",
) -> None:
    """Recursively walk payload nodes and keep file metadata for download decisions."""
    node_type = str(node.get("type", "file")).lower()

    if node_type == "folder":
        fallback_folder_name = f"folder_{node_id[:8]}" if node_id else "folder"
        folder_name = _normalize_payload_name(
            node.get("name"), fallback_folder_name, strip_emojis
        )
        folder_path = os.path.join(current_dir, sanitize_filename(folder_name))

        children = node.get("children")
        if not isinstance(children, dict) or not children:
            children = node.get("contents")

        if isinstance(children, dict):
            for child_id, child in children.items():
                if not isinstance(child, dict):
                    continue
                _walk_payload_items(
                    node=child,
                    current_dir=folder_path,
                    items=items,
                    strip_emojis=strip_emojis,
                    node_id=str(child_id),
                )
            return

        logger.warning(
            "Payload folder '%s' has no embedded children; skipping nested traversal",
            folder_name,
        )
        return

    link = node.get("link")
    if not isinstance(link, str) or not link.strip():
        logger.warning("Skipping payload file without direct link")
        return

    fallback_file_name = f"file_{node_id[:8]}" if node_id else "file"
    file_name = _normalize_payload_name(node.get("name"), fallback_file_name, strip_emojis)

    relative_path = node.get("relativePath")
    if isinstance(relative_path, str) and relative_path.strip():
        path_parts: List[str] = []
        for raw_part in relative_path.replace("\\", "/").split("/"):
            part = raw_part.strip()
            if not part or part in (".", ".."):
                continue
            safe_part = sanitize_filename(part)
            if safe_part:
                path_parts.append(safe_part)
        if path_parts:
            file_path = os.path.join(current_dir, *path_parts)
        else:
            file_path = os.path.join(current_dir, sanitize_filename(file_name))
    else:
        file_path = os.path.join(current_dir, sanitize_filename(file_name))

    item: Dict[str, Any] = {
        "link": link,
        "file_path": file_path,
    }

    expected_size = _parse_optional_int(node.get("size"))
    if expected_size is not None:
        item["size"] = expected_size

    md5_value = node.get("md5")
    if isinstance(md5_value, str) and md5_value.strip():
        item["md5"] = md5_value.strip().lower()

    items.append(item)


def collect_download_items_from_payload(
    payload: Dict[str, Any],
    base_dir: str,
    strip_emojis: bool = False,
) -> List[Dict[str, Any]]:
    """Convert payload into download items with file metadata."""
    root_node: Any = payload
    if "status" in payload:
        status, details = parse_api_error_details(payload)
        if status != "ok":
            raise ValueError(f"Payload status is '{status}': {details}")
        root_node = payload.get("data")

    if not isinstance(root_node, dict):
        raise ValueError("Payload missing root content object in 'data'")

    items: List[Dict[str, Any]] = []
    _walk_payload_items(
        node=root_node,
        current_dir=base_dir,
        items=items,
        strip_emojis=strip_emojis,
    )
    return items


def collect_download_jobs_from_payload(
    payload: Dict[str, Any],
    base_dir: str,
    strip_emojis: bool = False,
) -> List[Tuple[str, str]]:
    """
    Convert a raw GoFile content payload into downloadable (link, path) jobs.

    Accepts either the full API response shape (`{"status": "ok", "data": {...}}`)
    or a direct content node object (`{"type": "folder"|"file", ...}`).
    """
    items = collect_download_items_from_payload(
        payload=payload,
        base_dir=base_dir,
        strip_emojis=strip_emojis,
    )
    return [(item["link"], item["file_path"]) for item in items]


def write_failed_files_report(failed_files: List[Dict[str, Any]], out_dir: str) -> Optional[str]:
    """Persist failed download entries as a payload-retry JSON file."""
    if not failed_files:
        return None

    os.makedirs(out_dir, exist_ok=True)
    report_path = os.path.join(out_dir, "failed_files.json")
    try:
        with open(report_path, "w", encoding="utf-8") as report_fp:
            json.dump(failed_files, report_fp, indent=2)
    except OSError as e:
        logger.error(f"Could not write failed files report: {e}")
        return None

    logger.warning(
        "%s file(s) failed. Retry payload written to %s",
        len(failed_files),
        report_path,
    )
    return report_path


def failed_files_report_path(out_dir: str) -> str:
    """Return canonical failed-files report path for an output directory."""
    return os.path.join(out_dir, "failed_files.json")


def clear_failed_files_report(out_dir: str) -> None:
    """Remove stale failed-files report after all retries succeed."""
    report_path = failed_files_report_path(out_dir)
    if os.path.exists(report_path):
        try:
            os.remove(report_path)
        except OSError as e:
            logger.warning(f"Could not remove stale failed files report: {e}")


def parse_total_retries(raw_value: str) -> Optional[int]:
    """Parse --total-retries as positive integer or 'inf'."""
    normalized = raw_value.strip().lower()
    if normalized == "inf":
        return None

    try:
        parsed = int(normalized)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            "--total-retries must be a positive integer or 'inf'"
        ) from e

    if parsed < 1:
        raise argparse.ArgumentTypeError("--total-retries must be >= 1")

    return parsed


def _run_payload_batch(
    gofile_client: Any,
    payloads: List[Dict[str, Any]],
    out_dir: str,
) -> None:
    """Execute a payload batch using either execute_payload or download fallback."""
    logger.info(f"Loaded {len(payloads)} payload object(s)")
    for payload_index, payload in enumerate(payloads, start=1):
        logger.info(f"Starting payload {payload_index}/{len(payloads)}")
        if hasattr(gofile_client, "execute_payload"):
            gofile_client.execute_payload(
                dir=out_dir,
                payload=payload,
                progress_callback=lambda p: logger.info(f"Overall progress: {p}%"),
                overall_progress_callback=lambda p, eta: logger.info(
                    f"Overall progress: {p}% | ETA: {eta}"
                ),
                file_progress_callback=lambda f, p, size=None, **kwargs: logger.info(
                    f"File {f} progress: {p}%"
                ),
            )
        else:
            items = collect_download_items_from_payload(payload, base_dir=out_dir)
            for item in items:
                link = item["link"]
                file_path = item["file_path"]
                expected_size = item.get("size")
                expected_md5 = item.get("md5")

                if is_payload_file_already_downloaded(
                    file_path,
                    expected_size=expected_size,
                    expected_md5=expected_md5,
                ):
                    logger.info(f"Skipping already downloaded payload file: {file_path}")
                    continue

                downloaded = gofile_client.download(link, file_path)
                if downloaded is False and hasattr(gofile_client, "failed_files"):
                    failed_entry: Dict[str, Any] = {
                        "type": "file",
                        "name": os.path.basename(file_path),
                        "link": link,
                        "relativePath": os.path.relpath(file_path, out_dir).replace(os.sep, "/"),
                        "error": "download failed after retries",
                    }
                    if expected_size is not None:
                        failed_entry["size"] = expected_size
                    if expected_md5:
                        failed_entry["md5"] = expected_md5
                    gofile_client.failed_files.append(failed_entry)
                    if hasattr(gofile_client, "failed_report_dir"):
                        write_failed_files_report(gofile_client.failed_files, out_dir)


def _run_url_batch(
    gofile_client: Any,
    urls: List[str],
    out_dir: str,
    password: Optional[str],
) -> None:
    """Execute a URL batch through the standard /contents mode."""
    for index, url in enumerate(urls, start=1):
        logger.info(f"Starting download {index}/{len(urls)}: {url}")
        gofile_client.execute(
            dir=out_dir,
            url=url,
            password=password,
            progress_callback=lambda p: logger.info(f"Overall progress: {p}%"),
            overall_progress_callback=lambda p, eta: logger.info(
                f"Overall progress: {p}% | ETA: {eta}"
            ),
            name_callback=lambda name: logger.info(f"Task name set to: {name}"),
            file_progress_callback=lambda f, p, size=None, **kwargs: logger.info(
                f"File {f} progress: {p}%"
            ),
        )


def compute_file_md5(file_path: str, chunk_size: int = 1024 * 1024) -> Optional[str]:
    """Compute file md5 checksum for payload-based integrity checks."""
    try:
        md5_hasher = hashlib.md5()
        with open(file_path, "rb") as file_obj:
            while True:
                chunk = file_obj.read(chunk_size)
                if not chunk:
                    break
                md5_hasher.update(chunk)
        return md5_hasher.hexdigest()
    except OSError:
        return None


def is_payload_file_already_downloaded(
    file_path: str,
    expected_size: Optional[int] = None,
    expected_md5: Optional[str] = None,
) -> bool:
    """Check whether an existing local file matches payload metadata and can be skipped."""
    if not os.path.isfile(file_path):
        return False

    if expected_size is not None:
        try:
            if os.path.getsize(file_path) != expected_size:
                return False
        except OSError:
            return False

    if expected_md5:
        local_md5 = compute_file_md5(file_path)
        if not local_md5:
            return False
        return local_md5.lower() == expected_md5.lower()

    return True

def strip_emojis_func(text: str) -> str:
    """
    Remove emojis and other problematic Unicode characters from text.
    
    Args:
        text: Input string potentially containing emojis
        
    Returns:
        String with emojis removed
    """
    # Emoji pattern - covers most emoji ranges
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002500-\U00002BEF"  # chinese char
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002600-\U000026FF"  # Miscellaneous Symbols
        "\U00002700-\U000027BF"  # Dingbats
        "]+",
        flags=re.UNICODE
    )
    result = emoji_pattern.sub('', text)
    # Clean up any double spaces or trailing/leading spaces
    result = ' '.join(result.split())
    return result.strip()

def normalize_folder_name(name: str, custom_patterns: Optional[str] = None) -> str:
    """
    Normalize folder name by removing common prefixes like 'NEW FILES in'.
    This helps match folders that get renamed after completion.
    
    Args:
        name: Original folder name
        custom_patterns: Optional pipe-separated list of patterns to strip (e.g., '⭐NEW FILES in |NEW FILES in |⭐')
        
    Returns:
        Normalized folder name
    """
    # Default patterns
    patterns = [
        r'^⭐\s*NEW FILES in\s+',
        r'^NEW FILES in\s+',
        r'^⭐\s*',
        r'^\*+\s*NEW FILES in\s+',
        r'^\*+\s*',
    ]
    
    # Add custom patterns if provided
    if custom_patterns:
        custom_list = [p.strip() for p in custom_patterns.split('|') if p.strip()]
        # Convert custom patterns to regex patterns (escape special chars except spaces)
        for pattern in custom_list:
            # Escape regex special characters but preserve the pattern intent
            escaped = re.escape(pattern)
            # Replace escaped spaces with flexible whitespace matcher
            escaped = escaped.replace('\\ ', '\\s*')
            # Add anchors and trailing whitespace matcher
            regex_pattern = f'^{escaped}\\s*'
            patterns.insert(0, regex_pattern)  # Insert at beginning for priority
    
    result = name
    for pattern in patterns:
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)
    
    return result.strip()

class DownloadTracker:
    """
    Tracks downloaded files to enable incremental/sync downloads.
    """
    
    def __init__(self, base_dir: str, content_id: str, folder_pattern: Optional[str] = None):
        """
        Initialize download tracker.
        
        Args:
            base_dir: Base directory for downloads
            content_id: GoFile content ID being tracked
            folder_pattern: Custom patterns to strip from folder names (pipe-separated)
        """
        self.base_dir = base_dir
        self.content_id = content_id
        self.folder_pattern = folder_pattern
        # Store tracking files in /config directory for persistence
        config_dir = os.environ.get('CONFIG_DIR', '/config')
        os.makedirs(config_dir, exist_ok=True)
        self.tracking_file = os.path.join(config_dir, f".gofile_tracker_{content_id}.json")
        self.downloaded_files: Set[str] = set()
        self.load_tracking_data()
    
    def load_tracking_data(self) -> None:
        """Load previously downloaded file list from tracking file."""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r') as f:
                    data = json.load(f)
                    self.downloaded_files = set(data.get('files', []))
                    logger.info(f"Loaded tracking data: {len(self.downloaded_files)} previously downloaded files")
            except Exception as e:
                logger.warning(f"Could not load tracking data: {e}")
                self.downloaded_files = set()
    
    def save_tracking_data(self) -> None:
        """Save downloaded file list to tracking file."""
        try:
            os.makedirs(os.path.dirname(self.tracking_file), exist_ok=True)
            with open(self.tracking_file, 'w') as f:
                json.dump({
                    'content_id': self.content_id,
                    'last_updated': time.time(),
                    'files': list(self.downloaded_files)
                }, f, indent=2)
            logger.debug(f"Saved tracking data: {len(self.downloaded_files)} files")
        except Exception as e:
            logger.warning(f"Could not save tracking data: {e}")
    
    def is_downloaded(self, file_id: str, file_name: str) -> bool:
        """
        Check if a file has already been downloaded.
        
        Args:
            file_id: GoFile file ID
            file_name: File name
            
        Returns:
            True if file was previously downloaded
        """
        key = f"{file_id}:{file_name}"
        return key in self.downloaded_files
    
    def mark_downloaded(self, file_id: str, file_name: str) -> None:
        """
        Mark a file as downloaded.
        
        Args:
            file_id: GoFile file ID
            file_name: File name
        """
        key = f"{file_id}:{file_name}"
        self.downloaded_files.add(key)
        self.save_tracking_data()
    
    def find_existing_folder(self, folder_name: str, parent_dir: str) -> Optional[str]:
        """
        Find an existing folder that matches the given name, handling renames.
        
        Args:
            folder_name: Current folder name
            parent_dir: Parent directory to search in
            
        Returns:
            Path to existing folder or None
        """
        if not os.path.exists(parent_dir):
            return None
        
        # Normalize the target folder name with custom pattern
        normalized_target = normalize_folder_name(folder_name, self.folder_pattern)
        
        # Check for exact match first
        exact_path = os.path.join(parent_dir, sanitize_filename(folder_name))
        if os.path.isdir(exact_path):
            return exact_path
        
        # Look for similar folders (normalized match)
        for item in os.listdir(parent_dir):
            item_path = os.path.join(parent_dir, item)
            if os.path.isdir(item_path):
                normalized_item = normalize_folder_name(item, self.folder_pattern)
                if normalized_item == normalized_target:
                    logger.info(f"Found renamed folder: '{item}' matches '{folder_name}'")
                    return item_path
        
        return None

class GoFileMeta(type):
    """
    Metaclass for implementing the Singleton pattern.
    
    Ensures only one instance of GoFile is created.
    """
    _instances: Dict[type, Any] = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class GoFile(metaclass=GoFileMeta):
    """
    GoFile API client for downloading files and folders.
    
    Provides methods to authenticate with the GoFile API and download content.
    Uses a Singleton pattern to ensure only one instance is created.
    """
    
    def __init__(self) -> None:
        """Initialize the GoFile client and credential cache settings."""
        self.token: str = ""
        self.wt: str = ""
        self.meta_transport = build_meta_transport()
        self._download_session: Optional[Any] = None
        self.failed_files: List[Dict[str, Any]] = []
        self.failed_report_dir: Optional[str] = None
        self.token_cache_ttl = self._read_ttl_env("GOFILE_TOKEN_CACHE_TTL", DEFAULT_TOKEN_CACHE_TTL)
        self.wt_cache_ttl = self._read_ttl_env("GOFILE_WT_CACHE_TTL", DEFAULT_WT_CACHE_TTL)
        self.cache_file = os.path.join(get_runtime_config_dir(), ".gofile_api_cache.json")
        self._cache_loaded = False

    def _get_download_session(self) -> Any:
        """Reuse one curl_cffi session with browser impersonation for all file transfers."""
        if self._download_session is None:
            session_kwargs: Dict[str, Any] = {
                "impersonate": "chrome",
                "default_headers": True,
            }
            proxy_server = read_proxy_from_env()
            if proxy_server:
                session_kwargs["proxy"] = proxy_server
            self._download_session = curl_requests.Session(**session_kwargs)
        return self._download_session

    def clear_failed_files(self) -> None:
        """Reset in-memory failed download records for a new run."""
        self.failed_files = []

    def set_failed_report_dir(self, out_dir: str) -> None:
        """Configure output directory used for immediate failed_files flush."""
        self.failed_report_dir = out_dir

    def _record_failed_file(
        self,
        link: str,
        file_path: str,
        error: str,
        base_dir: str,
        expected_size: Optional[int] = None,
        expected_md5: Optional[str] = None,
    ) -> None:
        """Store failed file as payload-compatible entry for retry."""
        try:
            relative_path = os.path.relpath(file_path, base_dir)
        except ValueError:
            relative_path = os.path.basename(file_path)

        if relative_path.startswith(".."):
            relative_path = os.path.basename(file_path)

        entry: Dict[str, Any] = {
            "type": "file",
            "name": os.path.basename(file_path),
            "link": link,
            "relativePath": relative_path.replace(os.sep, "/"),
            "error": error,
        }
        if expected_size is not None:
            entry["size"] = expected_size
        if expected_md5:
            entry["md5"] = expected_md5.lower()
        self.failed_files.append(entry)

        if self.failed_report_dir:
            write_failed_files_report(self.failed_files, self.failed_report_dir)

    @staticmethod
    def _read_ttl_env(env_name: str, default_value: int) -> int:
        """Read cache TTL from environment with safe fallback."""
        raw_value = os.environ.get(env_name)
        if raw_value is None:
            return default_value
        try:
            ttl = int(raw_value)
        except ValueError:
            logger.warning(f"Invalid {env_name} value '{raw_value}', using default {default_value}")
            return default_value
        return max(ttl, 0)

    @staticmethod
    def _build_contents_params(password: Optional[str] = None) -> Dict[str, Any]:
        """Mirror GoFile web client query defaults for /contents requests."""
        params: Dict[str, Any] = {
            "contentFilter": "",
            "page": 1,
            "pageSize": DEFAULT_CONTENTS_PAGE_SIZE,
            "sortField": "name",
            "sortDirection": 1,
        }
        if password:
            params["password"] = hashlib.sha256(password.encode()).hexdigest()
        return params

    def _build_contents_headers(self, include_website_token: bool = True) -> Dict[str, str]:
        """Build /contents request headers with optional website token."""
        headers = {
            "Authorization": "Bearer " + self.token,
        }
        if include_website_token and self.wt:
            headers["X-Website-Token"] = self.wt
        return headers

    def _load_credential_cache(self) -> None:
        """Load cached account token and website token when still fresh."""
        if self._cache_loaded:
            return

        self._cache_loaded = True
        try:
            with open(self.cache_file, "r") as cache_fp:
                cache_data = json.load(cache_fp)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return

        now = time.time()
        token_data = cache_data.get("token", {})
        cached_token = token_data.get("value", "")
        token_updated_at = token_data.get("updated_at", 0)
        if cached_token and now - token_updated_at <= self.token_cache_ttl:
            self.token = cached_token

        wt_data = cache_data.get("wt", {})
        cached_wt = wt_data.get("value", "")
        wt_updated_at = wt_data.get("updated_at", 0)
        if cached_wt and now - wt_updated_at <= self.wt_cache_ttl:
            self.wt = cached_wt

    def _save_credential_cache(self, token_updated: bool = False, wt_updated: bool = False) -> None:
        """Persist token/wt cache with timestamp for TTL-based refresh."""
        if not token_updated and not wt_updated:
            return

        cache_data: Dict[str, Dict[str, Any]] = {}
        try:
            with open(self.cache_file, "r") as cache_fp:
                cache_data = json.load(cache_fp)
                if not isinstance(cache_data, dict):
                    cache_data = {}
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            cache_data = {}

        now = time.time()
        if token_updated and self.token:
            cache_data["token"] = {"value": self.token, "updated_at": now}
        if wt_updated and self.wt:
            cache_data["wt"] = {"value": self.wt, "updated_at": now}

        cache_dir = os.path.dirname(self.cache_file)
        try:
            os.makedirs(cache_dir, exist_ok=True)
            temp_path = self.cache_file + ".tmp"
            with open(temp_path, "w") as cache_fp:
                json.dump(cache_data, cache_fp, indent=2)
            os.replace(temp_path, self.cache_file)
        except OSError as cache_error:
            logger.warning(f"Could not persist credential cache: {cache_error}")
    
    def count_files(self, children: Dict[str, Dict]) -> int:
        """
        Count the total number of files in a folder structure.
        
        Note: This only counts files in the current level, not nested folders,
        since nested folder contents need separate API calls.
        
        Args:
            children: Dictionary of child items from GoFile API response
            
        Returns:
            int: Total number of files and folders found
        """
        count = 0
        for child in children.values():
            # Count each item (file or folder) as 1
            # We can't count nested folder contents without additional API calls
            count += 1
        return count
    
    def update_token(self, force_refresh: bool = False) -> None:
        """
        Update the access token used for API requests.
        
        Makes a request to GoFile's accounts API to get a fresh token.
        """
        if force_refresh:
            self.token = ""
        else:
            if self.token:
                return
            self._load_credential_cache()
            if self.token:
                return

        try:
            data = self.meta_transport.request_json(
                "POST",
                "https://api.gofile.io/accounts",
                timeout=DEFAULT_TIMEOUT,
                credentials="omit",
            )
        except Exception as e:
            logger.error(f"Cannot get token: {e}")
            return

        status, details = parse_api_error_details(data)
        if status == "ok":
            self.token = data["data"].get("token", "")
            if self.token:
                self._save_credential_cache(token_updated=True)
                logger.info("Updated token")
            else:
                logger.error("Token response did not contain token value")
        else:
            logger.error(f"Cannot get token [{status}]: {details}")
    
    def update_wt(self, force_refresh: bool = False) -> None:
        """
        Update the 'wt' (websiteToken) parameter needed for content requests.
        
        Extracts the wt parameter from GoFile's config.js JavaScript file.
        """
        if force_refresh:
            self.wt = ""
        else:
            if self.wt:
                return
            self._load_credential_cache()
            if self.wt:
                return

        try:
            alljs = self.meta_transport.request_text(
                "GET",
                "https://gofile.io/dist/js/config.js",
                timeout=DEFAULT_TIMEOUT,
            )
            if 'appdata.wt = "' in alljs:
                self.wt = alljs.split('appdata.wt = "')[1].split('"')[0]
                if self.wt:
                    self._save_credential_cache(wt_updated=True)
                    logger.info("Updated wt")
                else:
                    logger.error("wt extraction produced an empty value")
            else:
                logger.error("Cannot extract wt from config.js")
        except Exception as e:
            logger.error(f"Failed to get wt: {e}")
    
    def execute(self, 
                dir: str, 
                content_id: Optional[str] = None, 
                url: Optional[str] = None, 
                password: Optional[str] = None,
                progress_callback: Optional[Callable[[int], None]] = None, 
                cancel_event: Optional[Any] = None, 
                name_callback: Optional[Callable[[str], None]] = None,
                overall_progress_callback: Optional[Callable[[int, str], None]] = None, 
                start_time: Optional[float] = None,
                file_progress_callback: Optional[Callable[..., None]] = None,
                pause_callback: Optional[Callable[[], bool]] = None, 
                throttle_speed: Optional[int] = None,
                retry_attempts: int = 0,
                strip_emojis: bool = False,
                incremental: bool = False,
                tracker: Optional[DownloadTracker] = None,
                folder_pattern: Optional[str] = None,
                auth_retry: bool = True,
                failed_base_dir: Optional[str] = None) -> None:
        """
        Execute a download operation for a GoFile URL or content ID.
        
        This method handles both content IDs and URLs, authenticating as needed,
        and downloading either individual files or entire folder structures.
        
        Args:
            dir: Directory to save files to
            content_id: GoFile content ID
            url: GoFile URL (alternative to content_id)
            password: Optional password for protected content
            progress_callback: Callback for progress updates (0-100)
            cancel_event: Event to signal cancellation
            name_callback: Callback to update task name
            overall_progress_callback: Callback for overall progress updates (percent, ETA)
            start_time: Start time of the download
            file_progress_callback: Callback for file progress updates (filename, percent, size)
            pause_callback: Callback to check if download should pause
            throttle_speed: Download speed limit in KB/s
            retry_attempts: Number of retry attempts for failed downloads
            strip_emojis: Whether to strip emojis from folder/file names
            incremental: Enable incremental mode (skip already downloaded files)
            tracker: Download tracker instance (created automatically if None)
            folder_pattern: Custom patterns to strip from folder names (pipe-separated)
            auth_retry: Retry once after forcing token/wt refresh when API auth fails
            failed_base_dir: Root output directory used for failed-file retry paths
        """
        if failed_base_dir is None:
            failed_base_dir = dir

        if content_id is not None:
            self.update_token()
            self.update_wt()
            
            # Initialize tracker for incremental mode
            if incremental and tracker is None:
                tracker = DownloadTracker(dir, content_id, folder_pattern)
            
            request_params = self._build_contents_params(password=password)
            request_headers = self._build_contents_headers(include_website_token=True)
            
            try:
                data = self.meta_transport.request_json(
                    "GET",
                    f"https://api.gofile.io/contents/{content_id}",
                    headers=request_headers,
                    params=request_params,
                    timeout=DEFAULT_TIMEOUT,
                )
            except Exception as e:
                logger.error(f"Failed to fetch content {content_id}: {e}")
                return
            if data.get("status") != "ok":
                status, details = parse_api_error_details(data)

                if (
                    auth_retry
                    and status.strip().lower() == NOT_PREMIUM_STATUS
                    and "X-Website-Token" in request_headers
                ):
                    logger.warning(
                        "API error [error-notPremium], retrying once without X-Website-Token"
                    )
                    try:
                        data = self.meta_transport.request_json(
                            "GET",
                            f"https://api.gofile.io/contents/{content_id}",
                            headers=self._build_contents_headers(include_website_token=False),
                            params=request_params,
                            timeout=DEFAULT_TIMEOUT,
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to fetch content {content_id} without website token: {e}"
                        )
                        return
                    if data.get("status") == "ok":
                        status = "ok"
                    else:
                        status, details = parse_api_error_details(data)

                if data.get("status") != "ok":
                    if auth_retry and should_refresh_auth(status):
                        logger.warning(
                            f"API error [{status}], forcing credential refresh and retrying once: {details}"
                        )
                        self.update_token(force_refresh=True)
                        self.update_wt(force_refresh=True)
                        self.execute(
                            dir=dir,
                            content_id=content_id,
                            password=password,
                            progress_callback=progress_callback,
                            cancel_event=cancel_event,
                            name_callback=name_callback,
                            overall_progress_callback=overall_progress_callback,
                            start_time=start_time,
                            file_progress_callback=file_progress_callback,
                            pause_callback=pause_callback,
                            throttle_speed=throttle_speed,
                            retry_attempts=retry_attempts,
                            strip_emojis=strip_emojis,
                            incremental=incremental,
                            tracker=tracker,
                            folder_pattern=folder_pattern,
                            auth_retry=False,
                            failed_base_dir=failed_base_dir,
                        )
                        return

                    logger.error(f"API error [{status}]: {details}")
                    return
            if data["data"].get("passwordStatus", "passwordOk") != "passwordOk":
                logger.error("Invalid password: %s", data["data"].get("passwordStatus"))
                return
            if data["data"]["type"] == "folder":
                dirname = data["data"]["name"]
                
                # Strip emojis if requested
                if strip_emojis:
                    dirname_clean = strip_emojis_func(dirname)
                    if not dirname_clean:  # If name becomes empty after stripping
                        dirname = f"folder_{content_id[:8]}"
                    else:
                        dirname = dirname_clean
                
                if name_callback:
                    name_callback(sanitize_filename(dirname))
                
                # Check for existing folder (handles renames in incremental mode)
                if incremental and tracker:
                    existing_folder = tracker.find_existing_folder(dirname, dir)
                    if existing_folder:
                        logger.info(f"Using existing folder: {existing_folder}")
                        folder_path = existing_folder
                    else:
                        folder_path = os.path.join(dir, sanitize_filename(dirname))
                else:
                    folder_path = os.path.join(dir, sanitize_filename(dirname))
                
                # Create folder with better error handling
                try:
                    os.makedirs(folder_path, exist_ok=True)
                except PermissionError as e:
                    logger.error(f"Permission denied creating folder '{folder_path}': {e}")
                    logger.error(f"Check that the parent directory is writable. Current user UID: {os.getuid()}")
                    raise PermissionError(f"Cannot create folder '{folder_path}': Permission denied. Check Docker volume permissions.")
                except OSError as e:
                    logger.error(f"OS error creating folder '{folder_path}': {e}")
                    raise OSError(f"Cannot create folder '{folder_path}': {e}")
                
                # Get children - they might be in 'children' or 'contents' depending on API version
                children = data["data"].get("children", {})
                if not children:
                    children = data["data"].get("contents", {})
                
                if not children:
                    logger.warning(f"No children found in folder {content_id} ({dirname})")
                    # Don't return - empty folders are valid
                    if overall_progress_callback:
                        folder_name = data["data"].get("name", "folder")
                        overall_progress_callback(100, folder_name)
                    return
                
                # Calculate progress for THIS folder only (not recursive)
                overall_total = float(len(children))
                files_completed = 0.0
                
                for child_id, child in children.items():
                    if cancel_event and cancel_event.is_set():
                        break
                    
                    try:
                        child_type = child.get("type", "file")
                        
                        if child_type == "folder":
                            # Recursively process subfolder
                            logger.info(f"Processing subfolder: {child.get('name', child_id)}")
                            
                            # Recursively download subfolder contents
                            # Note: Progress tracking for subfolders is approximate since we need
                            # to make API calls to get their contents
                            self.execute(
                                dir=folder_path, 
                                content_id=child_id, 
                                password=password,
                                progress_callback=progress_callback, 
                                cancel_event=cancel_event,
                                name_callback=None,  # Don't update main task name for subfolders
                                overall_progress_callback=overall_progress_callback,
                                start_time=start_time, 
                                file_progress_callback=file_progress_callback,
                                pause_callback=pause_callback, 
                                throttle_speed=throttle_speed,
                                retry_attempts=retry_attempts,
                                strip_emojis=strip_emojis,
                                incremental=incremental,
                                tracker=tracker,
                                folder_pattern=folder_pattern,
                                auth_retry=auth_retry,
                                failed_base_dir=failed_base_dir,
                            )
                            files_completed += 1.0  # Count the folder as processed
                            
                        else:
                            # Download file
                            filename = child.get("name", "unknown")
                            
                            # Check if already downloaded in incremental mode
                            if incremental and tracker and tracker.is_downloaded(child_id, filename):
                                logger.info(f"Skipping already downloaded file: {filename}")
                                files_completed += 1.0
                                if callable(file_progress_callback):
                                    file_path = os.path.join(folder_path, sanitize_filename(filename))
                                    file_progress_callback(file_path, 100)
                                continue
                            
                            # Strip emojis if requested
                            if strip_emojis:
                                filename_clean = strip_emojis_func(filename)
                                if not filename_clean:  # If name becomes empty after stripping
                                    # Keep extension if present
                                    ext = os.path.splitext(filename)[1]
                                    filename = f"file_{child_id[:8]}{ext}"
                                else:
                                    filename = filename_clean
                            
                            file_path = os.path.join(folder_path, sanitize_filename(filename))
                            link = child.get("link", "")
                            child_size = _parse_optional_int(child.get("size"))
                            child_md5 = child.get("md5") if isinstance(child.get("md5"), str) else None
                              
                            if not link:
                                logger.error(f"No download link for file: {filename}")
                                self._record_failed_file(
                                    link="",
                                    file_path=file_path,
                                    error="missing direct download link",
                                    base_dir=failed_base_dir,
                                    expected_size=child_size,
                                    expected_md5=child_md5,
                                )
                                files_completed += 1.0
                                continue
                            
                            logger.info(f"Downloading file: {filename}")
                            
                            if callable(file_progress_callback):
                                file_progress_callback(file_path, 0)  # register start (0%)
                            
                            was_downloaded = self.download(
                                link, file_path, 
                                progress_callback=progress_callback,
                                cancel_event=cancel_event, 
                                file_progress_callback=file_progress_callback,
                                pause_callback=pause_callback, 
                                throttle_speed=throttle_speed,
                                retry_attempts=retry_attempts
                            )
                            
                            if was_downloaded:
                                # Mark as downloaded in incremental mode
                                if incremental and tracker:
                                    tracker.mark_downloaded(child_id, filename)

                                if callable(file_progress_callback):
                                    file_progress_callback(file_path, 100)  # file complete
                            else:
                                self._record_failed_file(
                                    link=link,
                                    file_path=file_path,
                                    error="download failed after retries",
                                    base_dir=failed_base_dir,
                                    expected_size=child_size,
                                    expected_md5=child_md5,
                                )
                             
                            files_completed += 1.0
                            
                    except Exception as e_inner:
                        logger.error(f"Error processing child {child_id}: {e_inner}")
                        files_completed += 1.0  # Count as processed even if failed
                        continue
                    
                    # Update progress after each child in this folder
                    if overall_progress_callback and overall_total > 0:
                        percent = int((files_completed / overall_total) * 100)
                        folder_name = data["data"].get("name", "folder")
                        overall_progress_callback(percent, folder_name)
                
                # Mark this folder as complete
                if overall_progress_callback:
                    folder_name = data["data"].get("name", "folder")
                    overall_progress_callback(100, folder_name)
            else:
                filename = data["data"]["name"]
                
                # Strip emojis if requested
                if strip_emojis:
                    filename_clean = strip_emojis_func(filename)
                    if not filename_clean:  # If name becomes empty after stripping
                        # Keep extension if present
                        ext = os.path.splitext(filename)[1]
                        filename = f"file_{content_id[:8]}{ext}"
                    else:
                        filename = filename_clean
                
                file_path = os.path.join(dir, sanitize_filename(filename))
                link = data["data"]["link"]
                file_size = _parse_optional_int(data["data"].get("size"))
                file_md5 = data["data"].get("md5") if isinstance(data["data"].get("md5"), str) else None
                if callable(name_callback):
                    name_callback(sanitize_filename(filename))
                if callable(file_progress_callback):
                    file_progress_callback(file_path, 0)
                was_downloaded = self.download(
                    link,
                    file_path,
                    progress_callback=progress_callback,
                    cancel_event=cancel_event,
                    file_progress_callback=file_progress_callback,
                    pause_callback=pause_callback,
                    throttle_speed=throttle_speed,
                    retry_attempts=retry_attempts,
                )
                if was_downloaded:
                    if callable(file_progress_callback):
                        file_progress_callback(file_path, 100)
                else:
                    self._record_failed_file(
                        link=link,
                        file_path=file_path,
                        error="download failed after retries",
                        base_dir=failed_base_dir,
                        expected_size=file_size,
                        expected_md5=file_md5,
                    )
        elif url is not None:
            normalized_url = normalize_gofile_url(url)
            if normalized_url is None:
                logger.error(f"Invalid URL: {url}")
                return

            cid = normalized_url.split("/")[-1]
            self.execute(
                dir=dir,
                content_id=cid,
                password=password,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
                name_callback=name_callback,
                overall_progress_callback=overall_progress_callback,
                start_time=start_time,
                file_progress_callback=file_progress_callback,
                pause_callback=pause_callback,
                throttle_speed=throttle_speed,
                retry_attempts=retry_attempts,
                strip_emojis=strip_emojis,
                incremental=incremental,
                tracker=tracker,
                folder_pattern=folder_pattern,
                auth_retry=auth_retry,
                failed_base_dir=failed_base_dir,
            )
        else:
            logger.error("Invalid parameters")

    def execute_payload(
        self,
        dir: str,
        payload: Dict[str, Any],
        progress_callback: Optional[Callable[[int], None]] = None,
        cancel_event: Optional[Any] = None,
        overall_progress_callback: Optional[Callable[[int, str], None]] = None,
        file_progress_callback: Optional[Callable[..., None]] = None,
        pause_callback: Optional[Callable[[], bool]] = None,
        throttle_speed: Optional[int] = None,
        retry_attempts: int = 0,
        strip_emojis: bool = False,
    ) -> None:
        """Download files directly from a pre-fetched content payload."""
        items = collect_download_items_from_payload(payload, base_dir=dir, strip_emojis=strip_emojis)
        if not items:
            logger.warning("No downloadable links found in provided payload")
            return

        total_jobs = len(items)
        for index, item in enumerate(items, start=1):
            if cancel_event and cancel_event.is_set():
                logger.info("Payload download cancelled")
                break

            link = item["link"]
            file_path = item["file_path"]
            expected_size = item.get("size")
            expected_md5 = item.get("md5")

            if is_payload_file_already_downloaded(
                file_path,
                expected_size=expected_size,
                expected_md5=expected_md5,
            ):
                logger.info(f"Skipping already downloaded payload file: {file_path}")
                if callable(file_progress_callback):
                    file_progress_callback(file_path, 100)
                if callable(overall_progress_callback):
                    overall_percent = int(index * 100 / total_jobs)
                    overall_progress_callback(overall_percent, "payload")
                continue

            logger.info(f"Downloading payload file {index}/{total_jobs}: {file_path}")
            if callable(file_progress_callback):
                file_progress_callback(file_path, 0)

            was_downloaded = self.download(
                link=link,
                file=file_path,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
                file_progress_callback=file_progress_callback,
                pause_callback=pause_callback,
                throttle_speed=throttle_speed,
                retry_attempts=retry_attempts,
            )

            if was_downloaded:
                if callable(file_progress_callback):
                    file_progress_callback(file_path, 100)
            else:
                self._record_failed_file(
                    link=link,
                    file_path=file_path,
                    error="download failed after retries",
                    base_dir=dir,
                    expected_size=expected_size,
                    expected_md5=expected_md5,
                )

            if callable(overall_progress_callback):
                overall_percent = int(index * 100 / total_jobs)
                overall_progress_callback(overall_percent, "payload")
    
    def download(self, 
                link: str, 
                file: str, 
                chunk_size: int = 8192, 
                progress_callback: Optional[Callable[[int], None]] = None,
                cancel_event: Optional[Any] = None, 
                file_progress_callback: Optional[Callable[..., None]] = None,
                pause_callback: Optional[Callable[[], bool]] = None, 
                throttle_speed: Optional[int] = None,
                retry_attempts: int = 0, 
                retry_delay: int = 5) -> bool:
        """
        Download a file from a GoFile link with various controls.
        
        Args:
            link: The file download link
            file: Path to save the file
            chunk_size: Size of download chunks in bytes
            progress_callback: Callback function to report download progress (0-100)
            cancel_event: Event to signal cancellation
            file_progress_callback: Callback to report file progress with size
            pause_callback: Callback to check if download should pause
            throttle_speed: Speed limit in KB/s (None for unlimited)
            retry_attempts: Number of retry attempts for failed downloads
            retry_delay: Seconds to wait between retry attempts
            
        Returns:
            bool: True when file is downloaded successfully, False otherwise
            
        Raises:
            Exception: If the download fails after all retry attempts
        """
        temp = file + ".part"
        attempts = 0
        bytes_since_last_check = 0
        last_check_time = time.time()
        
        while attempts <= retry_attempts:
            try:
                file_dir = os.path.dirname(file)
                os.makedirs(file_dir, exist_ok=True)
                size = os.path.getsize(temp) if os.path.exists(temp) else 0
                request_headers = {"Range": f"bytes={size}-"}
                request_kwargs: Dict[str, Any] = {
                    "headers": request_headers,
                    "stream": True,
                    "timeout": DEFAULT_TIMEOUT,
                }
                if self.token:
                    request_kwargs["cookies"] = {"accountToken": self.token}

                response = self._get_download_session().get(link, **request_kwargs)
                try:
                    response.raise_for_status()
                    total_size = int(response.headers.get("Content-Length", 0)) + size
                    downloaded = size
                    
                    # Register file with its size information
                    if file_progress_callback:
                        file_progress_callback(file, 0, size=total_size)  # Pass size here
                    
                    with open(temp, "ab") as f:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            # Check for pause - if paused, wait until unpaused
                            if pause_callback and pause_callback():
                                while pause_callback():
                                    time.sleep(0.5)  # Sleep for half a second before checking again
                        
                            f.write(chunk)
                            downloaded += len(chunk)
                            if progress_callback:
                                percentage = int(downloaded * 100 / total_size)
                                progress_callback(percentage)
                            if file_progress_callback:
                                file_progress_callback(file, int(downloaded * 100 / total_size), size=total_size)
                            if cancel_event and cancel_event.is_set():
                                logger.info("Download cancelled")
                                raise Exception("Cancelled")
                            
                            # Apply throttling if needed
                            if throttle_speed:
                                bytes_since_last_check += len(chunk)
                                current_time = time.time()
                                elapsed = current_time - last_check_time
                                
                                if elapsed > 0:  # Avoid division by zero
                                    current_rate = bytes_since_last_check / elapsed
                                    
                                    if current_rate > throttle_speed * 1024:
                                        # Need to sleep to maintain the desired rate
                                        sleep_time = (bytes_since_last_check / (throttle_speed * 1024)) - elapsed
                                        if sleep_time > 0:
                                            time.sleep(sleep_time)
                                        
                                        # Reset tracking after rate limiting
                                        bytes_since_last_check = 0
                                        last_check_time = time.time()
                    
                    # Rename temp file to final file when download is complete
                    os.rename(temp, file)
                    if file_progress_callback:
                        file_progress_callback(file, 100, size=total_size)
                    logger.info(f"Downloaded: {file} ({link})")
                    
                    # Download was successful, exit the retry loop
                    return True
                finally:
                    close_fn = getattr(response, "close", None)
                    if callable(close_fn):
                        close_fn()
                    
            except Exception as e:
                attempts += 1
                logger.warning(f"Download attempt {attempts} failed for {file}: {e}")
                
                if attempts <= retry_attempts:
                    logger.info(f"Retrying in {retry_delay} seconds... ({attempts}/{retry_attempts})")
                    if file_progress_callback:
                        file_progress_callback(file, -1, retry_info=f"Retry {attempts}/{retry_attempts}")  # -1 indicates retry state
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to download after {attempts} attempts: {file} ({link})")
                    if os.path.exists(temp):
                        os.remove(temp)
                    if file_progress_callback:
                        file_progress_callback(file, -2)  # -2 indicates permanent failure
                    break

        return False

def main(
    argv: Optional[List[str]] = None,
    input_fn: Callable[[str], str] = input,
    gofile_factory: Callable[[], Any] = GoFile,
) -> int:
    """
    Main function for CLI usage.
    
    Parses command line arguments and initiates download.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("url", nargs="?")
    parser.add_argument("-d", type=str, dest="dir", help="output directory")
    parser.add_argument("-p", type=str, dest="password", help="password")
    parser.add_argument(
        "--account-token",
        type=str,
        dest="account_token",
        help="reuse an existing account token; supports raw token or data.token=...",
    )
    payload_source_group = parser.add_mutually_exclusive_group()
    payload_source_group.add_argument(
        "--content-payload-file",
        type=str,
        dest="content_payload_file",
        help="download from a raw /contents API JSON payload file (use '-' for stdin)",
    )
    payload_source_group.add_argument(
        "-pb",
        "--payload-bundle",
        nargs="?",
        const=PAYLOAD_BUNDLE_PROMPT_SENTINEL,
        dest="payload_bundle",
        help=(
            "payload bundle text that includes accountToken + payloads (JSON or base64). "
            "Use '-pb' alone to paste bundle interactively and end with two blank lines"
        ),
    )
    parser.add_argument(
        "--refresh-auth",
        action="store_true",
        help="force refresh account token and website token before download",
    )
    parser.add_argument(
        "--total-retries",
        type=parse_total_retries,
        default=3,
        help="max retry rounds using failed_files payload (positive integer or 'inf')",
    )
    args = parser.parse_args(argv)

    out_dir = args.dir if args.dir is not None else "./output"

    initial_payload_source = args.content_payload_file
    initial_bundle_payloads: Optional[List[Dict[str, Any]]] = None
    bundle_account_token: Optional[str] = None

    if args.payload_bundle is not None:
        try:
            raw_bundle = read_payload_bundle_input(args.payload_bundle, input_fn=input_fn)
            bundle_account_token, initial_bundle_payloads = parse_payload_bundle(raw_bundle)
        except ValueError as bundle_error:
            logger.error(f"Invalid payload bundle: {bundle_error}")
            return 1

    gofile_client = gofile_factory()
    if hasattr(gofile_client, "set_failed_report_dir"):
        gofile_client.set_failed_report_dir(out_dir)
    elif hasattr(gofile_client, "failed_report_dir"):
        gofile_client.failed_report_dir = out_dir

    manual_account_token = None
    if args.account_token:
        manual_account_token = extract_account_token(args.account_token)
    elif bundle_account_token:
        manual_account_token = bundle_account_token

    if args.refresh_auth:
        if hasattr(gofile_client, "update_token") and manual_account_token is None:
            gofile_client.update_token(force_refresh=True)
        if hasattr(gofile_client, "update_wt"):
            gofile_client.update_wt(force_refresh=True)

    if manual_account_token is not None and hasattr(gofile_client, "token"):
        gofile_client.token = manual_account_token
        if hasattr(gofile_client, "_save_credential_cache"):
            gofile_client._save_credential_cache(token_updated=True)

    urls: List[str] = []
    if not initial_payload_source and initial_bundle_payloads is None:
        if args.url:
            raw_urls = [args.url]
        else:
            logger.info("Batch mode: enter one URL per line, then press Enter twice to start")
            raw_urls = collect_batch_urls(input_fn=input_fn)

        urls, invalid_lines = filter_gofile_urls(raw_urls)
        if invalid_lines:
            logger.warning(f"Skipped {len(invalid_lines)} invalid URL line(s)")

        if not urls:
            logger.error("No valid GoFile URLs provided")
            return 1

    retry_round = 0
    retry_limit = args.total_retries

    while True:
        if hasattr(gofile_client, "clear_failed_files"):
            gofile_client.clear_failed_files()
        elif hasattr(gofile_client, "failed_files"):
            gofile_client.failed_files = []

        try:
            if retry_round == 0:
                if initial_payload_source:
                    payloads = load_content_payloads(initial_payload_source)
                    _run_payload_batch(gofile_client, payloads, out_dir)
                elif initial_bundle_payloads is not None:
                    _run_payload_batch(gofile_client, initial_bundle_payloads, out_dir)
                else:
                    _run_url_batch(gofile_client, urls, out_dir, args.password)
            else:
                report_source = failed_files_report_path(out_dir)
                limit_label = "inf" if retry_limit is None else str(retry_limit)
                logger.info(
                    f"Retry round {retry_round}/{limit_label} using payload report: {report_source}"
                )
                retry_payloads = load_content_payloads(report_source)
                _run_payload_batch(gofile_client, retry_payloads, out_dir)
        except ValueError as payload_error:
            logger.error(f"Invalid content payload: {payload_error}")
            return 1

        failed_entries = getattr(gofile_client, "failed_files", [])
        if not isinstance(failed_entries, list):
            failed_entries = []

        if not failed_entries:
            clear_failed_files_report(out_dir)
            break

        report_path = write_failed_files_report(failed_entries, out_dir)
        if not report_path:
            logger.error("Cannot continue retry loop because failed_files report was not written")
            return 1

        if retry_limit is not None and retry_round >= retry_limit:
            logger.error(
                "Reached --total-retries limit with %s unresolved failed file(s)",
                len(failed_entries),
            )
            break

        retry_round += 1

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
