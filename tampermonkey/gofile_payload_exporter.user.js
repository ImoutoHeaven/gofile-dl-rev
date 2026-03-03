// ==UserScript==
// @name         GoFile Payload Exporter (JSONL)
// @namespace    https://gofile.io/
// @version      0.4.0
// @description  Batch fetch GoFile /contents payloads from real browser session and export JSONL.
// @author       OpenCode
// @match        https://gofile.io/*
// @match        https://www.gofile.io/*
// @grant        GM_setClipboard
// @grant        GM_openInTab
// @grant        unsafeWindow
// @run-at       document-start
// ==/UserScript==

(function () {
  "use strict";

  const PANEL_ID = "gpx-panel";
  const BUTTON_ID = "gpx-toggle";
  const STYLE_ID = "gpx-style";
  const CAPTURE_CHANNEL_NAME = "gpx-capture-v1";
  const CAPTURE_EVENT_NAME = "gpx-capture-event";
  const CAPTURE_HASH_JOB = "gpx_capture_job";
  const CAPTURE_HASH_CID = "gpx_capture_cid";
  const CAPTURE_TIMEOUT_MS = 35000;

  const state = {
    running: false,
    outputJsonl: "",
    outputBundleBase64: "",
    failures: [],
  };

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function toNonNegativeInt(value, fallback) {
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return fallback;
    }
    return parsed;
  }

  function extractContentId(rawUrl) {
    const cleaned = String(rawUrl || "").trim();
    if (!cleaned) {
      return null;
    }

    try {
      const parsed = new URL(cleaned);
      if (!/(^|\.)gofile\.io$/i.test(parsed.hostname)) {
        return null;
      }
      const pathParts = parsed.pathname.split("/").filter(Boolean);
      if (pathParts.length < 2 || pathParts[0] !== "d") {
        return null;
      }
      const contentId = pathParts[1];
      if (!/^[A-Za-z0-9]+$/.test(contentId)) {
        return null;
      }
      return contentId;
    } catch (_error) {
      return null;
    }
  }

  function parseJsonSafe(text) {
    try {
      return JSON.parse(String(text || ""));
    } catch (_error) {
      return null;
    }
  }

  function generateJobId() {
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
  }

  function buildCaptureTabUrl(rawUrl, jobId, contentId) {
    const parsed = new URL(rawUrl);
    const hash = parsed.hash.startsWith("#") ? parsed.hash.slice(1) : parsed.hash;
    const hashParams = new URLSearchParams(hash);
    hashParams.set(CAPTURE_HASH_JOB, jobId);
    hashParams.set(CAPTURE_HASH_CID, contentId);
    parsed.hash = hashParams.toString();
    return parsed.toString();
  }

  function parseCaptureConfigFromHash() {
    const hash = location.hash.startsWith("#") ? location.hash.slice(1) : location.hash;
    if (!hash) {
      return null;
    }

    const hashParams = new URLSearchParams(hash);
    const jobId = (hashParams.get(CAPTURE_HASH_JOB) || "").trim();
    const contentId = (hashParams.get(CAPTURE_HASH_CID) || "").trim();
    if (!jobId || !/^[A-Za-z0-9_-]+$/.test(jobId)) {
      return null;
    }
    if (!contentId || !/^[A-Za-z0-9]+$/.test(contentId)) {
      return null;
    }

    return { jobId, contentId };
  }

  function extractContentIdFromApiUrl(rawUrl) {
    const normalized = String(rawUrl || "");
    const match = normalized.match(/\/contents\/([A-Za-z0-9-]+)/i);
    return match && match[1] ? match[1] : "";
  }

  function payloadMatchesTarget(payload, expectedContentId, capturedUrl) {
    const expected = String(expectedContentId || "").trim().toLowerCase();
    if (!expected) {
      return true;
    }

    const idFromUrl = extractContentIdFromApiUrl(capturedUrl).toLowerCase();
    if (idFromUrl && idFromUrl === expected) {
      return true;
    }

    if (payload && payload.data && typeof payload.data === "object") {
      const code = typeof payload.data.code === "string" ? payload.data.code.trim().toLowerCase() : "";
      if (code && code === expected) {
        return true;
      }

      const id = typeof payload.data.id === "string" ? payload.data.id.trim().toLowerCase() : "";
      if (id && id === expected) {
        return true;
      }
    }

    return false;
  }

  let captureChannel = null;
  let captureListenerAttached = false;
  const pendingCaptureResolvers = new Map();

  function getCaptureChannel() {
    if (typeof BroadcastChannel !== "function") {
      return null;
    }
    if (!captureChannel) {
      captureChannel = new BroadcastChannel(CAPTURE_CHANNEL_NAME);
    }
    return captureChannel;
  }

  function attachCaptureChannelListener() {
    const channel = getCaptureChannel();
    if (!channel || captureListenerAttached) {
      return;
    }

    channel.addEventListener("message", (event) => {
      const message = event && event.data ? event.data : null;
      if (!message || message.type !== "gpx-capture-result" || !message.jobId) {
        return;
      }

      const resolver = pendingCaptureResolvers.get(message.jobId);
      if (!resolver) {
        return;
      }

      pendingCaptureResolvers.delete(message.jobId);
      resolver(message);
    });

    captureListenerAttached = true;
  }

  function waitForCaptureResult(jobId, timeoutMs) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        pendingCaptureResolvers.delete(jobId);
        resolve({
          type: "gpx-capture-result",
          jobId,
          ok: false,
          error: `capture timeout after ${Math.round(timeoutMs / 1000)}s`,
        });
      }, timeoutMs);

      pendingCaptureResolvers.set(jobId, (message) => {
        clearTimeout(timer);
        resolve(message);
      });
    });
  }

  function openCaptureTab(url) {
    if (typeof GM_openInTab === "function") {
      try {
        const tabHandle = GM_openInTab(url, {
          active: false,
          insert: true,
          setParent: true,
        });

        return {
          blocked: false,
          close: () => {
            try {
              if (tabHandle && typeof tabHandle.close === "function") {
                tabHandle.close();
              }
            } catch (_error) {
              // Ignore close failures.
            }
          },
        };
      } catch (_error) {
        // Fall through to window.open.
      }
    }

    const openedWindow = window.open(url, "_blank", "noopener,noreferrer");
    return {
      blocked: !openedWindow,
      close: () => {
        try {
          if (openedWindow && !openedWindow.closed) {
            openedWindow.close();
          }
        } catch (_error) {
          // Ignore close failures.
        }
      },
    };
  }

  function injectPageCaptureBridge(_expectedContentId) {
    const root = typeof unsafeWindow !== "undefined" && unsafeWindow ? unsafeWindow : window;
    if (!root || root.__gpxCaptureBridgeInstalled__) {
      return;
    }

    root.__gpxCaptureBridgeInstalled__ = true;

    const emit = (detail) => {
      try {
        window.dispatchEvent(new CustomEvent(CAPTURE_EVENT_NAME, { detail }));
      } catch (_error) {
        // Ignore bridge dispatch errors.
      }
    };

    const normalizeUrl = (rawUrl) => {
      try {
        return new URL(String(rawUrl || ""), root.location && root.location.href ? root.location.href : location.href)
          .href;
      } catch (_error) {
        return "";
      }
    };

    const shouldCapture = (rawUrl) => {
      const normalized = normalizeUrl(rawUrl);
      return /https:\/\/api\.gofile\.io\/contents\//i.test(normalized);
    };

    const emitParsedPayload = (payload, source, method, url, statusCode) => {
      if (!payload || typeof payload !== "object") {
        return;
      }
      if (!Object.prototype.hasOwnProperty.call(payload, "status")) {
        return;
      }
      if (!payload.data || typeof payload.data !== "object") {
        return;
      }
      if (
        !Object.prototype.hasOwnProperty.call(payload.data, "children") &&
        !Object.prototype.hasOwnProperty.call(payload.data, "code") &&
        !Object.prototype.hasOwnProperty.call(payload.data, "id")
      ) {
        return;
      }

      try {
        emit({
          source,
          method,
          url,
          statusCode,
          bodyText: JSON.stringify(payload),
        });
      } catch (_error) {
        // Ignore JSON serialization errors.
      }
    };

    try {
      if (typeof root.fetch === "function" && !root.__gpxFetchWrapped) {
        const originalFetch = root.fetch;
        root.fetch = function (...args) {
          return originalFetch.apply(this, args).then((response) => {
            try {
              const requestInput = args[0];
              const requestInit = args[1] || {};
              const requestUrl =
                typeof requestInput === "string"
                  ? requestInput
                  : (requestInput && requestInput.url) || response.url || "";
              const requestMethod = String(
                requestInit.method || (requestInput && requestInput.method) || "GET"
              ).toUpperCase();

              if (!shouldCapture(requestUrl)) {
                return response;
              }

              const cloned = response.clone();
              cloned
                .text()
                .then((bodyText) => {
                  emit({
                    source: "fetch",
                    method: requestMethod,
                    url: normalizeUrl(requestUrl),
                    statusCode: response.status,
                    bodyText,
                  });
                })
                .catch(() => {
                  emit({
                    source: "fetch",
                    method: requestMethod,
                    url: normalizeUrl(requestUrl),
                    statusCode: response.status,
                    bodyText: "",
                  });
                });
            } catch (_error) {
              // Ignore bridge capture errors.
            }

            return response;
          });
        };
        root.__gpxFetchWrapped = true;
      }
    } catch (_error) {
      // Ignore fetch hook failures.
    }

    try {
      const xhrProto = root.XMLHttpRequest && root.XMLHttpRequest.prototype;
      if (xhrProto && typeof xhrProto.open === "function" && typeof xhrProto.send === "function" && !root.__gpxXhrWrapped) {
        const originalOpen = xhrProto.open;
        const originalSend = xhrProto.send;

        xhrProto.open = function (method, url, ...rest) {
          this.__gpxCaptureUrl = url;
          this.__gpxCaptureMethod = String(method || "GET").toUpperCase();
          return originalOpen.call(this, method, url, ...rest);
        };

        xhrProto.send = function (...args) {
          if (shouldCapture(this.__gpxCaptureUrl)) {
            this.addEventListener(
              "loadend",
              () => {
                emit({
                  source: "xhr",
                  method: this.__gpxCaptureMethod || "GET",
                  url: normalizeUrl(this.__gpxCaptureUrl),
                  statusCode: this.status,
                  bodyText: typeof this.responseText === "string" ? this.responseText : "",
                });
              },
              { once: true }
            );
          }

          return originalSend.apply(this, args);
        };

        root.__gpxXhrWrapped = true;
      }
    } catch (_error) {
      // Ignore xhr hook failures.
    }

    try {
      if (root.JSON && typeof root.JSON.parse === "function" && !root.__gpxJsonParseWrapped) {
        const originalJsonParse = root.JSON.parse;
        root.JSON.parse = function (...args) {
          const parsed = originalJsonParse.apply(this, args);
          try {
            emitParsedPayload(parsed, "json-parse", "UNKNOWN", root.location ? root.location.href : location.href, 0);
          } catch (_error) {
            // Ignore parsed payload capture errors.
          }
          return parsed;
        };
        root.__gpxJsonParseWrapped = true;
      }
    } catch (_error) {
      // Ignore JSON.parse hook failures.
    }
  }

  function startCaptureAgentIfNeeded() {
    const config = parseCaptureConfigFromHash();
    if (!config) {
      return false;
    }

    const channel = getCaptureChannel();
    if (!channel) {
      return false;
    }

    injectPageCaptureBridge(config.contentId);

    let finished = false;
    let lastError = "no /contents response captured";

    function finish(message) {
      if (finished) {
        return;
      }
      finished = true;

      channel.postMessage({
        type: "gpx-capture-result",
        jobId: config.jobId,
        contentId: config.contentId,
        pageUrl: location.href,
        ...message,
      });

      setTimeout(() => {
        try {
          window.close();
        } catch (_error) {
          // Ignore close failures.
        }
      }, 500);
    }

    window.addEventListener(CAPTURE_EVENT_NAME, (event) => {
      if (finished) {
        return;
      }

      const detail = event && event.detail ? event.detail : {};
      const method = String(detail.method || "GET").toUpperCase();
      if (method === "OPTIONS") {
        return;
      }
      if ((detail.statusCode === 204 || detail.statusCode === 0) && !String(detail.bodyText || "").trim()) {
        return;
      }

      const payload = parseJsonSafe(detail.bodyText);
      if (!payload || typeof payload !== "object") {
        lastError = `captured non-JSON /contents response (method=${method}, http=${detail.statusCode || "?"})`;
        return;
      }

      if (!payloadMatchesTarget(payload, config.contentId, detail.url || "")) {
        const capturedId = extractContentIdFromApiUrl(detail.url || "");
        if (capturedId) {
          lastError = `captured /contents for other id: ${capturedId}`;
        }
        return;
      }

      const status = String(payload.status || "error-unknown").toLowerCase();
      if (status === "ok") {
        finish({
          ok: true,
          payload,
          source: detail.source || "unknown",
          capturedUrl: detail.url || "",
        });
        return;
      }

      lastError = describeApiError(payload);
    });

    setTimeout(() => {
      finish({ ok: false, error: lastError });
    }, CAPTURE_TIMEOUT_MS);

    return true;
  }

  function describeApiError(payload) {
    if (!payload || typeof payload !== "object") {
      return String(payload);
    }
    const status = String(payload.status || "error-unknown");
    const details = [];
    if (typeof payload.message === "string" && payload.message.trim()) {
      details.push(payload.message.trim());
    }
    if (typeof payload.error === "string" && payload.error.trim()) {
      details.push(payload.error.trim());
    }
    if (payload.data && typeof payload.data === "object") {
      if (typeof payload.data.error === "string" && payload.data.error.trim()) {
        details.push(payload.data.error.trim());
      }
      if (typeof payload.data.errorMessage === "string" && payload.data.errorMessage.trim()) {
        details.push(payload.data.errorMessage.trim());
      }
    }
    if (!details.length) {
      return status;
    }
    return `${status}: ${details.join(" | ")}`;
  }

  function getCookieValue(name) {
    const escapedName = String(name || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = document.cookie.match(new RegExp(`(?:^|; )${escapedName}=([^;]*)`));
    return match ? decodeURIComponent(match[1]) : "";
  }

  function parseAccountTokenString(raw) {
    const cleaned = String(raw || "").trim();
    if (!cleaned) {
      return "";
    }

    const assignmentMatch = cleaned.match(/data\.token\s*[:=]\s*['\"]?([^'\"\s,}]+)/i);
    if (assignmentMatch && assignmentMatch[1]) {
      return assignmentMatch[1].trim();
    }

    const parsed = parseJsonSafe(cleaned);
    if (parsed && typeof parsed === "object") {
      if (typeof parsed.token === "string" && parsed.token.trim()) {
        return parsed.token.trim();
      }
      if (parsed.data && typeof parsed.data === "object") {
        if (typeof parsed.data.token === "string" && parsed.data.token.trim()) {
          return parsed.data.token.trim();
        }
      }
    }

    return cleaned;
  }

  function resolveAccountToken() {
    const fromCookie = parseAccountTokenString(getCookieValue("accountToken"));
    if (fromCookie) {
      return fromCookie;
    }

    const root = typeof unsafeWindow !== "undefined" && unsafeWindow ? unsafeWindow : window;
    if (root && root.appdata && typeof root.appdata === "object") {
      const fromAppData = parseAccountTokenString(root.appdata.accountToken || root.appdata.token || "");
      if (fromAppData) {
        return fromAppData;
      }
    }

    try {
      const candidates = [
        localStorage.getItem("accountToken"),
        localStorage.getItem("token"),
        sessionStorage.getItem("accountToken"),
        sessionStorage.getItem("token"),
      ];
      for (const candidate of candidates) {
        const parsed = parseAccountTokenString(candidate || "");
        if (parsed) {
          return parsed;
        }
      }
    } catch (_error) {
      // Ignore storage access errors.
    }

    return "";
  }

  function toBase64UrlUtf8(text) {
    const bytes = new TextEncoder().encode(String(text || ""));
    let binary = "";
    const chunkSize = 0x8000;
    for (let offset = 0; offset < bytes.length; offset += chunkSize) {
      const slice = bytes.subarray(offset, Math.min(offset + chunkSize, bytes.length));
      binary += String.fromCharCode.apply(null, Array.from(slice));
    }

    return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  }

  function buildPayloadBundleBase64(payloadJsonl) {
    const jsonl = String(payloadJsonl || "").trim();
    if (!jsonl) {
      return "";
    }

    const accountToken = resolveAccountToken();
    const lineCount = jsonl.split(/\r?\n/).filter(Boolean).length;
    const bundle = {
      schema: "gofile-payload-bundle/v1",
      accountToken,
      payloadJsonl: jsonl,
      meta: {
        source: "tampermonkey-gofile-payload-exporter",
        generatedAt: new Date().toISOString(),
        payloadCount: lineCount,
      },
    };

    return toBase64UrlUtf8(JSON.stringify(bundle));
  }

  function downloadTextFile(filename, text, mimeType) {
    const blob = new Blob([text], { type: mimeType || "text/plain;charset=utf-8" });
    const objectUrl = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = objectUrl;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(objectUrl);
  }

  async function copyText(text) {
    if (!text) {
      return false;
    }
    if (typeof GM_setClipboard === "function") {
      GM_setClipboard(text);
      return true;
    }
    if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
      await navigator.clipboard.writeText(text);
      return true;
    }
    return false;
  }

  function ensureStyles() {
    if (document.getElementById(STYLE_ID)) {
      return;
    }

    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = `
      #${BUTTON_ID} {
        position: fixed;
        right: 18px;
        bottom: 18px;
        z-index: 2147483640;
        border: 0;
        border-radius: 999px;
        padding: 10px 14px;
        background: #0b5ed7;
        color: #fff;
        font: 600 13px/1.2 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        cursor: pointer;
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.25);
      }
      #gpx-overlay {
        position: fixed;
        inset: 0;
        z-index: 2147483638;
        background: rgba(7, 12, 20, 0.5);
      }
      #${PANEL_ID} {
        position: fixed;
        z-index: 2147483639;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
        width: min(900px, calc(100vw - 20px));
        max-height: calc(100vh - 20px);
        overflow: auto;
        background: #f9fbff;
        border-radius: 12px;
        border: 1px solid #d7e1f0;
        padding: 14px;
        color: #132033;
        box-shadow: 0 16px 44px rgba(0, 0, 0, 0.28);
        font: 14px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
      #${PANEL_ID}.gpx-hidden,
      #gpx-overlay.gpx-hidden {
        display: none;
      }
      .gpx-title-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 8px;
      }
      .gpx-title {
        font-weight: 700;
        font-size: 16px;
      }
      .gpx-close {
        border: 1px solid #c2cee0;
        background: #fff;
        border-radius: 8px;
        padding: 4px 9px;
        cursor: pointer;
      }
      .gpx-textarea,
      .gpx-output,
      .gpx-log {
        width: 100%;
        box-sizing: border-box;
        border: 1px solid #c2cee0;
        border-radius: 8px;
        background: #fff;
        color: #16283f;
        font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        padding: 8px;
      }
      .gpx-textarea {
        min-height: 108px;
      }
      .gpx-output {
        min-height: 120px;
        margin-top: 8px;
      }
      .gpx-log {
        min-height: 96px;
        margin-top: 8px;
        white-space: pre-wrap;
      }
      .gpx-actions,
      .gpx-options {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 8px;
      }
      .gpx-actions button {
        border: 1px solid #b6c7dd;
        background: #fff;
        color: #142742;
        border-radius: 8px;
        padding: 7px 11px;
        cursor: pointer;
      }
      .gpx-actions button.gpx-primary {
        background: #0b5ed7;
        border-color: #0b5ed7;
        color: #fff;
      }
      .gpx-options label {
        display: flex;
        gap: 6px;
        align-items: center;
        font-size: 12px;
      }
      .gpx-options input {
        width: 90px;
        border: 1px solid #b6c7dd;
        border-radius: 6px;
        padding: 5px 6px;
      }
      .gpx-status {
        margin-top: 8px;
        font-size: 13px;
      }
    `;

    document.head.appendChild(style);
  }

  function createUi() {
    if (document.getElementById(BUTTON_ID)) {
      return null;
    }

    ensureStyles();

    const button = document.createElement("button");
    button.id = BUTTON_ID;
    button.type = "button";
    button.textContent = "GoFile JSONL";

    const overlay = document.createElement("div");
    overlay.id = "gpx-overlay";
    overlay.className = "gpx-hidden";

    const panel = document.createElement("div");
    panel.id = PANEL_ID;
    panel.className = "gpx-hidden";
    panel.innerHTML = `
      <div class="gpx-title-row">
        <div class="gpx-title">GoFile /contents -> compat JSONL</div>
        <button type="button" class="gpx-close" data-gpx-close="1">Close</button>
      </div>
      <textarea class="gpx-textarea" data-gpx-input placeholder="One gofile.io/d/... URL per line"></textarea>
      <div class="gpx-options">
        <label>Delay(ms)<input type="number" min="0" step="100" value="800" data-gpx-delay></label>
        <label>Retries<input type="number" min="0" step="1" value="3" data-gpx-retries></label>
      </div>
      <div class="gpx-actions">
        <button type="button" class="gpx-primary" data-gpx-capture>Capture from page tabs</button>
        <button type="button" data-gpx-copy>Copy JSONL</button>
        <button type="button" data-gpx-download>Download JSONL</button>
        <button type="button" data-gpx-copy-bundle>Copy Bundle (base64)</button>
        <button type="button" data-gpx-download-bundle>Download Bundle</button>
        <button type="button" data-gpx-clear>Clear</button>
      </div>
      <div class="gpx-status" data-gpx-status>Idle</div>
      <textarea class="gpx-output" data-gpx-output readonly placeholder="Success payload JSONL output"></textarea>
      <pre class="gpx-log" data-gpx-log></pre>
    `;

    document.body.appendChild(overlay);
    document.body.appendChild(panel);
    document.body.appendChild(button);

    return {
      button,
      overlay,
      panel,
      input: panel.querySelector("[data-gpx-input]"),
      delayInput: panel.querySelector("[data-gpx-delay]"),
      retriesInput: panel.querySelector("[data-gpx-retries]"),
      captureButton: panel.querySelector("[data-gpx-capture]"),
      copyButton: panel.querySelector("[data-gpx-copy]"),
      downloadButton: panel.querySelector("[data-gpx-download]"),
      copyBundleButton: panel.querySelector("[data-gpx-copy-bundle]"),
      downloadBundleButton: panel.querySelector("[data-gpx-download-bundle]"),
      clearButton: panel.querySelector("[data-gpx-clear]"),
      closeButton: panel.querySelector("[data-gpx-close]"),
      status: panel.querySelector("[data-gpx-status]"),
      output: panel.querySelector("[data-gpx-output]"),
      log: panel.querySelector("[data-gpx-log]"),
    };
  }

  function mount() {
    attachCaptureChannelListener();

    const refs = createUi();
    if (!refs) {
      return;
    }

    function setOpen(open) {
      refs.panel.classList.toggle("gpx-hidden", !open);
      refs.overlay.classList.toggle("gpx-hidden", !open);
    }

    function setRunning(running) {
      state.running = running;
      refs.captureButton.disabled = running;
      refs.copyButton.disabled = running;
      refs.downloadButton.disabled = running;
      refs.copyBundleButton.disabled = running;
      refs.downloadBundleButton.disabled = running;
      refs.clearButton.disabled = running;
      refs.input.disabled = running;
      refs.delayInput.disabled = running;
      refs.retriesInput.disabled = running;
    }

    function setStatus(text) {
      refs.status.textContent = text;
    }

    function appendLog(text) {
      const lines = refs.log.textContent ? refs.log.textContent.split("\n") : [];
      lines.push(text);
      refs.log.textContent = lines.slice(-200).join("\n");
      refs.log.scrollTop = refs.log.scrollHeight;
    }

    function getInputUrls() {
      return refs.input.value
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);
    }

    function collectTargets() {
      const rawUrls = getInputUrls();
      if (!rawUrls.length) {
        return { empty: true, targets: [], failures: [] };
      }

      const deduped = new Map();
      const failures = [];
      for (const rawUrl of rawUrls) {
        const contentId = extractContentId(rawUrl);
        if (!contentId) {
          failures.push({ url: rawUrl, error: "invalid URL" });
          continue;
        }
        if (!deduped.has(contentId)) {
          deduped.set(contentId, rawUrl);
        }
      }

      const targets = Array.from(deduped.entries()).map(([contentId, url]) => ({ contentId, url }));
      return { empty: false, targets, failures };
    }

    function applyResults(successPayloads, failures) {
      state.outputJsonl = successPayloads.map((payload) => JSON.stringify(payload)).join("\n");
      state.outputBundleBase64 = buildPayloadBundleBase64(state.outputJsonl);
      state.failures = failures;
      refs.output.value = state.outputJsonl;

      if (failures.length) {
        appendLog("Failures:");
        failures.forEach((item) => {
          appendLog(`- ${item.url}: ${item.error}`);
        });
      }

      setStatus(`Done. success=${successPayloads.length}, failed=${failures.length}`);
    }

    async function handleCapture() {
      if (state.running) {
        return;
      }

      const channel = getCaptureChannel();
      if (!channel) {
        setStatus("BroadcastChannel unavailable in this browser; capture mode cannot run.");
        return;
      }

      const parsedInput = collectTargets();
      if (parsedInput.empty) {
        setStatus("Please input at least one URL.");
        return;
      }

      const failures = parsedInput.failures.slice();
      const targets = parsedInput.targets;
      if (!targets.length) {
        state.outputJsonl = "";
        state.failures = failures;
        refs.output.value = "";
        setStatus(`No valid URL found (${failures.length} invalid line(s)).`);
        return;
      }

      const options = {
        baseDelayMs: toNonNegativeInt(refs.delayInput.value, 800),
        maxRetries: toNonNegativeInt(refs.retriesInput.value, 3),
      };

      refs.log.textContent = "";
      refs.output.value = "";
      setRunning(true);

      try {
        appendLog("Mode: capture real page /contents responses");
        appendLog(`Targets: ${targets.length}, Invalid lines: ${failures.length}`);
        const successPayloads = [];

        for (let index = 0; index < targets.length; index += 1) {
          const target = targets[index];
          setStatus(`Capturing ${index + 1}/${targets.length}: ${target.url}`);

          let captured = false;
          let lastError = "capture failed";

          for (let attempt = 0; attempt <= options.maxRetries; attempt += 1) {
            const jobId = generateJobId();
            const captureUrl = buildCaptureTabUrl(target.url, jobId, target.contentId);
            const tabHandle = openCaptureTab(captureUrl);
            if (tabHandle.blocked) {
              lastError = "failed to open capture tab (popup blocked)";
              appendLog(`[${index + 1}/${targets.length}] FAIL ${target.contentId}: ${lastError}`);
              break;
            }

            appendLog(
              `[${index + 1}/${targets.length}] Opened capture tab for ${target.contentId} (attempt ${attempt + 1}/${options.maxRetries + 1})`
            );

            const result = await waitForCaptureResult(jobId, CAPTURE_TIMEOUT_MS + 2000);
            tabHandle.close();

            if (result && result.ok && result.payload) {
              successPayloads.push(result.payload);
              captured = true;
              appendLog(
                `[${index + 1}/${targets.length}] OK ${target.contentId} via ${result.source || "page"}`
              );
              break;
            }

            lastError = (result && result.error) || "capture attempt failed";
            appendLog(
              `[${index + 1}/${targets.length}] Attempt ${attempt + 1} failed for ${target.contentId}: ${lastError}`
            );

            if (attempt < options.maxRetries) {
              const waitMs = Math.round(options.baseDelayMs * Math.pow(1.7, attempt + 1));
              if (waitMs > 0) {
                appendLog(`- ${target.contentId}: retry capture in ${waitMs}ms`);
                await sleep(waitMs);
              }
            }
          }

          if (!captured) {
            failures.push({ url: target.url, contentId: target.contentId, error: lastError });
          }

          if (index < targets.length - 1 && options.baseDelayMs > 0) {
            await sleep(options.baseDelayMs);
          }
        }

        applyResults(successPayloads, failures);
      } finally {
        setRunning(false);
      }
    }

    async function handleCopy() {
      if (!state.outputJsonl) {
        setStatus("No JSONL output to copy.");
        return;
      }
      try {
        const copied = await copyText(state.outputJsonl);
        setStatus(copied ? "Copied JSONL to clipboard." : "Clipboard not available in this context.");
      } catch (error) {
        setStatus(`Copy failed: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    function handleDownload() {
      if (!state.outputJsonl) {
        setStatus("No JSONL output to download.");
        return;
      }
      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      downloadTextFile(
        `gofile-payloads-${stamp}.jsonl`,
        `${state.outputJsonl}\n`,
        "application/x-ndjson;charset=utf-8"
      );
      setStatus("Downloaded JSONL file.");
    }

    async function handleCopyBundle() {
      if (!state.outputJsonl) {
        setStatus("No JSONL output to bundle.");
        return;
      }

      if (!state.outputBundleBase64) {
        state.outputBundleBase64 = buildPayloadBundleBase64(state.outputJsonl);
      }
      if (!state.outputBundleBase64) {
        setStatus("Could not build payload bundle.");
        return;
      }

      try {
        const copied = await copyText(state.outputBundleBase64);
        if (copied) {
          setStatus("Copied base64 payload bundle. CLI: run.py -pb then paste + double Enter.");
        } else {
          setStatus("Clipboard not available in this context.");
        }
      } catch (error) {
        setStatus(`Copy bundle failed: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    function handleDownloadBundle() {
      if (!state.outputJsonl) {
        setStatus("No JSONL output to bundle.");
        return;
      }

      if (!state.outputBundleBase64) {
        state.outputBundleBase64 = buildPayloadBundleBase64(state.outputJsonl);
      }
      if (!state.outputBundleBase64) {
        setStatus("Could not build payload bundle.");
        return;
      }

      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      downloadTextFile(`gofile-payload-bundle-${stamp}.txt`, `${state.outputBundleBase64}\n`);
      setStatus("Downloaded base64 payload bundle.");
    }

    function handleClear() {
      state.outputJsonl = "";
      state.outputBundleBase64 = "";
      state.failures = [];
      refs.output.value = "";
      refs.log.textContent = "";
      setStatus("Cleared output.");
    }

    refs.button.addEventListener("click", () => setOpen(true));
    refs.overlay.addEventListener("click", () => setOpen(false));
    refs.closeButton.addEventListener("click", () => setOpen(false));
    refs.captureButton.addEventListener("click", () => {
      void handleCapture();
    });
    refs.copyButton.addEventListener("click", () => {
      void handleCopy();
    });
    refs.downloadButton.addEventListener("click", handleDownload);
    refs.copyBundleButton.addEventListener("click", () => {
      void handleCopyBundle();
    });
    refs.downloadBundleButton.addEventListener("click", handleDownloadBundle);
    refs.clearButton.addEventListener("click", handleClear);

    setStatus("Idle");
  }

  const isCaptureTab = startCaptureAgentIfNeeded();
  if (!isCaptureTab) {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", mount, { once: true });
    } else {
      mount();
    }
  }
})();
