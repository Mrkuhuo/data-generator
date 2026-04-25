import { createReadStream, existsSync, readFileSync, statSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { createServer as createHttpServer, request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(rootDir, "frontend", "dist");
const port = Number(process.env.FRONTEND_PORT || 5173);
const backendBaseUrl = new URL(process.env.BACKEND_BASE_URL || "http://127.0.0.1:8888");
const isWslRuntime = detectWslRuntime();
const wslHostCandidates = isWslRuntime ? readWslHostCandidates() : [];

const contentTypes = new Map([
  [".html", "text/html; charset=utf-8"],
  [".js", "application/javascript; charset=utf-8"],
  [".css", "text/css; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".svg", "image/svg+xml"],
  [".png", "image/png"],
  [".jpg", "image/jpeg"],
  [".jpeg", "image/jpeg"],
  [".gif", "image/gif"],
  [".ico", "image/x-icon"],
  [".woff", "font/woff"],
  [".woff2", "font/woff2"],
  [".txt", "text/plain; charset=utf-8"]
]);

if (!existsSync(distDir)) {
  console.error(`Frontend dist not found: ${distDir}`);
  process.exit(1);
}

function detectWslRuntime() {
  if (process.platform !== "linux") {
    return false;
  }

  if (process.env.WSL_INTEROP) {
    return true;
  }

  try {
    return readFileSync("/proc/version", "utf8").toLowerCase().includes("microsoft");
  } catch {
    return false;
  }
}

function readWslHostCandidates() {
  try {
    const content = readFileSync("/etc/resolv.conf", "utf8");
    const hosts = content
      .split(/\r?\n/)
      .map((line) => line.match(/^nameserver\s+([^\s#]+)/)?.[1] ?? null)
      .filter((value) => typeof value === "string" && value.length > 0);

    return [...new Set(hosts)];
  } catch {
    return [];
  }
}

function resolveAssetPath(requestPath) {
  const safePath = decodeURIComponent(requestPath.split("?")[0]);
  const candidate = path.resolve(distDir, `.${safePath}`);
  if (!candidate.startsWith(distDir)) {
    return null;
  }
  if (existsSync(candidate) && statSync(candidate).isFile()) {
    return candidate;
  }
  return null;
}

function collectRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];

    req.on("data", (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

function isLoopbackHost(hostname) {
  return hostname === "127.0.0.1" || hostname === "localhost" || hostname === "::1";
}

function buildProxyTargets(requestPath) {
  const primaryTarget = new URL(requestPath, backendBaseUrl);
  const candidates = [primaryTarget];

  if (isWslRuntime && isLoopbackHost(backendBaseUrl.hostname)) {
    for (const host of wslHostCandidates) {
      const fallbackTarget = new URL(requestPath, backendBaseUrl);
      fallbackTarget.hostname = host;
      candidates.push(fallbackTarget);
    }
  }

  const seen = new Set();
  return candidates.filter((candidate) => {
    const key = `${candidate.protocol}//${candidate.host}${candidate.pathname}${candidate.search}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function shouldRetryProxy(error) {
  return Boolean(
    error &&
    typeof error === "object" &&
    "code" in error &&
    ["ECONNREFUSED", "ECONNRESET", "EHOSTUNREACH", "ETIMEDOUT"].includes(error.code)
  );
}

function sendProxyRequest(targetUrl, req, bodyBuffer) {
  return new Promise((resolve, reject) => {
    const transport = targetUrl.protocol === "https:" ? httpsRequest : httpRequest;
    const headers = {
      ...req.headers,
      host: `${targetUrl.hostname}${targetUrl.port ? `:${targetUrl.port}` : ""}`
    };

    if (bodyBuffer.length > 0) {
      headers["content-length"] = String(bodyBuffer.length);
    }

    const proxy = transport(
      {
        protocol: targetUrl.protocol,
        hostname: targetUrl.hostname,
        port: targetUrl.port,
        path: `${targetUrl.pathname}${targetUrl.search}`,
        method: req.method,
        headers
      },
      (proxyRes) => resolve(proxyRes)
    );

    proxy.on("error", reject);

    if (bodyBuffer.length > 0) {
      proxy.write(bodyBuffer);
    }
    proxy.end();
  });
}

async function proxyRequest(req, res) {
  const requestPath = req.url || "/";
  const bodyBuffer = await collectRequestBody(req);
  const targets = buildProxyTargets(requestPath);
  let lastError = null;

  for (let index = 0; index < targets.length; index += 1) {
    const targetUrl = targets[index];

    try {
      const proxyRes = await sendProxyRequest(targetUrl, req, bodyBuffer);
      res.writeHead(proxyRes.statusCode || 502, proxyRes.headers);
      proxyRes.pipe(res);
      return;
    } catch (error) {
      lastError = error;

      if (!shouldRetryProxy(error) || index === targets.length - 1) {
        break;
      }
    }
  }

  const detail = lastError instanceof Error ? lastError.message : "Unknown proxy error";
  res.writeHead(502, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify({
    success: false,
    message: "Frontend proxy request failed",
    detail
  }));
}

async function serveIndex(res) {
  const indexPath = path.join(distDir, "index.html");
  const content = await readFile(indexPath);
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(content);
}

const server = createHttpServer(async (req, res) => {
  try {
    const requestPath = req.url || "/";

    if (requestPath.startsWith("/api")) {
      await proxyRequest(req, res);
      return;
    }

    const assetPath = resolveAssetPath(requestPath === "/" ? "/index.html" : requestPath);
    if (assetPath) {
      const ext = path.extname(assetPath).toLowerCase();
      res.writeHead(200, {
        "Content-Type": contentTypes.get(ext) || "application/octet-stream"
      });
      createReadStream(assetPath).pipe(res);
      return;
    }

    await serveIndex(res);
  } catch (error) {
    res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
    res.end(error instanceof Error ? error.message : "Unknown server error");
  }
});

server.listen(port, "0.0.0.0", () => {
  console.log(`Frontend server ready on http://127.0.0.1:${port}`);
  console.log(`Proxying /api to ${backendBaseUrl.origin}`);
  if (isWslRuntime && wslHostCandidates.length > 0 && isLoopbackHost(backendBaseUrl.hostname)) {
    console.log(`WSL fallback hosts for backend proxy: ${wslHostCandidates.join(", ")}`);
  }
});
