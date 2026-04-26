import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(new URL("..", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1"));
const outputDir = resolve(root, "docs/assets/wechat-launch");
const runtimeDir = resolve(root, ".runtime/wechat-launch-images");

mkdirSync(outputDir, { recursive: true });
mkdirSync(runtimeDir, { recursive: true });

const chrome = findChrome();

const palette = {
  ink: "#11192d",
  muted: "#34425f",
  teal: "#0f8178",
  bg: "#f2f7fd",
  navy: "#111928",
  blue: "#2b5bd7",
  green: "#25c76a",
  cyan: "#24b7ad",
  sky: "#37b8ed",
  orange: "#c96a09",
  purple: "#7a39e6",
  pink: "#cf1b72",
  yellow: "#fff0bd"
};

const images = [
  {
    file: "01-system-architecture",
    eyebrow: "平台架构总览",
    title: ["一套平台", "写入五类目标端"],
    subtitle: "Spring Boot API + Vue 中文工作台",
    body: [
      "平台负责连接管理、数据生成、调度执行和结果回溯，",
      "目标端覆盖 MySQL、PostgreSQL、SQL Server、Oracle、Kafka，",
      "让测试数据从手写脚本，变成一个稳定的写入流程。"
    ],
    pills: [
      ["5 类目标端", "#dbeafe", "#1d4ed8"],
      ["Basic Auth", "#dcfce7", "#15803d"],
      ["密码加密", "#fef3c7", "#b45309"],
      ["执行回溯", "#fce7f3", "#be185d"]
    ],
    rightTitle: "写入架构",
    diagram: systemDiagram
  },
  {
    file: "02-user-flow",
    eyebrow: "使用流程总览",
    title: ["从连接开始", "到真实写入"],
    subtitle: "已有表自动映射，新表手动配置",
    body: [
      "用户先连接目标数据源，再选择已有表或定义新表，",
      "字段、类型、主键、非空信息尽量自动带出，",
      "最终执行一次写入，或者启动持续写入和定时写入。"
    ],
    pills: [
      ["连接测试", "#dbeafe", "#1d4ed8"],
      ["字段映射", "#dcfce7", "#15803d"],
      ["写入计划", "#fef3c7", "#b45309"],
      ["执行结果", "#fce7f3", "#be185d"]
    ],
    rightTitle: "用户路径",
    diagram: userFlowDiagram
  },
  {
    file: "03-execution-sequence",
    eyebrow: "执行链路总览",
    title: ["不只提交任务", "还要返回结果"],
    subtitle: "before / after / delta / validation",
    body: [
      "任务启动后先创建 RUNNING 实例，",
      "再进入数据生成、非空校验、目标端写入，",
      "执行结束后返回写入前后条数、净增条数和错误摘要。"
    ],
    pills: [
      ["RUNNING 实例", "#dbeafe", "#1d4ed8"],
      ["数据生成", "#dcfce7", "#15803d"],
      ["目标写入", "#fef3c7", "#b45309"],
      ["结果校验", "#fce7f3", "#be185d"]
    ],
    rightTitle: "执行链路",
    diagram: executionDiagram
  },
  {
    file: "04-relational-task-flow",
    eyebrow: "关系任务总览",
    title: ["父子表写入", "不留半成品"],
    subtitle: "JDBC 事务回滚 + 关系校验",
    body: [
      "关系任务会先生成父表，再根据父数据生成子表，",
      "数据库目标端使用同一个 JDBC 事务写入多张表，",
      "子表失败时回滚父表，避免测试环境留下脏数据。"
    ],
    pills: [
      ["父子关系", "#dbeafe", "#1d4ed8"],
      ["外键映射", "#dcfce7", "#15803d"],
      ["JDBC 事务", "#fef3c7", "#b45309"],
      ["失败回滚", "#fce7f3", "#be185d"]
    ],
    rightTitle: "关系写入",
    diagram: relationalDiagram
  },
  {
    file: "05-kafka-json-flow",
    eyebrow: "Kafka 消息总览",
    title: ["贴一段 JSON", "生成复杂消息"],
    subtitle: "Topic、Key、Header、路径映射",
    body: [
      "Kafka 不需要从空白字段开始配置，",
      "用户可以直接粘贴示例 JSON 或导入 JSON Schema，",
      "平台解析结构后再配置生成规则和父子 Topic 映射。"
    ],
    pills: [
      ["示例 JSON", "#dbeafe", "#1d4ed8"],
      ["JSON Schema", "#dcfce7", "#15803d"],
      ["Key / Header", "#fef3c7", "#b45309"],
      ["Topic 写入", "#fce7f3", "#be185d"]
    ],
    rightTitle: "复杂消息",
    diagram: kafkaDiagram
  },
  {
    file: "06-capability-map",
    eyebrow: "能力地图总览",
    title: ["一个工作台", "覆盖生成全流程"],
    subtitle: "连接、任务、关系、Kafka、执行、安全",
    body: [
      "平台把模拟数据生成拆成几条清晰主线，",
      "连接目标端，定义结构，生成数据，执行写入，查看结果，",
      "再补上认证、加密和旧接口隐藏，形成可发布的闭环。"
    ],
    pills: [
      ["数据源连接", "#dbeafe", "#1d4ed8"],
      ["写入任务", "#dcfce7", "#15803d"],
      ["关系任务", "#fef3c7", "#b45309"],
      ["发布安全", "#fce7f3", "#be185d"]
    ],
    rightTitle: "能力地图",
    diagram: capabilityDiagram
  }
];

for (const image of images) {
  const html = renderHtml(image);
  const htmlPath = resolve(runtimeDir, `${image.file}.html`);
  const pngPath = resolve(outputDir, `${image.file}.png`);
  writeFileSync(htmlPath, html, "utf8");
  execFileSync(chrome, [
    "--headless=new",
    "--disable-gpu",
    "--hide-scrollbars",
    "--force-device-scale-factor=1",
    "--window-size=1600,900",
    `--screenshot=${pngPath}`,
    pathToFileUrl(htmlPath)
  ], { stdio: "inherit" });
}

function renderHtml(image) {
  return `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<style>
html, body { margin: 0; width: 1600px; height: 900px; overflow: hidden; background: ${palette.bg}; }
body { font-family: "Microsoft YaHei", "PingFang SC", "Noto Sans CJK SC", Arial, sans-serif; }
</style>
</head>
<body>
${renderSvg(image)}
</body>
</html>`;
}

function renderSvg(image) {
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="900" viewBox="0 0 1600 900">
  <defs>
    <filter id="cardShadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="8" dy="10" stdDeviation="0" flood-color="#cbd5e1" flood-opacity="0.9"/>
    </filter>
    <filter id="softShadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="8" dy="10" stdDeviation="0" flood-color="#0f172a" flood-opacity="0.18"/>
    </filter>
  </defs>
  <rect width="1600" height="900" fill="${palette.bg}"/>
  <circle cx="90" cy="90" r="260" fill="#c9ddf2"/>
  <circle cx="1368" cy="90" r="170" fill="#c9eeee"/>
  <circle cx="1376" cy="690" r="170" fill="#c9eeee"/>
  ${text(86, 102, image.eyebrow, 36, palette.ink, 800)}
  <rect x="80" y="150" width="770" height="650" rx="36" fill="#fff" stroke="#d5e2f3" stroke-width="2" filter="url(#cardShadow)"/>
  ${multiLineText(130, 265, image.title, 64, palette.ink, 900, 82)}
  ${text(130, 400, image.subtitle, 30, palette.teal, 800)}
  ${multiLineText(130, 455, image.body, 21, palette.muted, 500, 42)}
  ${pills(image.pills)}
  <rect x="920" y="170" width="600" height="610" rx="36" fill="${palette.navy}" filter="url(#softShadow)"/>
  ${text(974, 250, image.rightTitle, 30, "#fff", 800)}
  ${image.diagram()}
  </svg>`;
}

function text(x, y, value, size, fill, weight = 600) {
  return `<text x="${x}" y="${y}" font-size="${size}" font-weight="${weight}" fill="${fill}">${escapeXml(value)}</text>`;
}

function multiLineText(x, y, lines, size, fill, weight = 600, lineHeight = 42) {
  return `<text x="${x}" y="${y}" font-size="${size}" font-weight="${weight}" fill="${fill}">
    ${lines.map((line, index) => `<tspan x="${x}" dy="${index === 0 ? 0 : lineHeight}">${escapeXml(line)}</tspan>`).join("")}
  </text>`;
}

function pills(items) {
  let cursor = 124;
  return items.map(([label, bg, fg], index) => {
    const width = Math.max(130, Math.min(190, estimateTextWidth(label) + 46));
    const x = cursor;
    cursor += width + 18;
    return `<rect x="${x}" y="610" width="${width}" height="47" rx="22" fill="${bg}"/>
    ${text(x + 24, 640, label, 22, fg, 800)}`;
  }).join("");
}

function node(x, y, w, h, label, color, size = 24) {
  return `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="18" fill="${color}"/>
  <text x="${x + w / 2}" y="${y + h / 2 + size / 3}" text-anchor="middle" font-size="${size}" font-weight="800" fill="#fff">${escapeXml(label)}</text>`;
}

function line(x1, y1, x2, y2, color = "#aab7cc", width = 5) {
  return `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${color}" stroke-width="${width}" stroke-linecap="round"/>`;
}

function dot(x, y, color = "#aab7cc") {
  return `<circle cx="${x}" cy="${y}" r="6" fill="${color}"/>`;
}

function systemDiagram() {
  return `
  ${node(990, 315, 150, 58, "Vue", palette.blue)}
  ${line(1140, 344, 1200, 344)}
  ${node(1200, 315, 210, 58, "Spring Boot", palette.green)}
  ${line(1095, 373, 1095, 466)}
  ${node(1010, 466, 170, 58, "生成引擎", palette.cyan)}
  ${line(1180, 495, 1220, 495)}
  ${node(1220, 466, 190, 58, "MySQL", palette.sky)}
  ${line(1095, 524, 1095, 606)}
  ${node(1010, 606, 170, 58, "调度器", palette.purple)}
  ${line(1180, 635, 1220, 635)}
  ${node(1220, 606, 190, 58, "Kafka", palette.pink)}
  ${text(974, 735, "从中文工作台到真实目标端，链路完整闭环。", 22, "#fff", 700)}
  `;
}

function userFlowDiagram() {
  const xs = 1050;
  const ys = [270, 365, 460, 555, 650];
  return `
  ${node(xs, ys[0], 220, 58, "连接数据源", palette.blue)}
  ${line(xs + 110, ys[0] + 58, xs + 110, ys[1])}
  ${node(xs, ys[1], 220, 58, "选择表结构", palette.teal)}
  ${line(xs + 110, ys[1] + 58, xs + 110, ys[2])}
  ${node(xs, ys[2], 220, 58, "配置规则", palette.orange)}
  ${line(xs + 110, ys[2] + 58, xs + 110, ys[3])}
  ${node(xs, ys[3], 220, 58, "执行写入", palette.purple)}
  ${line(xs + 110, ys[3] + 58, xs + 110, ys[4])}
  ${node(xs, ys[4], 220, 58, "查看结果", palette.pink)}
  ${line(1270, ys[0] + 29, 1360, ys[0] + 29)}
  ${node(1360, ys[0] + 2, 115, 54, "测试", palette.green, 22)}
  ${line(1270, ys[2] + 29, 1360, ys[2] + 29)}
  ${node(1360, ys[2] + 2, 115, 54, "预览", palette.sky, 22)}
  ${text(974, 735, "能选择就不填写，能自动就不手动。", 22, "#fff", 700)}
  `;
}

function executionDiagram() {
  const steps = [
    ["RUNNING 实例", palette.blue],
    ["生成数据", palette.green],
    ["校验规则", palette.cyan],
    ["目标写入", palette.sky],
    ["结果回溯", palette.pink]
  ];
  return steps.map(([label, color], index) => {
    const y = 308 + index * 78;
    return `${node(996, y, 190, 52, label, color, 22)}
    ${index < steps.length - 1 ? line(1091, y + 52, 1091, y + 78) : ""}
    ${index === 1 ? `${line(1186, y + 26, 1260, y + 26)}${node(1260, y - 1, 180, 54, "before", palette.orange, 22)}` : ""}
    ${index === 3 ? `${line(1186, y + 26, 1260, y + 26)}${node(1260, y - 1, 180, 54, "after / delta", palette.purple, 22)}` : ""}`;
  }).join("") + text(974, 735, "不是只说提交成功，而是告诉你写入了什么。", 22, "#fff", 700);
}

function relationalDiagram() {
  return `
  ${node(990, 305, 170, 58, "父表", palette.blue)}
  ${line(1075, 363, 1075, 455)}
  ${node(990, 455, 170, 58, "子表", palette.green)}
  ${line(1160, 334, 1240, 334)}
  ${node(1240, 305, 220, 58, "生成父数据", palette.cyan)}
  ${line(1160, 484, 1240, 484)}
  ${node(1240, 455, 220, 58, "映射外键", palette.sky)}
  ${line(1075, 513, 1075, 605)}
  ${node(970, 605, 210, 58, "JDBC 事务", palette.purple)}
  ${line(1180, 634, 1240, 634)}
  ${node(1240, 605, 220, 58, "失败回滚", palette.pink)}
  ${text(974, 735, "父子写入要一起成功，不能留下半截数据。", 22, "#fff", 700)}
  `;
}

function kafkaDiagram() {
  return `
  ${node(980, 325, 200, 58, "示例 JSON", palette.blue)}
  ${line(1180, 354, 1220, 354)}
  ${node(1220, 325, 230, 58, "解析 Schema", palette.green)}
  ${line(1090, 383, 1090, 475)}
  ${node(980, 475, 200, 58, "配置规则", palette.cyan)}
  ${line(1180, 504, 1220, 504)}
  ${node(1220, 475, 230, 58, "Key / Header", palette.sky)}
  ${line(1090, 533, 1090, 625)}
  ${node(980, 625, 200, 58, "生成消息", palette.purple)}
  ${line(1180, 654, 1220, 654)}
  ${node(1220, 625, 230, 58, "写入 Topic", palette.pink)}
  ${text(974, 735, "复杂消息从一段 JSON 开始，不从空白表单开始。", 22, "#fff", 700)}
  `;
}

function capabilityDiagram() {
  const centerX = 1220;
  const centerY = 505;
  const items = [
    [1040, 305, "数据源", palette.blue],
    [1265, 305, "写入任务", palette.green],
    [955, 480, "关系任务", palette.cyan],
    [1330, 480, "Kafka", palette.sky],
    [1040, 655, "执行回溯", palette.purple],
    [1275, 655, "发布安全", palette.pink]
  ];
  const lines = items.map(([x, y, , color]) => line(centerX, centerY, x + 75, y + 26, color, 7)).join("");
  const nodes = items.map(([x, y, label, color]) => node(x, y, 150, 52, label, color, 22)).join("");
  return `
  ${lines}
  ${nodes}
  <circle cx="${centerX}" cy="${centerY}" r="84" fill="#1f4ed8"/>
  <text x="${centerX}" y="${centerY - 8}" text-anchor="middle" font-size="24" font-weight="900" fill="#fff">模拟数据</text>
  <text x="${centerX}" y="${centerY + 26}" text-anchor="middle" font-size="24" font-weight="900" fill="#fff">生成平台</text>
  ${text(974, 735, "从连接到安全发布，一张图看完整闭环。", 22, "#fff", 700)}
  `;
}

function estimateTextWidth(value) {
  let width = 0;
  for (const char of value) {
    if (/[\u4e00-\u9fff]/.test(char)) {
      width += 22;
    } else if (char === " ") {
      width += 8;
    } else if (char === "/" || char === "-") {
      width += 10;
    } else {
      width += 12;
    }
  }
  return width;
}

function escapeXml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;");
}

function pathToFileUrl(filePath) {
  return `file:///${filePath.replaceAll("\\", "/").replace(/^([A-Za-z]:)/, "$1")}`;
}

function findChrome() {
  const candidates = [
    process.env.CHROME_PATH,
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe"
  ].filter(Boolean);
  const found = candidates.find((candidate) => existsSync(candidate));
  if (!found) {
    throw new Error("未找到 Chrome 或 Edge，请设置 CHROME_PATH 后重试。");
  }
  return found;
}
