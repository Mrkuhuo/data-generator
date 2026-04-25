<template>
  <section class="payload-workbench">
    <aside class="payload-workbench__tree">
      <div class="payload-workbench__tree-header">
        <div>
          <div class="payload-workbench__eyebrow">
            <span class="pill pill--soft">Kafka</span>
            <span class="pill pill--soft">复杂 JSON</span>
          </div>
          <h4>消息结构</h4>
          <p>左侧选择节点，右侧编辑字段属性和生成规则。</p>
        </div>
        <div class="payload-workbench__tree-meta">
          <span class="pill pill--soft">节点 {{ treeEntries.length }}</span>
          <span class="pill pill--soft">标量 {{ scalarNodeCount }}</span>
        </div>
      </div>

      <section v-if="scalarPathEntries.length" class="payload-workbench__path-summary">
        <div class="payload-workbench__path-summary-header">
          <div>
            <h5>已解析字段</h5>
            <p>解析完成后可以直接点击字段，右侧会定位到对应规则。</p>
          </div>
          <span class="pill pill--soft">{{ scalarPathEntries.length }}</span>
        </div>

        <div class="payload-workbench__path-list">
          <button
            v-for="entry in scalarPathEntries"
            :key="`path-${entry.id}`"
            class="payload-workbench__path-item"
            :class="{ 'payload-workbench__path-item--active': entry.id === selectedNodeId }"
            type="button"
            @click="selectNode(entry.id)"
          >
            <strong>{{ entry.path }}</strong>
            <span>{{ entry.summary }}</span>
          </button>
        </div>
      </section>

      <div class="payload-workbench__tree-list">
        <button
          v-for="entry in treeEntries"
          :key="entry.id"
          class="payload-workbench__tree-item"
          :class="{ 'payload-workbench__tree-item--active': entry.id === selectedNodeId }"
          :style="{ '--tree-depth': String(entry.depth) }"
          :data-tree-node-id="entry.id"
          type="button"
          @click="selectNode(entry.id)"
        >
          <div class="payload-workbench__tree-item-main">
            <span class="payload-workbench__tree-item-label">{{ entry.label }}</span>
            <span class="payload-workbench__tree-item-summary">{{ entry.summary }}</span>
          </div>
          <span class="pill pill--soft">{{ nodeTypeLabel(entry.node.type) }}</span>
        </button>
      </div>
    </aside>

    <article
      v-if="selectedNode"
      class="payload-node payload-workbench__inspector"
      :class="{ 'validation-block--error': activeNodeId === selectedNode.id }"
      :data-node-id="selectedNode.id"
    >
      <header class="payload-node__header">
        <div class="payload-node__identity">
          <div class="payload-node__eyebrow">
            <span class="pill pill--soft">{{ nodeTypeLabel(selectedNode.type) }}</span>
            <span v-if="isSelectedRoot" class="pill pill--soft">根节点</span>
            <span v-else-if="selectedEntry?.isItem" class="pill pill--soft">数组元素</span>
          </div>
          <h4>{{ selectedHeading }}</h4>
          <p class="payload-node__summary">{{ selectedSummary }}</p>
        </div>

        <div class="panel__actions panel__actions--tight payload-node__header-actions">
          <button v-if="!isSelectedRoot" class="button button--danger" type="button" @click="removeSelectedNode">删除</button>
        </div>
      </header>

      <div class="payload-node__body">
        <div v-if="!isSelectedRoot" class="field field--half payload-node__grid">
          <label v-if="!selectedEntry?.isItem">
            <span>字段名</span>
            <input
              v-model.trim="selectedNode.name"
              :class="{ 'field-control--error': isFieldHighlighted('name') }"
              data-node-field="name"
              type="text"
              :placeholder="selectedNamePlaceholder"
            />
          </label>

          <label>
            <span>节点类型</span>
            <select
              v-model="selectedNode.type"
              :class="{ 'field-control--error': isFieldHighlighted('type') }"
              data-node-field="type"
              @change="handleSelectedTypeChange"
            >
              <option v-for="type in nodeTypes" :key="type" :value="type">{{ nodeTypeLabel(type) }}</option>
            </select>
          </label>
        </div>

        <div v-if="!isSelectedRoot" class="payload-node__toggles">
          <label class="checkbox-chip">
            <input v-model="selectedNode.nullable" type="checkbox" />
            <span>允许为空</span>
          </label>
        </div>

        <template v-if="selectedNode.type === 'OBJECT'">
          <section
            class="payload-node__section"
            :class="{ 'validation-block--error': isSectionHighlighted('children') }"
            data-node-section="children"
          >
            <div class="payload-node__section-header">
              <div>
                <h5>{{ isSelectedRoot ? "顶层字段" : "对象字段" }}</h5>
                <p>{{ selectedNode.children.length ? `已配置 ${selectedNode.children.length} 个子节点` : "还没有子节点，先从新增字段开始。" }}</p>
              </div>

              <div class="panel__actions panel__actions--tight payload-node__section-actions">
                <button
                  class="button button--ghost"
                  :class="{ 'field-control--error': isActionHighlighted('add-scalar') }"
                  data-node-action="add-scalar"
                  type="button"
                  @click="addChildToSelected('SCALAR')"
                >
                  新增标量
                </button>
                <button
                  class="button button--ghost"
                  :class="{ 'field-control--error': isActionHighlighted('add-object') }"
                  data-node-action="add-object"
                  type="button"
                  @click="addChildToSelected('OBJECT')"
                >
                  新增对象
                </button>
                <button
                  class="button button--ghost"
                  :class="{ 'field-control--error': isActionHighlighted('add-array') }"
                  data-node-action="add-array"
                  type="button"
                  @click="addChildToSelected('ARRAY')"
                >
                  新增数组
                </button>
              </div>
            </div>

            <div v-if="selectedNode.children.length" class="payload-node__linked-list">
              <button
                v-for="child in selectedNode.children"
                :key="child.id"
                class="payload-node__linked-item"
                :class="{ 'payload-node__linked-item--active': child.id === selectedNodeId }"
                type="button"
                @click="selectNode(child.id)"
              >
                <div>
                  <strong>{{ readableNodeName(child, false) }}</strong>
                  <p>{{ describeNodeSummary(child, false) }}</p>
                </div>
                <span class="pill pill--soft">{{ nodeTypeLabel(child.type) }}</span>
              </button>
            </div>
            <p v-else class="payload-node__empty">暂无字段</p>
          </section>
        </template>

        <template v-else-if="selectedNode.type === 'ARRAY'">
          <div class="field field--half payload-node__grid">
            <label>
              <span>最少元素数</span>
              <input
                v-model.number="selectedNode.minItems"
                :class="{ 'field-control--error': isFieldHighlighted('minItems') }"
                data-node-field="minItems"
                type="number"
                min="0"
                placeholder="默认 1"
              />
            </label>

            <label>
              <span>最多元素数</span>
              <input
                v-model.number="selectedNode.maxItems"
                :class="{ 'field-control--error': isFieldHighlighted('maxItems') }"
                data-node-field="maxItems"
                type="number"
                min="0"
                placeholder="默认与最少元素数一致"
              />
            </label>
          </div>

          <section
            class="payload-node__section"
            :class="{ 'validation-block--error': isSectionHighlighted('item-schema') }"
            data-node-section="item-schema"
          >
            <div class="payload-node__section-header">
              <div>
                <h5>数组元素结构</h5>
                <p>{{ selectedNode.itemSchema ? "已定义元素结构，点击即可继续编辑。" : "先选择数组元素的类型。" }}</p>
              </div>

              <div class="panel__actions panel__actions--tight payload-node__section-actions">
                <button
                  class="button button--ghost"
                  :class="{ 'field-control--error': isActionHighlighted('set-item-scalar') }"
                  data-node-action="set-item-scalar"
                  type="button"
                  @click="setSelectedItemSchema('SCALAR')"
                >
                  标量元素
                </button>
                <button
                  class="button button--ghost"
                  :class="{ 'field-control--error': isActionHighlighted('set-item-object') }"
                  data-node-action="set-item-object"
                  type="button"
                  @click="setSelectedItemSchema('OBJECT')"
                >
                  对象元素
                </button>
                <button
                  class="button button--ghost"
                  :class="{ 'field-control--error': isActionHighlighted('set-item-array') }"
                  data-node-action="set-item-array"
                  type="button"
                  @click="setSelectedItemSchema('ARRAY')"
                >
                  数组元素
                </button>
              </div>
            </div>

            <div v-if="selectedNode.itemSchema" class="payload-node__linked-list">
              <button class="payload-node__linked-item" type="button" @click="selectNode(selectedNode.itemSchema.id)">
                <div>
                  <strong>{{ readableNodeName(selectedNode.itemSchema, true) }}</strong>
                  <p>{{ describeNodeSummary(selectedNode.itemSchema, true) }}</p>
                </div>
                <span class="pill pill--soft">{{ nodeTypeLabel(selectedNode.itemSchema.type) }}</span>
              </button>
            </div>
            <p v-else class="payload-node__empty">未设置元素结构</p>
          </section>
        </template>

        <template v-else>
          <div class="field field--half payload-node__grid">
            <label>
              <span>值类型</span>
              <select
                v-model="selectedNode.valueType"
                :class="{ 'field-control--error': isFieldHighlighted('valueType') }"
                data-node-field="valueType"
              >
                <option v-for="valueType in valueTypes" :key="valueType" :value="valueType">{{ valueTypeLabel(valueType) }}</option>
              </select>
            </label>

            <label>
              <span>生成规则</span>
              <select
                v-model="selectedNode.generatorType"
                :class="{ 'field-control--error': isFieldHighlighted('generatorType') }"
                data-node-field="generatorType"
                @change="resetGeneratorConfig"
              >
                <option v-for="type in generatorTypes" :key="type" :value="type">{{ labelColumnGeneratorType(type) }}</option>
              </select>
            </label>
          </div>

          <section class="payload-node__config-card">
            <div class="payload-node__config-header">
              <div>
                <h5>推荐配置</h5>
                <p>{{ generatorEditorHint }}</p>
              </div>
              <div class="payload-node__config-meta">
                <span class="pill pill--soft">{{ valueTypeLabel(selectedNode.valueType) }}</span>
                <span class="pill pill--soft">{{ labelColumnGeneratorType(selectedNode.generatorType) }}</span>
              </div>
            </div>

            <div v-if="selectedNode.generatorType === 'SEQUENCE'" class="field field--half payload-node__grid">
              <label>
                <span>起始值</span>
                <input :value="readGeneratorNumber('start', 1)" type="number" @input="updateGeneratorNumber('start', $event)" />
              </label>
              <label>
                <span>步长</span>
                <input :value="readGeneratorNumber('step', 1)" type="number" @input="updateGeneratorNumber('step', $event)" />
              </label>
            </div>

            <div v-else-if="selectedNode.generatorType === 'RANDOM_INT'" class="field field--half payload-node__grid">
              <label>
                <span>最小值</span>
                <input :value="readGeneratorNumber('min', 0)" type="number" @input="updateGeneratorNumber('min', $event)" />
              </label>
              <label>
                <span>最大值</span>
                <input :value="readGeneratorNumber('max', 1000)" type="number" @input="updateGeneratorNumber('max', $event)" />
              </label>
            </div>

            <div v-else-if="selectedNode.generatorType === 'RANDOM_DECIMAL'" class="field field--half payload-node__grid">
              <label>
                <span>最小值</span>
                <input :value="readGeneratorNumber('min', 0)" type="number" step="0.01" @input="updateGeneratorNumber('min', $event)" />
              </label>
              <label>
                <span>最大值</span>
                <input :value="readGeneratorNumber('max', 1000)" type="number" step="0.01" @input="updateGeneratorNumber('max', $event)" />
              </label>
              <label>
                <span>小数位</span>
                <input :value="readGeneratorNumber('scale', 2)" type="number" min="0" @input="updateGeneratorNumber('scale', $event)" />
              </label>
            </div>

            <template v-else-if="selectedNode.generatorType === 'STRING'">
              <div class="field field--half payload-node__grid">
                <label>
                  <span>模式</span>
                  <select :value="readGeneratorMode()" @change="updateGeneratorMode">
                    <option value="random">随机字符串</option>
                    <option value="email">邮箱地址</option>
                  </select>
                </label>
                <label>
                  <span>前缀</span>
                  <input :value="readGeneratorText('prefix', '')" type="text" placeholder="可选" @input="updateGeneratorText('prefix', $event)" />
                </label>
              </div>

              <div class="field field--half payload-node__grid">
                <label v-if="readGeneratorMode() === 'random'">
                  <span>随机长度</span>
                  <input :value="readGeneratorNumber('length', 12)" type="number" min="1" @input="updateGeneratorNumber('length', $event)" />
                </label>
                <label v-else>
                  <span>邮箱域名</span>
                  <input :value="readGeneratorText('domain', 'demo.local')" type="text" placeholder="demo.local" @input="updateGeneratorText('domain', $event)" />
                </label>

                <label>
                  <span>后缀</span>
                  <input :value="readGeneratorText('suffix', '')" type="text" placeholder="可选" @input="updateGeneratorText('suffix', $event)" />
                </label>
              </div>

              <div v-if="readGeneratorMode() === 'random'" class="field field--half payload-node__grid">
                <label>
                  <span>字符集</span>
                  <select :value="readGeneratorCharsetPreset()" @change="updateGeneratorCharsetPreset">
                    <option v-for="option in randomStringCharsetOptions" :key="option.value" :value="option.value">
                      {{ option.label }}
                    </option>
                    <option value="CUSTOM">自定义</option>
                  </select>
                </label>

                <label v-if="readGeneratorCharsetPreset() === 'CUSTOM'">
                  <span>自定义字符</span>
                  <input :value="readGeneratorCharset()" type="text" placeholder="例如：Abc123_-" @input="updateGeneratorText('charset', $event)" />
                </label>
              </div>
            </template>

            <div v-else-if="selectedNode.generatorType === 'ENUM'" class="field payload-node__grid">
              <label>
                <span>可选值</span>
                <textarea :value="readGeneratorValues()" rows="4" placeholder="每行一个值，也支持英文逗号分隔" @input="updateGeneratorValues"></textarea>
              </label>
            </div>

            <div v-else-if="selectedNode.generatorType === 'BOOLEAN'" class="field field--half payload-node__grid">
              <label>
                <span>true 概率</span>
                <input :value="readGeneratorNumber('trueRate', 0.5)" type="number" min="0" max="1" step="0.01" @input="updateGeneratorNumber('trueRate', $event)" />
              </label>
            </div>

            <div v-else-if="selectedNode.generatorType === 'DATETIME'" class="field field--half payload-node__grid">
              <label>
                <span>开始时间</span>
                <input :value="readGeneratorDateTime('from', defaultGeneratorFrom())" type="datetime-local" @input="updateGeneratorText('from', $event)" />
              </label>
              <label>
                <span>结束时间</span>
                <input :value="readGeneratorDateTime('to', defaultGeneratorTo())" type="datetime-local" @input="updateGeneratorText('to', $event)" />
              </label>
            </div>

            <div v-else class="payload-node__config-note">
              <strong>自动生成唯一值</strong>
              <p>当前规则不需要额外参数，系统会为每条消息自动生成 UUID。</p>
            </div>
          </section>

          <details
            class="details-panel payload-node__details"
            :open="advancedConfigOpen || isFieldHighlighted('generatorConfig')"
            @toggle="handleAdvancedToggle"
          >
            <summary class="payload-node__details-summary">
              <span>高级 JSON</span>
              <small>{{ generatorConfigMeta }}</small>
            </summary>
            <div class="details-panel__content payload-node__details-content">
              <p class="payload-node__details-hint">常见配置直接使用上面的推荐表单即可；只有在需要精细控制时，才需要手动编辑 JSON。</p>
              <textarea
                v-model.trim="selectedNode.generatorConfigJson"
                class="payload-node__code"
                :class="{ 'field-control--error': isFieldHighlighted('generatorConfig') }"
                data-node-field="generatorConfig"
                rows="8"
                placeholder='例如：{"start":1,"step":1}'
              ></textarea>
            </div>
          </details>
        </template>
      </div>
    </article>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { labelColumnGeneratorType } from "../utils/display";

type GeneratorType = "SEQUENCE" | "RANDOM_INT" | "RANDOM_DECIMAL" | "STRING" | "ENUM" | "BOOLEAN" | "DATETIME" | "UUID";
type PayloadNodeType = "OBJECT" | "ARRAY" | "SCALAR";
type PayloadValueType = "STRING" | "INT" | "LONG" | "DECIMAL" | "BOOLEAN" | "DATETIME" | "UUID";
type RandomStringCharsetPreset = "LOWERCASE" | "LOWERCASE_DIGITS" | "LETTERS_DIGITS" | "DIGITS" | "CUSTOM";

interface PayloadSchemaNodeDraft {
  id: string;
  name: string;
  type: PayloadNodeType;
  nullable: boolean;
  valueType: PayloadValueType;
  generatorType: GeneratorType;
  generatorConfigJson: string;
  children: PayloadSchemaNodeDraft[];
  itemSchema: PayloadSchemaNodeDraft | null;
  minItems: number | null;
  maxItems: number | null;
}

interface TreeEntry {
  id: string;
  node: PayloadSchemaNodeDraft;
  depth: number;
  label: string;
  summary: string;
  isItem: boolean;
}

interface ScalarPathEntry {
  id: string;
  node: PayloadSchemaNodeDraft;
  path: string;
  summary: string;
}

const props = withDefaults(defineProps<{
  node: PayloadSchemaNodeDraft;
  activeSelector?: string | null;
}>(), {
  activeSelector: null
});

const generatorTypes: GeneratorType[] = ["SEQUENCE", "RANDOM_INT", "RANDOM_DECIMAL", "STRING", "ENUM", "BOOLEAN", "DATETIME", "UUID"];
const nodeTypes: PayloadNodeType[] = ["OBJECT", "ARRAY", "SCALAR"];
const valueTypes: PayloadValueType[] = ["STRING", "INT", "LONG", "DECIMAL", "BOOLEAN", "DATETIME", "UUID"];
const randomStringCharsetOptions: Array<{ value: Exclude<RandomStringCharsetPreset, "CUSTOM">; label: string; charset: string }> = [
  { value: "LOWERCASE", label: "小写字母", charset: "abcdefghijklmnopqrstuvwxyz" },
  { value: "LOWERCASE_DIGITS", label: "小写字母 + 数字", charset: "abcdefghijklmnopqrstuvwxyz0123456789" },
  { value: "LETTERS_DIGITS", label: "大小写字母 + 数字", charset: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" },
  { value: "DIGITS", label: "仅数字", charset: "0123456789" }
];

const manualSelectedNodeId = ref(props.node.id);
const advancedConfigOpen = ref(false);
const stringCharsetPreset = ref<RandomStringCharsetPreset | null>(null);

const activeNodeId = computed(() => {
  if (!props.activeSelector) {
    return null;
  }
  const match = props.activeSelector.match(/\[data-node-id="([^"]+)"\]/);
  return match ? match[1] : null;
});

const treeEntries = computed(() => flattenNodeTree(props.node));
const scalarNodeCount = computed(() => treeEntries.value.filter((entry) => entry.node.type === "SCALAR").length);
const scalarPathEntries = computed(() => collectScalarPathEntries(props.node));
const firstSelectableNodeId = computed(() => scalarPathEntries.value[0]?.id ?? treeEntries.value[1]?.id ?? props.node.id);
const selectedNodeId = computed(() => {
  if (activeNodeId.value && findNodeById(props.node, activeNodeId.value)) {
    return activeNodeId.value;
  }
  if (findNodeById(props.node, manualSelectedNodeId.value)) {
    return manualSelectedNodeId.value;
  }
  return firstSelectableNodeId.value;
});
const selectedEntry = computed(() => treeEntries.value.find((entry) => entry.id === selectedNodeId.value) ?? null);
const selectedNode = computed(() => selectedEntry.value?.node ?? props.node);
const isSelectedRoot = computed(() => selectedNode.value.id === props.node.id);
const currentNodeSelector = computed(() => `[data-node-id="${selectedNode.value.id}"]`);

const selectedHeading = computed(() => {
  if (isSelectedRoot.value) {
    return "Kafka 消息结构";
  }
  if (selectedEntry.value?.isItem) {
    return `${nodeTypeLabel(selectedNode.value.type)}元素`;
  }
  return readableNodeName(selectedNode.value, false);
});

const selectedSummary = computed(() => describeNodeSummary(selectedNode.value, Boolean(selectedEntry.value?.isItem)));

const selectedNamePlaceholder = computed(() => {
  if (selectedNode.value.type === "OBJECT") {
    return "例如：order";
  }
  if (selectedNode.value.type === "ARRAY") {
    return "例如：items";
  }
  return "例如：amount";
});

const generatorEditorHint = computed(() => {
  switch (selectedNode.value.generatorType) {
    case "SEQUENCE":
      return "适合主键、自增编号这类连续字段。";
    case "RANDOM_INT":
      return "设置整数范围，系统会自动在区间内取值。";
    case "RANDOM_DECIMAL":
      return "适合金额、比例这类带小数的数据。";
    case "STRING":
      return "先选模式，再配置长度、前后缀或邮箱域名。";
    case "ENUM":
      return "按候选值集合随机抽取，适合状态、类型字段。";
    case "BOOLEAN":
      return "通过 true 概率控制真假分布。";
    case "DATETIME":
      return "系统会在时间区间内随机生成时间值。";
    case "UUID":
      return "无需额外配置，默认自动生成唯一标识。";
  }
});

const generatorConfigMeta = computed(() => {
  const count = Object.keys(readGeneratorConfig()).length;
  return count ? `当前 ${count} 个参数` : "使用默认参数";
});

function selectNode(nodeId: string) {
  manualSelectedNodeId.value = nodeId;
  advancedConfigOpen.value = false;
  stringCharsetPreset.value = null;
}

function nodeTypeLabel(type: PayloadNodeType) {
  switch (type) {
    case "OBJECT":
      return "对象";
    case "ARRAY":
      return "数组";
    case "SCALAR":
      return "标量";
  }
}

function valueTypeLabel(type: PayloadValueType) {
  switch (type) {
    case "STRING":
      return "字符串";
    case "INT":
      return "整数";
    case "LONG":
      return "长整数";
    case "DECIMAL":
      return "小数";
    case "BOOLEAN":
      return "布尔";
    case "DATETIME":
      return "时间";
    case "UUID":
      return "UUID";
  }
}

function readableNodeName(node: PayloadSchemaNodeDraft, isItem: boolean) {
  if (isItem) {
    return "数组元素";
  }
  if (node.name.trim()) {
    return node.name.trim();
  }
  switch (node.type) {
    case "OBJECT":
      return "未命名对象";
    case "ARRAY":
      return "未命名数组";
    case "SCALAR":
      return "未命名字段";
  }
}

function describeNodeSummary(node: PayloadSchemaNodeDraft, isItem: boolean) {
  if (node.type === "OBJECT") {
    return isItem ? `对象元素 / ${node.children.length} 个子节点` : `${node.children.length} 个子节点`;
  }
  if (node.type === "ARRAY") {
    const minItems = node.minItems ?? 1;
    const maxItems = node.maxItems ?? node.minItems ?? 3;
    return `元素数量 ${minItems} - ${maxItems}`;
  }
  return `${valueTypeLabel(node.valueType)} / ${labelColumnGeneratorType(node.generatorType)}`;
}

function flattenNodeTree(root: PayloadSchemaNodeDraft) {
  const entries: TreeEntry[] = [];

  const visit = (node: PayloadSchemaNodeDraft, depth: number, isItem: boolean) => {
    entries.push({
      id: node.id,
      node,
      depth,
      label: depth === 0 ? "消息根节点" : readableNodeName(node, isItem),
      summary: describeNodeSummary(node, isItem),
      isItem
    });

    if (node.type === "OBJECT") {
      node.children.forEach((child) => visit(child, depth + 1, false));
      return;
    }

    if (node.type === "ARRAY" && node.itemSchema) {
      visit(node.itemSchema, depth + 1, true);
    }
  };

  visit(root, 0, false);
  return entries;
}

function collectScalarPathEntries(node: PayloadSchemaNodeDraft, parentPath = ""): ScalarPathEntry[] {
  if (node.type === "OBJECT") {
    return node.children.flatMap((child) => {
      const nextPath = joinPath(parentPath, child.name);
      return collectScalarPathEntries(child, nextPath);
    });
  }

  if (node.type === "ARRAY") {
    const arrayPath = parentPath ? `${parentPath}[]` : "[]";
    return node.itemSchema ? collectScalarPathEntries(node.itemSchema, arrayPath) : [];
  }

  return [{
    id: node.id,
    node,
    path: parentPath || "(root)",
    summary: `${valueTypeLabel(node.valueType)} / ${labelColumnGeneratorType(node.generatorType)}`
  }];
}

function joinPath(parentPath: string, name: string) {
  const segment = name.trim();
  if (!segment) {
    return parentPath;
  }
  if (!parentPath) {
    return segment;
  }
  return `${parentPath}.${segment}`;
}

function findNodeById(node: PayloadSchemaNodeDraft, nodeId: string): PayloadSchemaNodeDraft | null {
  if (node.id === nodeId) {
    return node;
  }

  for (const child of node.children) {
    const matched = findNodeById(child, nodeId);
    if (matched) {
      return matched;
    }
  }

  if (node.itemSchema) {
    return findNodeById(node.itemSchema, nodeId);
  }

  return null;
}

function findParentNode(node: PayloadSchemaNodeDraft, targetId: string): PayloadSchemaNodeDraft | null {
  if (node.children.some((child) => child.id === targetId) || node.itemSchema?.id === targetId) {
    return node;
  }

  for (const child of node.children) {
    const matched = findParentNode(child, targetId);
    if (matched) {
      return matched;
    }
  }

  if (node.itemSchema) {
    return findParentNode(node.itemSchema, targetId);
  }

  return null;
}

function removeNodeById(node: PayloadSchemaNodeDraft, targetId: string): boolean {
  const childIndex = node.children.findIndex((child) => child.id === targetId);
  if (childIndex >= 0) {
    node.children.splice(childIndex, 1);
    return true;
  }

  if (node.itemSchema?.id === targetId) {
    node.itemSchema = null;
    return true;
  }

  for (const child of node.children) {
    if (removeNodeById(child, targetId)) {
      return true;
    }
  }

  return node.itemSchema ? removeNodeById(node.itemSchema, targetId) : false;
}

function createNode(type: PayloadNodeType): PayloadSchemaNodeDraft {
  const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  if (type === "OBJECT") {
    return {
      id,
      name: "",
      type,
      nullable: false,
      valueType: "STRING",
      generatorType: "STRING",
      generatorConfigJson: "{}",
      children: [],
      itemSchema: null,
      minItems: null,
      maxItems: null
    };
  }

  if (type === "ARRAY") {
    return {
      id,
      name: "",
      type,
      nullable: false,
      valueType: "STRING",
      generatorType: "STRING",
      generatorConfigJson: "{}",
      children: [],
      itemSchema: null,
      minItems: 1,
      maxItems: 3
    };
  }

  return {
    id,
    name: "",
    type,
    nullable: false,
    valueType: "STRING",
    generatorType: "STRING",
    generatorConfigJson: defaultGeneratorConfig("STRING"),
    children: [],
    itemSchema: null,
    minItems: null,
    maxItems: null
  };
}

function addChildToSelected(type: PayloadNodeType) {
  if (selectedNode.value.type !== "OBJECT") {
    return;
  }
  const child = createNode(type);
  selectedNode.value.children.push(child);
  selectNode(child.id);
}

function setSelectedItemSchema(type: PayloadNodeType) {
  if (selectedNode.value.type !== "ARRAY") {
    return;
  }
  const item = createNode(type);
  selectedNode.value.itemSchema = item;
  selectNode(item.id);
}

function removeSelectedNode() {
  if (isSelectedRoot.value) {
    return;
  }
  const parent = findParentNode(props.node, selectedNode.value.id);
  if (!parent) {
    return;
  }
  removeNodeById(props.node, selectedNode.value.id);
  selectNode(parent.id);
}

function handleSelectedTypeChange() {
  resetNodeByType(selectedNode.value, selectedNode.value.type);
  advancedConfigOpen.value = false;
  stringCharsetPreset.value = null;
}

function resetNodeByType(node: PayloadSchemaNodeDraft, type: PayloadNodeType) {
  if (type === "OBJECT") {
    node.valueType = "STRING";
    node.generatorType = "STRING";
    node.generatorConfigJson = "{}";
    node.itemSchema = null;
    node.minItems = null;
    node.maxItems = null;
    node.children = [];
    return;
  }

  if (type === "ARRAY") {
    node.valueType = "STRING";
    node.generatorType = "STRING";
    node.generatorConfigJson = "{}";
    node.children = [];
    node.minItems = 1;
    node.maxItems = 3;
    node.itemSchema = null;
    return;
  }

  node.children = [];
  node.itemSchema = null;
  node.minItems = null;
  node.maxItems = null;
  node.valueType = node.valueType || "STRING";
  node.generatorType = node.generatorType || "STRING";
  node.generatorConfigJson = defaultGeneratorConfig(node.generatorType);
}

function isFieldHighlighted(field: string) {
  return props.activeSelector === `${currentNodeSelector.value} [data-node-field="${field}"]`;
}

function isActionHighlighted(action: string) {
  return props.activeSelector === `${currentNodeSelector.value} [data-node-action="${action}"]`;
}

function isSectionHighlighted(section: string) {
  return props.activeSelector === `${currentNodeSelector.value} [data-node-section="${section}"]`;
}

function handleAdvancedToggle(event: Event) {
  advancedConfigOpen.value = (event.currentTarget as HTMLDetailsElement).open;
}

function readGeneratorConfig() {
  try {
    return selectedNode.value.generatorConfigJson ? JSON.parse(selectedNode.value.generatorConfigJson) as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function writeGeneratorConfig(config: Record<string, unknown>) {
  selectedNode.value.generatorConfigJson = JSON.stringify(config, null, 2);
}

function defaultRandomCharset() {
  return randomStringCharsetOptions[0].charset;
}

function readGeneratorText(key: string, fallback = "") {
  const value = readGeneratorConfig()[key];
  return typeof value === "string" && value.trim() ? value : fallback;
}

function readGeneratorNumber(key: string, fallback: number) {
  const value = readGeneratorConfig()[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function readGeneratorValues() {
  const values = readGeneratorConfig().values;
  if (!Array.isArray(values)) {
    return "";
  }
  return values.map((value) => String(value)).join("\n");
}

function readGeneratorMode() {
  return readGeneratorText("mode", "random");
}

function readGeneratorCharset() {
  return readGeneratorText("charset", defaultRandomCharset());
}

function readGeneratorCharsetPreset(): RandomStringCharsetPreset {
  if (stringCharsetPreset.value === "CUSTOM") {
    return "CUSTOM";
  }
  const matchedOption = randomStringCharsetOptions.find((option) => option.charset === readGeneratorCharset());
  if (matchedOption) {
    return matchedOption.value;
  }
  return stringCharsetPreset.value ?? "CUSTOM";
}

function readGeneratorDateTime(key: string, fallback: string) {
  const value = readGeneratorText(key, fallback);
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return fallback;
  }
  return formatDateTimeLocal(parsed);
}

function updateGeneratorConfig(patch: Record<string, unknown>) {
  writeGeneratorConfig({ ...readGeneratorConfig(), ...patch });
}

function updateGeneratorText(key: string, event: Event) {
  if (key === "charset") {
    stringCharsetPreset.value = "CUSTOM";
  }
  updateGeneratorConfig({ [key]: (event.target as HTMLInputElement | HTMLTextAreaElement).value });
}

function updateGeneratorNumber(key: string, event: Event) {
  const rawValue = (event.target as HTMLInputElement).value;
  updateGeneratorConfig({ [key]: rawValue === "" ? null : Number(rawValue) });
}

function updateGeneratorValues(event: Event) {
  const value = (event.target as HTMLTextAreaElement).value;
  updateGeneratorConfig({
    values: value
      .split(/[\n,，]+/)
      .map((item) => item.trim())
      .filter(Boolean)
  });
}

function updateGeneratorMode(event: Event) {
  const mode = (event.target as HTMLSelectElement).value;
  if (mode === "email") {
    writeGeneratorConfig({
      mode: "email",
      prefix: readGeneratorText("prefix"),
      suffix: readGeneratorText("suffix"),
      domain: readGeneratorText("domain", "demo.local")
    });
    return;
  }

  writeGeneratorConfig({
    mode: "random",
    prefix: readGeneratorText("prefix"),
    suffix: readGeneratorText("suffix"),
    length: readGeneratorNumber("length", 12),
    charset: readGeneratorCharset()
  });
}

function updateGeneratorCharsetPreset(event: Event) {
  const preset = (event.target as HTMLSelectElement).value as RandomStringCharsetPreset;
  const matchedOption = randomStringCharsetOptions.find((option) => option.value === preset);
  if (matchedOption) {
    stringCharsetPreset.value = matchedOption.value;
    updateGeneratorConfig({ charset: matchedOption.charset });
    return;
  }
  stringCharsetPreset.value = "CUSTOM";
}

function defaultGeneratorConfig(type: GeneratorType) {
  switch (type) {
    case "SEQUENCE":
      return JSON.stringify({ start: 1, step: 1 }, null, 2);
    case "RANDOM_INT":
      return JSON.stringify({ min: 0, max: 1000 }, null, 2);
    case "RANDOM_DECIMAL":
      return JSON.stringify({ min: 0, max: 1000, scale: 2 }, null, 2);
    case "STRING":
      return JSON.stringify({ mode: "random", length: 12, charset: defaultRandomCharset(), prefix: "", suffix: "" }, null, 2);
    case "ENUM":
      return JSON.stringify({ values: ["A", "B", "C"] }, null, 2);
    case "BOOLEAN":
      return JSON.stringify({ trueRate: 0.5 }, null, 2);
    case "DATETIME":
      return JSON.stringify({ from: defaultGeneratorFrom(), to: defaultGeneratorTo() }, null, 2);
    case "UUID":
      return JSON.stringify({}, null, 2);
  }
}

function resetGeneratorConfig() {
  advancedConfigOpen.value = false;
  stringCharsetPreset.value = null;
  selectedNode.value.generatorConfigJson = defaultGeneratorConfig(selectedNode.value.generatorType);
}

function formatDateTimeLocal(value: Date) {
  const year = value.getFullYear();
  const month = `${value.getMonth() + 1}`.padStart(2, "0");
  const day = `${value.getDate()}`.padStart(2, "0");
  const hours = `${value.getHours()}`.padStart(2, "0");
  const minutes = `${value.getMinutes()}`.padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function defaultGeneratorFrom() {
  const value = new Date();
  value.setDate(value.getDate() - 30);
  return formatDateTimeLocal(value);
}

function defaultGeneratorTo() {
  return formatDateTimeLocal(new Date());
}
</script>

<style scoped>
.payload-workbench {
  display: grid;
  grid-template-columns: minmax(280px, 320px) minmax(0, 1fr);
  gap: 18px;
  align-items: start;
}

.payload-workbench__tree,
.payload-workbench__inspector {
  min-width: 0;
}

.payload-workbench__tree {
  display: grid;
  gap: 16px;
  padding: 18px;
  border: 1px solid rgba(48, 69, 54, 0.1);
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.9);
  position: sticky;
  top: 12px;
}

.payload-workbench__tree-header {
  display: grid;
  gap: 12px;
}

.payload-workbench__eyebrow {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 6px;
}

.payload-workbench__tree-header h4 {
  margin: 0 0 6px;
}

.payload-workbench__tree-header p {
  margin: 0;
  color: var(--muted);
  line-height: 1.6;
}

.payload-workbench__tree-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.payload-workbench__path-summary {
  display: grid;
  gap: 12px;
  padding: 14px;
  border-radius: 16px;
  border: 1px solid rgba(48, 69, 54, 0.1);
  background: rgba(247, 248, 244, 0.8);
}

.payload-workbench__path-summary-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.payload-workbench__path-summary-header h5,
.payload-workbench__path-summary-header p {
  margin: 0;
}

.payload-workbench__path-summary-header p {
  margin-top: 4px;
  color: var(--muted);
  line-height: 1.5;
}

.payload-workbench__path-list {
  display: grid;
  gap: 8px;
  max-height: 220px;
  overflow: auto;
}

.payload-workbench__path-item {
  display: grid;
  gap: 4px;
  width: 100%;
  padding: 10px 12px;
  border: 1px solid rgba(48, 69, 54, 0.08);
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.82);
  color: var(--ink);
  text-align: left;
}

.payload-workbench__path-item strong,
.payload-workbench__path-item span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.payload-workbench__path-item span {
  color: var(--muted);
  font-size: 13px;
}

.payload-workbench__path-item--active {
  border-color: rgba(47, 125, 87, 0.26);
  background: rgba(47, 125, 87, 0.08);
}

.payload-workbench__tree-list {
  display: grid;
  gap: 8px;
  max-height: 640px;
  overflow: auto;
  padding-right: 4px;
}

.payload-workbench__tree-item {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  width: 100%;
  padding: 12px 14px 12px calc(14px + var(--tree-depth, 0) * 14px);
  border: 1px solid rgba(48, 69, 54, 0.08);
  border-radius: 14px;
  background: rgba(247, 248, 244, 0.72);
  color: var(--ink);
  text-align: left;
}

.payload-workbench__tree-item--active {
  border-color: rgba(47, 125, 87, 0.28);
  background: rgba(47, 125, 87, 0.1);
  box-shadow: inset 0 0 0 1px rgba(47, 125, 87, 0.08);
}

.payload-workbench__tree-item-main {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.payload-workbench__tree-item-label {
  font-weight: 700;
}

.payload-workbench__tree-item-summary {
  color: var(--muted);
  font-size: 13px;
  line-height: 1.5;
}

.payload-node {
  display: grid;
  gap: 18px;
  padding: 20px;
  border: 1px solid rgba(15, 23, 42, 0.08);
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.96);
}

.payload-node__header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}

.payload-node__identity {
  display: grid;
  gap: 8px;
  min-width: 0;
}

.payload-node__eyebrow {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.payload-node__identity h4,
.payload-node__section-header h5 {
  margin: 0;
}

.payload-node__summary,
.payload-node__section-header p,
.payload-node__empty {
  margin: 0;
  color: var(--muted);
  line-height: 1.6;
}

.payload-node__body {
  display: grid;
  gap: 16px;
}

.payload-node__grid {
  gap: 14px;
}

.payload-node__toggles {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.payload-node__section {
  display: grid;
  gap: 14px;
  padding: 18px;
  border-radius: 18px;
  border: 1px solid rgba(48, 69, 54, 0.1);
  background: rgba(247, 248, 244, 0.84);
}

.payload-node__section-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 14px;
  flex-wrap: wrap;
}

.payload-node__section-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.payload-node__linked-list {
  display: grid;
  gap: 10px;
}

.payload-node__linked-item {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  width: 100%;
  padding: 14px 16px;
  border: 1px solid rgba(48, 69, 54, 0.1);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.82);
  color: var(--ink);
  text-align: left;
}

.payload-node__linked-item--active {
  border-color: rgba(47, 125, 87, 0.26);
  background: rgba(47, 125, 87, 0.08);
}

.payload-node__linked-item strong,
.payload-node__linked-item p {
  margin: 0;
}

.payload-node__linked-item p {
  margin-top: 4px;
  color: var(--muted);
  line-height: 1.5;
}

.payload-node__empty {
  padding: 14px 16px;
  border-radius: 14px;
  border: 1px dashed rgba(48, 69, 54, 0.18);
  background: rgba(255, 255, 255, 0.76);
}

.payload-node__config-card {
  display: grid;
  gap: 16px;
  padding: 18px;
  border-radius: 18px;
  border: 1px solid rgba(48, 69, 54, 0.12);
  background: rgba(247, 248, 244, 0.88);
}

.payload-node__config-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 14px;
  flex-wrap: wrap;
}

.payload-node__config-header h5 {
  margin: 0 0 6px;
}

.payload-node__config-header p {
  margin: 0;
  color: var(--muted);
  line-height: 1.6;
}

.payload-node__config-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.payload-node__config-note {
  display: grid;
  gap: 6px;
  padding: 14px 16px;
  border-radius: 14px;
  border: 1px dashed rgba(48, 69, 54, 0.18);
  background: rgba(255, 255, 255, 0.8);
}

.payload-node__config-note strong,
.payload-node__config-note p {
  margin: 0;
}

.payload-node__config-note p {
  color: var(--muted);
  line-height: 1.6;
}

.payload-node__details {
  margin: 0;
}

.payload-node__details-summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.payload-node__details-summary small {
  color: var(--muted);
  font-weight: 600;
}

.payload-node__details-content {
  display: grid;
  gap: 12px;
}

.payload-node__details-hint {
  margin: 0;
  color: var(--muted);
  line-height: 1.6;
}

.payload-node__code {
  width: 100%;
  min-width: 0;
  min-height: 180px;
  padding: 14px 16px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.9);
  box-sizing: border-box;
  resize: vertical;
  font-family: "Consolas", "SFMono-Regular", monospace;
  font-size: 13px;
  line-height: 1.55;
}

@media (max-width: 1100px) {
  .payload-workbench {
    grid-template-columns: 1fr;
  }

  .payload-workbench__tree {
    position: static;
  }
}

@media (max-width: 720px) {
  .payload-workbench__tree,
  .payload-node {
    padding: 16px;
    border-radius: 18px;
  }

  .payload-node__section {
    padding: 14px;
  }

  .payload-node__details-summary {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
