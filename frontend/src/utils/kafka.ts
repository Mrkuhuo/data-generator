export type KafkaSchemaSource = "EXAMPLE_JSON" | "JSON_SCHEMA";
export type KafkaHeaderMode = "FIXED" | "FIELD";

export interface KafkaSchemaImportWarning {
  path: string;
  code: string;
  message: string;
}

export interface KafkaSchemaImportResult {
  schemaSource: KafkaSchemaSource;
  payloadSchemaJson: string;
  scalarPaths: string[];
  warnings: KafkaSchemaImportWarning[];
}

export interface KafkaHeaderEntryDraft {
  name: string;
  mode: KafkaHeaderMode;
  value: string;
  path: string;
}
