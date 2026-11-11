{{- define "rag.labels" -}}
app.kubernetes.io/name: agentic-rag
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
runai/queue: {{ required "Set runai.projectName to the registered Run:ai project" .Values.runai.projectName | quote }}
run.ai/category: Deploy
{{- if .Values.runai.projectId }}
run.ai/project: {{ .Values.runai.projectId | quote }}
{{- end }}
{{- end -}}
{{- define "rag.bindings" -}}
{{- $v := deepCopy .Values.bindings -}}
{{- $_ := set $v "APPLICATION_IMAGE" .Values.application.image -}}
{{- $_ := set $v "NAMESPACE" .Release.Namespace -}}
{{- $_ := set $v "RUNTIME_SECRET" .Values.secrets.runtime -}}
{{- $_ := set $v "ADMIN_SECRET" .Values.secrets.admin -}}
{{- $_ := set $v "ES_CA_SECRET" .Values.secrets.elasticsearchCA -}}
{{- $_ := set $v "ARTIFACT_PVC" .Values.storage.artifacts.claim -}}
{{- $_ := set $v "SOURCE_PVC" .Values.storage.source.claim -}}
{{- $_ := set $v "CHECKPOINT_PVC" .Values.storage.checkpoint.claim -}}
{{- $_ := set $v "CONFIG_PVC" .Values.storage.configuration.claim -}}
{{- $_ := set $v "PRIVATE_RUNTIME_SECRET" .Values.privateAnalysis.runtimeSecret -}}
{{- $_ := set $v "PRIVATE_SCOPE_SECRET" .Values.privateAnalysis.scopeSecret -}}
{{- $_ := set $v "PRIVATE_ARTIFACT_PVC" .Values.privateAnalysis.artifactClaim -}}
{{- $_ := set $v "PRIVATE_QUERY_MCP_URL" (printf "http://%s-analysis-query-mcp.%s.svc.cluster.local:8012/mcp" .Release.Name .Release.Namespace) -}}
{{- $checkpoint := trimPrefix "sha256:" (toString (get $v "EMBED_CHECKPOINT_SHA256")) -}}
{{- $_ := set $v "EMBED_MODEL_DIGEST" (printf "sha256:%s" $checkpoint) -}}
{{- $_ := set $v "INDEX_MODEL_DIGEST" (printf "sha256:%s" $checkpoint) -}}
{{- $embedding := dict "model" "nvidia/Nemotron-3.5-Embed-1B-NVFP4" "checkpoint" $checkpoint "dimensions" 2048 "modality" "text" "input_type" "passage" "truncate" "NONE" "library" "26.8.2" -}}
{{- $_ := set $v "EMBED_CONFIG_DIGEST" (printf "sha256:%s" (toJson $embedding | sha256sum)) -}}
{{- $index := dict "dimensions" 2048 "dtype" "float32" "normalization" "unit" "metric" "IP" "schema_version" 1 "embedding_checkpoint_sha256" $checkpoint -}}
{{- $_ := set $v "INDEX_CONFIG_DIGEST" (printf "sha256:%s" (toJson $index | sha256sum)) -}}
{{- $_ := set $v "CAPTION_MODEL_DIGEST" (printf "sha256:%s" (trimPrefix "sha256:" (toString (get $v "CAPTION_MODEL_DIGEST")))) -}}
{{- range $key,$url := dict "EMBED_URL" "EMBED_ENDPOINT" "CAPTION_URL" "CAPTION_ENDPOINT" "PAGE_ELEMENTS_URL" "PAGE_ELEMENTS_ENDPOINT" "TABLE_STRUCTURE_URL" "TABLE_STRUCTURE_ENDPOINT" "OCR_URL" "OCR_ENDPOINT" -}}
{{- $_ := set $v $key (get $v $url) -}}
{{- end -}}
{{- toJson $v -}}
{{- end -}}
{{- define "rag.resolve" -}}
{{- $text := .text -}}
{{- range $key,$value := .bindings -}}
{{- $escaped := trimSuffix "\"" (trimPrefix "\"" (toJson (toString $value))) -}}
{{- $text = replace (printf "${%s}" $key) $escaped $text -}}
{{- end -}}
{{- if regexMatch "\\$\\{[A-Z0-9_]+\\}|REPLACE_[A-Z0-9_]+" $text -}}
{{- fail "Complete the private YAML values: unresolved configuration placeholder" -}}
{{- end -}}
{{- $text -}}
{{- end -}}
