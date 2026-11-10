{{- define "infra.labels" -}}
app.kubernetes.io/name: agentic-rag-infrastructure
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
runai/queue: {{ required "Set runai.projectName to the registered project" .Values.runai.projectName | quote }}
run.ai/category: Deploy
{{- if .Values.runai.projectId }}
run.ai/project: {{ .Values.runai.projectId | quote }}
{{- end }}
{{- end -}}
