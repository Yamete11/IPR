{{/* Chart name doubles as the Deployment/Service name and the in-cluster DNS name. */}}
{{- define "svc.name" -}}
{{ .Chart.Name }}
{{- end -}}

{{- define "svc.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: smartcity
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "svc.selectorLabels" -}}
app: {{ .Chart.Name }}
app.kubernetes.io/name: {{ .Chart.Name }}
{{- end -}}

{{- define "svc.imageTag" -}}
{{ .Values.global.imageTag | default .Values.image.tag }}
{{- end -}}
