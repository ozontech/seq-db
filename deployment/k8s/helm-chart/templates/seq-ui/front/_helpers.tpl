{{/*
Expand the name of the chart.
*/}}
{{- define "seq-ui-front.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}-sequi-front
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "seq-ui-front.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}-sequi-front
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}-sequi-front
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "seq-ui-front.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "seq-ui-front.labels" -}}
helm.sh/chart: {{ include "seq-ui-front.chart" . }}
{{ include "seq-ui-front.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "seq-ui-front.selectorLabels" -}}
app.kubernetes.io/name: {{ include "seq-ui-front.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "seq-ui-front.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "seq-ui-front.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "seq-ui.url" -}}
  {{- if .Values.sequi.ingress.enabled -}}
    {{- $protocol := "http" -}}
    {{- if .Values.sequi.ingress.tls -}}
      {{- $protocol = "https" -}}
    {{- end -}}
    {{- $host := (index .Values.sequi.ingress.hosts 0).host -}}
    {{- $path := (index (index .Values.sequi.ingress.hosts 0).paths 0).path -}}
    {{- printf "%s://%s%s" $protocol $host $path -}}
  {{- else -}}
    {{- .Values.sequi.front.SEQ_UI_URL -}}
  {{- end -}}
{{- end -}}
