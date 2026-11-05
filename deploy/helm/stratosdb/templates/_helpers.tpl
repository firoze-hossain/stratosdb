{{/*
Expand the name of the chart.
*/}}
{{- define "stratosdb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "stratosdb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Primary service and statefulset name
*/}}
{{- define "stratosdb.primaryFullname" -}}
{{- printf "%s-primary" (include "stratosdb.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Replica service and statefulset name
*/}}
{{- define "stratosdb.replicaFullname" -}}
{{- printf "%s-replica" (include "stratosdb.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Headless service name
*/}}
{{- define "stratosdb.headlessServiceName" -}}
{{- printf "%s-headless" (include "stratosdb.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Watchdog deployment name
*/}}
{{- define "stratosdb.watchdogFullname" -}}
{{- printf "%s-watchdog" (include "stratosdb.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Secret name
*/}}
{{- define "stratosdb.secretName" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecret }}
{{- else }}
{{- printf "%s-auth" (include "stratosdb.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "stratosdb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "stratosdb.labels" -}}
helm.sh/chart: {{ include "stratosdb.chart" . }}
{{ include "stratosdb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "stratosdb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "stratosdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
