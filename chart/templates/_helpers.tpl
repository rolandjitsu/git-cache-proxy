{{/*
Chart name, overridable via nameOverride.
*/}}
{{- define "git-cache-proxy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Fully qualified app name. Truncated at 63 chars for the Kubernetes name limit.
Used as the resource name and as the stable `app` selector label - do not change
its shape, since editing a Deployment selector is a breaking in-place upgrade.
*/}}
{{- define "git-cache-proxy.fullname" -}}
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
