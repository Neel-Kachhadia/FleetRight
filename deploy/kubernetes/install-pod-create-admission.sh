#!/usr/bin/env bash
set -euo pipefail

context=${FLEETRIGHT_LAB_CONTEXT:-kind-fleetright-safety-lab}
namespace=${FLEETRIGHT_LAB_NAMESPACE:-fleetright-v2}
kubeconfig=${FLEETRIGHT_LAB_ADMIN_KUBECONFIG:-/home/neel/.cache/fleetright/safety-lab.kubeconfig}
kind_cluster=${FLEETRIGHT_KIND_CLUSTER:-fleetright-safety-lab}
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/fleetright-pod-admission.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

if [[ "$(kubectl --kubeconfig "$kubeconfig" config current-context)" != "$context" ]]; then
  echo "refusing to install: current kubectl context is not $context" >&2
  exit 1
fi

docker build -t fleetright-workload-controller:local \
  -f "$repo_root/deploy/kubernetes/workload-controller.Dockerfile" "$repo_root"
kind load docker-image --name "$kind_cluster" fleetright-workload-controller:local

if ! kubectl --kubeconfig "$kubeconfig" --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
  kubectl --kubeconfig "$kubeconfig" --context "$context" create namespace "$namespace"
fi
kubectl --kubeconfig "$kubeconfig" --context "$context" label --overwrite namespace "$namespace" \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/audit=restricted \
  pod-security.kubernetes.io/warn=restricted
kubectl --kubeconfig "$kubeconfig" --context "$context" apply -f "$repo_root/deploy/kubernetes/fleetrightworkload-crd.yaml"

if kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" get secret fleetright-pod-admission-tls >/dev/null 2>&1; then
  kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" get secret fleetright-pod-admission-tls \
    -o jsonpath='{.data.tls\.crt}' | base64 -d >"$tmp_dir/tls.crt"
  kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" get secret fleetright-pod-admission-tls \
    -o jsonpath='{.data.tls\.key}' | base64 -d >"$tmp_dir/tls.key"
else
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
    -keyout "$tmp_dir/tls.key" -out "$tmp_dir/tls.crt" \
    -subj "/CN=fleetright-pod-admission.$namespace.svc" \
    -addext "subjectAltName=DNS:fleetright-pod-admission.$namespace.svc,DNS:fleetright-pod-admission.$namespace.svc.cluster.local" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign" \
    -addext "extendedKeyUsage=serverAuth"
fi
chmod 0600 "$tmp_dir/tls.key"
kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" create secret tls fleetright-pod-admission-tls \
  --cert="$tmp_dir/tls.crt" --key="$tmp_dir/tls.key" \
  --dry-run=client -o yaml | kubectl --kubeconfig "$kubeconfig" --context "$context" apply -f -

kubectl --kubeconfig "$kubeconfig" --context "$context" apply \
  -f "$repo_root/deploy/kubernetes/fleetright-pod-admission-webhook.yaml"
kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" rollout restart deployment/fleetright-pod-admission
kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" rollout status \
  deployment/fleetright-pod-admission --timeout=180s

ca_bundle="$(base64 <"$tmp_dir/tls.crt" | tr -d '\n')"
sed "s|__CA_BUNDLE__|$ca_bundle|g" \
  "$repo_root/deploy/kubernetes/fleetright-pod-admission-configuration.yaml" \
  | kubectl --kubeconfig "$kubeconfig" --context "$context" apply -f -

kubectl --kubeconfig "$kubeconfig" --context "$context" apply -f "$repo_root/deploy/kubernetes/fleetright-workload-controller.yaml"
kubectl --kubeconfig "$kubeconfig" --context "$context" apply -f "$repo_root/deploy/kubernetes/rbac.yaml"
kubectl --kubeconfig "$kubeconfig" --context "$context" apply -f "$repo_root/deploy/kubernetes/node-mutation-policy.yaml"
kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" rollout restart deployment/fleetright-workload-controller
kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" rollout status \
  deployment/fleetright-workload-controller --timeout=180s

policy="$(kubectl --kubeconfig "$kubeconfig" --context "$context" get validatingwebhookconfiguration fleetright-pod-admission \
  -o jsonpath='{.webhooks[0].failurePolicy}')"
if [[ "$policy" != Fail ]]; then
  echo "Pod CREATE admission is not fail closed" >&2
  exit 1
fi
echo "Pod CREATE admission installed and fail closed in $context/$namespace"
