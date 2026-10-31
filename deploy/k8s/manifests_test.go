package k8s

import (
	"bytes"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

type object map[string]any

func (o object) kind() string { return str(o["kind"]) }
func (o object) name() string { return str(dig(o, "metadata", "name")) }

func str(v any) string { s, _ := v.(string); return s }

func dig(v any, path ...string) any {
	if o, ok := v.(object); ok {
		v = map[string]any(o)
	}
	for _, p := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[p]
	}
	return v
}

func render(t *testing.T, dir string) []object {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not installed")
	}
	out, err := exec.Command("kubectl", "kustomize", dir).Output()
	require.NoError(t, err)
	dec := yaml.NewDecoder(bytes.NewReader(out))
	var objs []object
	for {
		var raw map[string]any
		if err := dec.Decode(&raw); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		if raw != nil {
			objs = append(objs, object(raw))
		}
	}
	return objs
}

func labelsOf(d object) map[string]any {
	l, _ := dig(d, "spec", "template", "metadata", "labels").(map[string]any)
	return l
}

func selects(sel map[string]any, labels map[string]any) bool {
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return len(sel) > 0
}

// checkOverlay validates cross references; secrets named in external must be supplied by whoever deploys the overlay.
func checkOverlay(t *testing.T, dir string, external ...string) (deployments map[string]object, objs []object) {
	objs = render(t, dir)
	deployments = map[string]object{}
	for _, o := range objs {
		if o.kind() == "Deployment" {
			deployments[o.name()] = o
		}
	}
	has := func(kind, name string) bool {
		for _, o := range objs {
			if o.kind() == kind && o.name() == name {
				return true
			}
		}
		return false
	}

	for n, d := range deployments {
		sel, _ := dig(d, "spec", "selector", "matchLabels").(map[string]any)
		require.True(t, selects(sel, labelsOf(d)), "%s: selector must match pod labels", n)
		require.Equal(t, "notifications", str(dig(d, "metadata", "namespace")), n)
		containers, _ := dig(d, "spec", "template", "spec", "containers").([]any)
		require.NotEmpty(t, containers, n)
		for _, c := range containers {
			from, _ := c.(map[string]any)["envFrom"].([]any)
			for _, e := range from {
				em := e.(map[string]any)
				if ref, ok := em["configMapRef"].(map[string]any); ok {
					require.True(t, has("ConfigMap", str(ref["name"])), "%s: missing ConfigMap %v", n, ref["name"])
				}
				if ref, ok := em["secretRef"].(map[string]any); ok {
					name := str(ref["name"])
					supplied := has("Secret", name)
					for _, e := range external {
						supplied = supplied || e == name
					}
					require.True(t, supplied, "%s: missing Secret %v", n, ref["name"])
				}
			}
		}
	}
	for _, o := range objs {
		switch o.kind() {
		case "Service":
			sel, _ := dig(o, "spec", "selector").(map[string]any)
			found := false
			for _, d := range deployments {
				found = found || selects(sel, labelsOf(d))
			}
			require.True(t, found, "service %s selects no deployment", o.name())
		case "PodDisruptionBudget":
			sel, _ := dig(o, "spec", "selector", "matchLabels").(map[string]any)
			found := false
			for _, d := range deployments {
				found = found || selects(sel, labelsOf(d))
			}
			require.True(t, found, "pdb %s selects no deployment", o.name())
		case "HorizontalPodAutoscaler", "ScaledObject":
			target := str(dig(o, "spec", "scaleTargetRef", "name"))
			require.Contains(t, deployments, target, "%s %s targets a missing deployment", o.kind(), o.name())
		}
	}
	return deployments, objs
}

func TestBaseManifestsAreConsistent(t *testing.T) {
	deployments, objs := checkOverlay(t, "base", "notif-secrets")
	require.Len(t, deployments, 12)

	for _, o := range objs {
		if o.kind() != "ScaledObject" || !strings.HasPrefix(o.name(), "worker-") {
			continue
		}
		channel := strings.TrimPrefix(o.name(), "worker-")
		triggers, _ := dig(o, "spec", "triggers").([]any)
		require.Len(t, triggers, 2, "%s must scale on both priority lanes", o.name())
		for _, tr := range triggers {
			md := dig(tr, "metadata").(map[string]any)
			require.Contains(t, str(md["topic"]), "."+channel+".", o.name())
			require.Contains(t, str(md["consumerGroup"]), "worker-"+channel+"-", o.name())
		}
	}

	for n, d := range deployments {
		pod := dig(d, "spec", "template", "spec").(map[string]any)
		c := pod["containers"].([]any)[0].(map[string]any)
		sc := c["securityContext"].(map[string]any)
		require.Equal(t, false, sc["allowPrivilegeEscalation"], n)
		require.Equal(t, true, sc["readOnlyRootFilesystem"], n)
		require.Equal(t, true, dig(pod, "securityContext", "runAsNonRoot"), n)
		require.NotNil(t, c["readinessProbe"], n)
		require.NotNil(t, c["livenessProbe"], n)
		require.NotNil(t, dig(c, "resources", "limits", "memory"), n)
	}
}

func TestLocalOverlayIsSelfContained(t *testing.T) {
	deployments, objs := checkOverlay(t, "overlays/local")
	for _, o := range objs {
		require.NotEqual(t, "HorizontalPodAutoscaler", o.kind(), "metrics-server is not available on kind")
		require.NotEqual(t, "ScaledObject", o.kind(), "the KEDA operator is not available on kind")
	}
	for _, dep := range []string{"kafka", "dynamodb", "redis", "mailpit", "mockprovider"} {
		require.Contains(t, deployments, dep)
	}
	for n, d := range deployments {
		if dig(d, "spec", "replicas") != nil {
			require.EqualValues(t, 1, dig(d, "spec", "replicas"), n)
		}
		containers, _ := dig(d, "spec", "template", "spec", "containers").([]any)
		for _, c := range containers {
			img := str(c.(map[string]any)["image"])
			require.False(t, strings.HasSuffix(img, ":latest"), "%s: local images must be tagged :dev, got %s", n, img)
		}
	}
}
