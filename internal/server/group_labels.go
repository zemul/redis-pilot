package server

import (
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

const maxGroupLabels = 20

func normalizeGroupLabels(labels map[string]string) (map[string]string, error) {
	if len(labels) > maxGroupLabels {
		return nil, fmt.Errorf("labels must not contain more than %d entries", maxGroupLabels)
	}
	normalized := make(map[string]string, len(labels))
	for rawKey, rawValue := range labels {
		key := strings.TrimSpace(rawKey)
		value := strings.TrimSpace(rawValue)
		if key == "" || len(key) > 63 {
			return nil, fmt.Errorf("label key must be 1-63 characters")
		}
		for _, r := range key {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
				continue
			}
			return nil, fmt.Errorf("invalid label key %q", key)
		}
		if len([]rune(value)) > 128 {
			return nil, fmt.Errorf("label value for %q must not exceed 128 characters", key)
		}
		for _, r := range value {
			if unicode.IsControl(r) {
				return nil, fmt.Errorf("label value for %q contains control characters", key)
			}
		}
		if _, exists := normalized[key]; exists {
			return nil, fmt.Errorf("duplicate label key %q", key)
		}
		normalized[key] = value
	}
	return normalized, nil
}

func parseLabelSelector(values []string) (map[string]string, error) {
	selector := make(map[string]string, len(values))
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("label filter must use key=value")
		}
		normalized, err := normalizeGroupLabels(map[string]string{key: value})
		if err != nil {
			return nil, err
		}
		for k, v := range normalized {
			selector[k] = v
		}
	}
	return selector, nil
}

func groupMatchesLabels(group *apitypes.InstanceGroupState, selector map[string]string) bool {
	if len(selector) == 0 {
		return true
	}
	if group == nil {
		return false
	}
	for key, value := range selector {
		if group.Labels[key] != value {
			return false
		}
	}
	return true
}

func cloneLabels(labels map[string]string) map[string]string {
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func (s *Server) instanceGroupLabels(c *gin.Context) {
	var req struct {
		Group  string             `json:"group" binding:"required"`
		Labels *map[string]string `json:"labels" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	groupName := strings.TrimSpace(req.Group)
	labels, err := normalizeGroupLabels(*req.Labels)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	var before map[string]string
	found := false
	err = s.db.WithInstances(func(instances *apitypes.InstancesState) error {
		group := instances.Groups[groupName]
		if group == nil {
			return nil
		}
		found = true
		before = cloneLabels(group.Labels)
		group.Labels = labels
		group.UpdatedAt = time.Now().Format(time.RFC3339)
		return nil
	})
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		fail(c, http.StatusNotFound, "instance group not found: "+groupName)
		return
	}

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c), Action: "instance_group.labels.update",
		Level: audit.LevelNormal, Result: "success", ObjectType: "instance_group", ObjectID: groupName, GroupName: groupName,
		Target: map[string]interface{}{"group": groupName}, Before: map[string]interface{}{"labels": before}, After: map[string]interface{}{"labels": labels},
	})
	ok(c, gin.H{"group": groupName, "labels": labels})
}
