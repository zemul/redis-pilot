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

func normalizeGroupLabels(labels []string) ([]string, error) {
	if len(labels) > maxGroupLabels {
		return nil, fmt.Errorf("labels must not contain more than %d entries", maxGroupLabels)
	}
	normalized := make([]string, 0, len(labels))
	seen := make(map[string]struct{}, len(labels))
	for _, raw := range labels {
		label := strings.TrimSpace(raw)
		if label == "" || len([]rune(label)) > 64 {
			return nil, fmt.Errorf("label must be 1-64 characters")
		}
		for _, r := range label {
			if unicode.IsControl(r) || r == ',' {
				return nil, fmt.Errorf("label %q contains unsupported characters", label)
			}
		}
		if _, exists := seen[label]; exists {
			continue
		}
		seen[label] = struct{}{}
		normalized = append(normalized, label)
	}
	return normalized, nil
}

func parseLabelSelector(values []string) ([]string, error) {
	return normalizeGroupLabels(values)
}

func groupMatchesLabels(group *apitypes.InstanceGroupState, selector []string) bool {
	if len(selector) == 0 {
		return true
	}
	if group == nil {
		return false
	}
	available := make(map[string]struct{}, len(group.Labels))
	for _, label := range group.Labels {
		available[label] = struct{}{}
	}
	for _, label := range selector {
		if _, ok := available[label]; !ok {
			return false
		}
	}
	return true
}

func cloneLabels(labels []string) []string {
	return append([]string(nil), labels...)
}

func (s *Server) instanceGroupLabels(c *gin.Context) {
	var req struct {
		Group  string    `json:"group" binding:"required"`
		Labels *[]string `json:"labels" binding:"required"`
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

	var before []string
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
