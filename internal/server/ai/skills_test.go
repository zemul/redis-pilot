package ai

import (
	"strings"
	"testing"
)

func TestParseSkill(t *testing.T) {
	content := "---\nname: redis-diagnose\ndescription: 诊断 Redis 实例性能问题\n---\n# 诊断流程\n1. 采集 INFO\n"
	sk := parseSkill(content)
	if sk.Name != "redis-diagnose" {
		t.Errorf("name = %q, want redis-diagnose", sk.Name)
	}
	if sk.Description != "诊断 Redis 实例性能问题" {
		t.Errorf("description = %q", sk.Description)
	}
	if !strings.Contains(sk.Body, "诊断流程") {
		t.Errorf("body missing markdown content: %q", sk.Body)
	}
	if strings.Contains(sk.Body, "name:") {
		t.Errorf("body should not contain frontmatter: %q", sk.Body)
	}
}

func TestParseSkillNoFrontmatter(t *testing.T) {
	sk := parseSkill("# 纯正文没有 frontmatter")
	if sk.Body != "# 纯正文没有 frontmatter" {
		t.Errorf("body = %q", sk.Body)
	}
}

func TestLoadSkillsEmbedded(t *testing.T) {
	skills, err := LoadSkills("")
	if err != nil {
		t.Fatalf("LoadSkills: %v", err)
	}
	if len(skills) < 7 {
		t.Fatalf("expected >=7 embedded skills, got %d", len(skills))
	}
	for _, sk := range skills {
		if sk.Name == "" {
			t.Errorf("skill has empty name")
		}
		if sk.Body == "" {
			t.Errorf("skill %q has empty body", sk.Name)
		}
	}
	t.Logf("loaded %d skills", len(skills))
}
