package ai

import (
	"embed"
	"io/fs"
	"os"
	"sort"
	"strings"
)

// embeddedSkills 内嵌 skills 副本。构建前由 Makefile 从仓库根 skills/ 同步到
// skills_embed/（go:embed 不能跨目录，见 docs/architecture/ai-assistant.md §3.4）。
//
//go:embed skills_embed
var embeddedSkills embed.FS

// Skill 一个运维技能：YAML frontmatter 的 name/description + markdown 正文。
type Skill struct {
	Name        string
	Description string
	Body        string
}

// LoadSkills 加载所有技能。dir 非空则从磁盘读取（开发热更），否则用内嵌副本。
func LoadSkills(dir string) ([]Skill, error) {
	var fsys fs.FS
	var root string
	if dir != "" {
		fsys = os.DirFS(dir)
		root = "."
	} else {
		fsys = embeddedSkills
		root = "skills_embed"
	}
	entries, err := fs.ReadDir(fsys, root)
	if err != nil {
		return nil, err
	}
	var skills []Skill
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		data, err := fs.ReadFile(fsys, root+"/"+e.Name()+"/SKILL.md")
		if err != nil {
			continue // 子目录无 SKILL.md，跳过
		}
		sk := parseSkill(string(data))
		if sk.Name == "" {
			sk.Name = e.Name()
		}
		skills = append(skills, sk)
	}
	sort.Slice(skills, func(i, j int) bool { return skills[i].Name < skills[j].Name })
	return skills, nil
}

// parseSkill 解析形如 "---\nname: x\ndescription: y\n---\n正文" 的 SKILL.md。
func parseSkill(content string) Skill {
	sk := Skill{Body: content}
	if !strings.HasPrefix(content, "---") {
		return sk
	}
	rest := content[3:]
	end := strings.Index(rest, "\n---")
	if end < 0 {
		return sk
	}
	front := rest[:end]
	sk.Body = strings.TrimLeft(rest[end+4:], "\n")
	for _, line := range strings.Split(front, "\n") {
		key, val, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		val = strings.Trim(strings.TrimSpace(val), `"'`)
		switch strings.TrimSpace(key) {
		case "name":
			sk.Name = val
		case "description":
			sk.Description = val
		}
	}
	return sk
}
