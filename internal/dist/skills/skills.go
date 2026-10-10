// Package skills reads the skills directory at boot and merges the skills into
// the one manifest that every process gets.
//
// When several skills grant the same syscall, their "capabilities" lists are
// joined and every other key of the grant must be identical.
package skills

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

type Skill struct {
	// Name is the file name without ".json".
	Name         string            `json:"-"`
	Description  string            `json:"description"`
	Instructions string            `json:"instructions"`
	Syscalls     []json.RawMessage `json:"syscalls"`
}

const capabilitiesKey = "capabilities"

// Load reads every *.json file in dir, in file-name order. An unknown key is an
// error, so a misspelled field cannot drop a restriction unnoticed.
func Load(dir string) ([]Skill, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("scan skills directory: %w", err)
	}
	var skills []Skill
	for _, entry := range entries {
		name, isSkill := strings.CutSuffix(entry.Name(), ".json")
		if entry.IsDir() || !isSkill {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read skill %s: %w", name, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		skill := Skill{Name: name}
		if err := decoder.Decode(&skill); err != nil {
			return nil, fmt.Errorf("decode skill %s: %w", name, err)
		}
		skills = append(skills, skill)
	}
	return skills, nil
}

// Merge builds the manifest for program. Grants keep the order in which their
// syscall first appears.
func Merge(program string, skills []Skill) (aurora.Manifest, error) {
	manifest := aurora.Manifest{Version: aurora.ManifestVersion, Program: program}
	grants := map[string]map[string]any{}
	owners := map[string]string{}
	var order []string
	for _, skill := range skills {
		manifest.Skills = append(manifest.Skills, aurora.Skill{
			Name:         skill.Name,
			Description:  skill.Description,
			Instructions: skill.Instructions,
		})
		granted := map[string]bool{}
		for i, raw := range skill.Syscalls {
			grant, name, err := decodeGrant(raw)
			if err != nil {
				return aurora.Manifest{}, fmt.Errorf("skill %s: grant %d: %w", skill.Name, i, err)
			}
			switch {
			case strings.HasPrefix(name, "sys."):
				return aurora.Manifest{}, fmt.Errorf("skill %s: %q needs no grant: every process gets sys.*", skill.Name, name)
			case granted[name]:
				return aurora.Manifest{}, fmt.Errorf("skill %s: syscall %q is granted twice", skill.Name, name)
			}
			granted[name] = true

			merged, seen := grants[name]
			if !seen {
				merged = maps.Clone(grant)
				delete(merged, capabilitiesKey)
				grants[name], owners[name] = merged, skill.Name
				order = append(order, name)
			} else if key := differentSetting(merged, grant); key != "" {
				return aurora.Manifest{}, fmt.Errorf("skills %s and %s grant %q with different %q", owners[name], skill.Name, name, key)
			}
			if capabilities, _ := grant[capabilitiesKey].([]any); len(capabilities) > 0 {
				joined, _ := merged[capabilitiesKey].([]any)
				merged[capabilitiesKey] = appendNew(joined, capabilities...)
			}
		}
	}
	for _, name := range order {
		raw, _ := json.Marshal(grants[name])
		var syscall aurora.Syscall
		if err := json.Unmarshal(raw, &syscall); err != nil {
			return aurora.Manifest{}, fmt.Errorf("syscall %q: %w", name, err)
		}
		manifest.Syscalls = append(manifest.Syscalls, syscall)
	}
	return manifest, nil
}

func decodeGrant(raw json.RawMessage) (map[string]any, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var grant map[string]any
	if err := decoder.Decode(&grant); err != nil {
		return nil, "", err
	}
	name, _ := grant["syscall"].(string)
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, "", errors.New("a syscall name is required")
	}
	grant["syscall"] = name
	if capabilities, ok := grant[capabilitiesKey]; ok {
		if _, isList := capabilities.([]any); !isList {
			return nil, "", fmt.Errorf("syscall %q: %q must be a list", name, capabilitiesKey)
		}
	}
	return grant, name, nil
}

// differentSetting returns the first key, other than capabilities, whose value
// differs between two grants, or "" when they agree.
func differentSetting(a, b map[string]any) string {
	keys := slices.Sorted(maps.Keys(a))
	keys = append(keys, slices.Sorted(maps.Keys(b))...)
	for _, key := range keys {
		if key != capabilitiesKey && !reflect.DeepEqual(a[key], b[key]) {
			return key
		}
	}
	return ""
}

func appendNew(list []any, items ...any) []any {
	for _, item := range items {
		if !slices.ContainsFunc(list, func(have any) bool { return reflect.DeepEqual(have, item) }) {
			list = append(list, item)
		}
	}
	return list
}
