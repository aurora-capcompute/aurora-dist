package skills

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func skill(name string, grants ...string) Skill {
	s := Skill{Name: name}
	for _, grant := range grants {
		s.Syscalls = append(s.Syscalls, json.RawMessage(grant))
	}
	return s
}

func TestMergeJoinsCapabilitiesOfTheSameSyscall(t *testing.T) {
	manifest, err := Merge("agent", []Skill{
		skill("status", `{"syscall":"core.internet","timeout_ms":5000,"capabilities":[{"methods":["GET"],"domain":"status.example.com"}]}`),
		skill("docs",
			`{"syscall":"core.internet","timeout_ms":5000,"capabilities":[{"domain":"docs.example.com","methods":["GET"]},{"domain":"status.example.com","methods":["GET"]}]}`,
			`{"syscall":"core.scratch","capabilities":[{"operation":"get"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Syscalls) != 2 || manifest.Syscalls[0].Syscall != "core.internet" || manifest.Syscalls[1].Syscall != "core.scratch" {
		t.Fatalf("syscalls = %+v, want core.internet then core.scratch", manifest.Syscalls)
	}
	want := `{"timeout_ms":5000,"capabilities":[{"methods":["GET"],"domain":"status.example.com"},{"methods":["GET"],"domain":"docs.example.com"}]}`
	if !jsonEqual(t, manifest.Syscalls[0].Config, want) {
		t.Fatalf("merged config = %s, want %s", manifest.Syscalls[0].Config, want)
	}
}

func jsonEqual(t *testing.T, got json.RawMessage, want string) bool {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(got, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &b); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(a, b)
}

func TestMergeRefusesDifferentSettings(t *testing.T) {
	for name, second := range map[string]string{
		"value":   `{"syscall":"core.internet","timeout_ms":9000}`,
		"missing": `{"syscall":"core.internet"}`,
		"hidden":  `{"syscall":"core.internet","timeout_ms":5000,"hidden":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Merge("agent", []Skill{
				skill("a", `{"syscall":"core.internet","timeout_ms":5000}`),
				skill("b", second),
			})
			if err == nil || !strings.Contains(err.Error(), "skills a and b") {
				t.Fatalf("err = %v, want a conflict that names both skills", err)
			}
		})
	}
}

func TestMergeRefusesASyscallGrantedTwiceInOneSkill(t *testing.T) {
	_, err := Merge("agent", []Skill{
		skill("a", `{"syscall":"core.scratch"}`, `{"syscall":"core.scratch"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "granted twice") {
		t.Fatalf("err = %v, want a duplicate grant error", err)
	}
}

func TestMergeRefusesAGrantWithoutASyscall(t *testing.T) {
	_, err := Merge("agent", []Skill{skill("a", `{"capabilities":[]}`)})
	if err == nil {
		t.Fatal("a grant with no syscall name merged")
	}
}

func TestLoadReadsSkillsInNameOrder(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("b.json", `{"description":"second","instructions":"Do it this way.","syscalls":[{"syscall":"core.scratch"}]}`)
	write("a.json", `{"description":"first","syscalls":[]}`)
	write("notes.md", `not a skill`)

	loaded, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].Name != "a" || loaded[1].Name != "b" ||
		loaded[1].Description != "second" || loaded[1].Instructions != "Do it this way." {
		t.Fatalf("loaded = %+v", loaded)
	}
}

func TestLoadRefusesAnUnknownKey(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "typo.json"), []byte(`{"syscall":[{"syscall":"core.internet"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "typo") {
		t.Fatalf("err = %v, want the skill named", err)
	}
}

func TestMergeKeepsEverySkillsText(t *testing.T) {
	a := skill("a")
	a.Description, a.Instructions = "first", "Check the status page."
	b := skill("b", `{"syscall":"core.scratch"}`)
	b.Description = "second"
	manifest, err := Merge("agent", []Skill{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Skills) != 2 || manifest.Skills[0].Name != "a" || manifest.Skills[0].Instructions != "Check the status page." ||
		manifest.Skills[1].Description != "second" {
		t.Fatalf("skills = %+v", manifest.Skills)
	}
}

func TestMergeRefusesASysGrant(t *testing.T) {
	_, err := Merge("agent", []Skill{skill("nap", `{"syscall":"sys.timer"}`)})
	if err == nil || !strings.Contains(err.Error(), "sys.timer") {
		t.Fatalf("err = %v, want a sys.* grant refused", err)
	}
}
