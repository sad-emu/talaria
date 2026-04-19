package connector

import (
    "fmt"
    "os"
    "path/filepath"
    "strings"

    "gopkg.in/yaml.v3"
)

func LoadPoliciesFromDir(dir string) ([]ConnectorPolicy, error) {
    entries, err := os.ReadDir(dir)
    if err != nil {
        return nil, fmt.Errorf("connector policy: read dir %q: %w", dir, err)
    }

    var out []ConnectorPolicy
    for _, e := range entries {
        if e.IsDir() {
            continue
        }
        name := strings.ToLower(e.Name())
        if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
            continue
        }

        path := filepath.Join(dir, e.Name())
        b, err := os.ReadFile(path)
        if err != nil {
            return nil, fmt.Errorf("connector policy: read %q: %w", path, err)
        }

        var p ConnectorPolicy
        if err := yaml.Unmarshal(b, &p); err != nil {
            return nil, fmt.Errorf("connector policy: parse %q: %w", path, err)
        }
        if strings.TrimSpace(p.Name) == "" {
            base := strings.TrimSuffix(e.Name(), filepath.Ext(e.Name()))
            p.Name = base
        }
        // default enabled
        if !p.Enabled {
            p.Enabled = true
        }
        out = append(out, p)
    }

    return out, nil
}