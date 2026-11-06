/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package local is local repo of templates
package local

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"functioncore/pkg/cli/template/entity"
	"functioncore/pkg/cli/utils"
)

const (
	repoPath        = "local-repo"
	templateTypeNum = 2
)

// Repository repo config
type Repository struct {
	RepoPath string
}

// Repo git Repository
func Repo() (*Repository, error) {
	resourcePath, err := utils.GetResourcePath()
	if err != nil {
		return nil, err
	}
	templateRepo := &Repository{
		RepoPath: filepath.Join(resourcePath, repoPath),
	}
	if !utils.Exists(templateRepo.RepoPath) {
		return nil, errors.New("local-repo not exists")
	}
	return templateRepo, nil
}

// GetTemplates get templates
func (r *Repository) GetTemplates(templateType string,
	runtime string, path string) ([]entity.TemplateInfo, string, error) {
	// read manifest.json
	manifestPath := filepath.Join(r.RepoPath, path)
	data, err := ioutil.ReadFile(manifestPath)
	if err != nil {
		return nil, "", err
	}
	manifest := make(map[string]map[string][]entity.TemplateInfo, templateTypeNum)
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, "", err
	}
	templateMap := manifest[templateType]
	if templateMap == nil {
		return nil, "", fmt.Errorf("type: %s has no template to use", templateType)
	}
	templates := templateMap[runtime]
	if len(templates) == 0 {
		return nil, "", fmt.Errorf("runtime: %s has no template to use", runtime)
	}
	return templates, r.RepoPath, nil
}
