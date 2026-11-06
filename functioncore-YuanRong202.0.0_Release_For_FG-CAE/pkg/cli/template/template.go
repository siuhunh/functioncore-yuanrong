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

// Package template is api of template
package template

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/template/entity"
	"functioncore/pkg/cli/template/repo"
	"functioncore/pkg/cli/utils/colorprint"
	"functioncore/pkg/cli/utils/terminal"
)

const manifestFile = "manifest.json"

// GetTemplate get a template
func (s *Selector) GetTemplate() (entity.TemplateInfo, error) {
	var templateInfo entity.TemplateInfo
	templateRepo, err := repo.InitRepo(s.Source)
	if err != nil {
		return templateInfo, err
	}
	// get templates from repo
	templates, repoPath, err := templateRepo.GetTemplates(s.Type, s.Runtime, manifestFile)
	if err != nil {
		return templateInfo, err
	}
	if s.Interactive {
		return s.interactiveSelect(repoPath, templates), nil
	}

	return nonInteractiveSelect(repoPath, s.Selected, templates)
}

func nonInteractiveSelect(repoPath, selectedTemplateName string,
	templates []entity.TemplateInfo) (entity.TemplateInfo, error) {
	var templateInfo entity.TemplateInfo

	if selectedTemplateName == "" {
		return templateInfo, errors.New("the template name cannot be empty")
	}
	for _, templateInfo := range templates {
		if strings.ToLower(selectedTemplateName) == strings.ToLower(templateInfo.TemplateName) {
			templateInfo.TemplatePath = filepath.Join(repoPath, templateInfo.Directory)
			return templateInfo, nil
		}
	}

	return templateInfo, fmt.Errorf("template:%s not exists", selectedTemplateName)
}

func (s *Selector) interactiveSelect(repoPath string, templates []entity.TemplateInfo) entity.TemplateInfo {
	fmt.Fprintf(s.W, "\n%s quick start %s templates:\n", constant.PlatformName, s.Type)
	var templateChoices string
	var num int
	var err error

	for i, template := range templates {
		fmt.Fprintf(s.W, "\t %d- %s \n", i+1, template.Description)
		templateChoices += fmt.Sprintf(" %s ", template.Description)
		// default helloworld-world template
		if strings.ToLower(template.TemplateName) == "hello-world" {
			num = i + 1
		}
	}
	showStr := fmt.Sprintf("Template [%d]: ", num)
	colorprint.PrintInteractive(s.W, showStr)
	if line := terminal.ScanLine(s.R, s.W, len(showStr)); line != "" {
		num, err = strconv.Atoi(line)
		if err != nil {
			colorprint.PrintfColorWord(s.W, "The template must in [%s].", colorprint.ErrorColor, templateChoices)
			return s.interactiveSelect(repoPath, templates)
		}
	}

	if num <= 0 || num > len(templates) {
		colorprint.PrintfColorWord(s.W, "The template must in [%s].", colorprint.ErrorColor, templateChoices)
		return s.interactiveSelect(repoPath, templates)
	}

	templateInfo := templates[num-1]
	templateInfo.TemplatePath = filepath.Join(repoPath, templateInfo.Directory)
	return templateInfo
}
