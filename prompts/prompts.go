package prompts

import (
	"fmt"
	"github.com/manifoldco/promptui"
	"os"
)

type Command struct {
	Name        string
	Description string
	Action      func() error
}

func PromptConnectionString() (string, error) {
	prompt := promptui.Prompt{
		Label: "Enter connection string",
	}

	result, err := prompt.Run()
	if err != nil {
		return "", fmt.Errorf("prompt failed: %w", err)
	}

	return result, nil
}

func PromptSelectTopic(topics []string) (string, error) {
	prompt := promptui.Select{
		Label: "Select a topic",
		Items: topics,
	}

	_, result, err := prompt.Run()
	if err != nil {
		return "", fmt.Errorf("prompt failed: %w", err)
	}

	return result, nil
}

func PromptSelectSubscription(subscriptions []string) (string, error) {
	prompt := promptui.Select{
		Label: "Select a subscription",
		Items: subscriptions,
	}

	_, result, err := prompt.Run()
	if err != nil {
		return "", fmt.Errorf("prompt failed: %w", err)
	}

	return result, nil
}

func PromptCommandList(commands []Command) error {
	prompt := promptui.Select{
		Label: "Select Command",
		Items: commands,
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}?",
			Active:   "\U0001F449 {{ .Name | cyan }} ({{ .Description | red }})",
			Inactive: "  {{ .Name | cyan }} ({{ .Description | red }})",
			Selected: "\U0001F3C1 {{ .Name | red | cyan }}",
		},
	}

	i, _, err := prompt.Run()

	if err != nil {
		fmt.Printf("Prompt failed %v\n", err)
		os.Exit(1)
	}

	err = commands[i].Action()
	if err != nil {
		fmt.Printf("Command failed %v\n", err)
		os.Exit(1)
	}

	return nil
}

func PromptFileName(defaultValue *string) (string, error) {
	prompt := promptui.Prompt{
		Label: "Enter file name",
	}

	if defaultValue != nil {
		prompt.Default = *defaultValue
	}

	result, err := prompt.Run()
	if err != nil {
		return "", fmt.Errorf("prompt failed: %w", err)
	}

	return result, nil
}
