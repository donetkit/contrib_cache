package uuid

import "strings"

import (
	"github.com/google/uuid"
)

func NewUUID() string {
	uuid, err := GenerateGoogleUUID()
	if err == nil {
		return strings.ReplaceAll(uuid, "-", "")
	}
	uuid, _ = GenerateUUID()
	return uuid
}

func GenerateGoogleUUID() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
