//go:build unit

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func intPtrHelper(v int) *int { return &v }

func TestEffectiveLoadFactor_NilAccount(t *testing.T) {
	var a *Account
	require.Equal(t, 1, a.EffectiveLoadFactor())
}

func TestEffectiveLoadFactor_PositiveConcurrency(t *testing.T) {
	a := &Account{Concurrency: 5}
	require.Equal(t, 5, a.EffectiveLoadFactor())
}

func TestEffectiveLoadFactor_ZeroConcurrency(t *testing.T) {
	a := &Account{Concurrency: 0}
	require.Equal(t, 1, a.EffectiveLoadFactor())
}

func TestEffectiveLoadFactor_LoadFactorIgnored(t *testing.T) {
	// LoadFactor 被忽略，始终使用 Concurrency
	a := &Account{Concurrency: 5, LoadFactor: intPtrHelper(10000)}
	require.Equal(t, 5, a.EffectiveLoadFactor())
}
