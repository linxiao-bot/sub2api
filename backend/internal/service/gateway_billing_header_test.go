package service

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// TestComputeBillingHeader_TestVectors 验证算法文档（CCH_ALGORITHM.md）中的测试向量
func TestComputeBillingHeader_TestVectors(t *testing.T) {
	t.Run("message=hey", func(t *testing.T) {
		body := []byte(`{"messages":[{"role":"user","content":"hey"}]}`)
		header := computeBillingHeader(body)
		// SHA-256("hey")[:5] = "fa690"
		require.Regexp(t, `^x-anthropic-billing-header: cc_version=2\.1\.78\.[0-9a-f]{3}; cc_entrypoint=cli; cch=fa690;$`, header)
	})

	t.Run("message=empty", func(t *testing.T) {
		body := []byte(`{"messages":[{"role":"user","content":""}]}`)
		header := computeBillingHeader(body)
		// SHA-256("")[:5] = "e3b0c"
		require.True(t, strings.HasSuffix(header, "cch=e3b0c;"), "got: %s", header)
	})

	t.Run("no_user_message", func(t *testing.T) {
		body := []byte(`{"messages":[{"role":"assistant","content":"hi"}]}`)
		header := computeBillingHeader(body)
		// 无 user 消息等同于空字符串
		require.True(t, strings.HasSuffix(header, "cch=e3b0c;"), "got: %s", header)
	})
}

// TestComputeBillingHeader_ArrayContent 验证 content 为数组时的文本拼接
func TestComputeBillingHeader_ArrayContent(t *testing.T) {
	t.Run("text_blocks_concatenated", func(t *testing.T) {
		body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"hel"},{"type":"text","text":"lo"}]}]}`)
		header := computeBillingHeader(body)
		// concatenated = "hello", SHA-256("hello")[:5] = "2cf24"
		require.True(t, strings.HasSuffix(header, "cch=2cf24;"), "got: %s", header)
	})

	t.Run("non_text_blocks_skipped", func(t *testing.T) {
		body := []byte(`{"messages":[{"role":"user","content":[{"type":"image","source":{}},{"type":"text","text":"hey"}]}]}`)
		header := computeBillingHeader(body)
		require.True(t, strings.HasSuffix(header, "cch=fa690;"), "got: %s", header)
	})
}

// TestComputeBillingHeader_Format 验证输出格式与抓包一致
func TestComputeBillingHeader_Format(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hey"}]}`)
	header := computeBillingHeader(body)
	require.Regexp(t,
		`^x-anthropic-billing-header: cc_version=\d+\.\d+\.\d+\.[0-9a-f]{3}; cc_entrypoint=cli; cch=[0-9a-f]{5};$`,
		header,
	)
}

// TestInjectBillingHeader_InsertsAsFirstElement 验证 billing header 被注入为 system 第一个元素
func TestInjectBillingHeader_InsertsAsFirstElement(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hey"}],"system":[{"type":"text","text":"You are helpful.","cache_control":{"type":"ephemeral"}}]}`)
	result := injectBillingHeader(body)

	sys := gjson.GetBytes(result, "system")
	require.True(t, sys.IsArray())

	items := sys.Array()
	require.GreaterOrEqual(t, len(items), 2)
	require.True(t, strings.HasPrefix(items[0].Get("text").String(), "x-anthropic-billing-header"))
	// billing entry 不应有 cache_control
	require.False(t, items[0].Get("cache_control").Exists())
	require.Equal(t, "You are helpful.", items[1].Get("text").String())
}

// TestInjectBillingHeader_ReplacesExisting 验证客户端已有的 billing header 被替换
func TestInjectBillingHeader_ReplacesExisting(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hey"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=1.0.0.abc; cc_entrypoint=cli; cch=00000;"},{"type":"text","text":"System prompt."}]}`)
	result := injectBillingHeader(body)

	items := gjson.GetBytes(result, "system").Array()
	require.Len(t, items, 2)
	require.True(t, strings.HasPrefix(items[0].Get("text").String(), "x-anthropic-billing-header"))
	require.NotContains(t, items[0].Get("text").String(), "1.0.0.abc")
	require.Equal(t, "System prompt.", items[1].Get("text").String())
}

// TestInjectBillingHeader_NoSystem 验证无 system 时创建数组
func TestInjectBillingHeader_NoSystem(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hey"}]}`)
	result := injectBillingHeader(body)

	sys := gjson.GetBytes(result, "system")
	require.True(t, sys.IsArray())
	items := sys.Array()
	require.Len(t, items, 1)
	require.True(t, strings.HasPrefix(items[0].Get("text").String(), "x-anthropic-billing-header"))
}

// TestInjectBillingHeader_StringSystem 验证 string 格式 system 被转为数组
func TestInjectBillingHeader_StringSystem(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hey"}],"system":"You are helpful."}`)
	result := injectBillingHeader(body)

	items := gjson.GetBytes(result, "system").Array()
	require.Len(t, items, 2)
	require.True(t, strings.HasPrefix(items[0].Get("text").String(), "x-anthropic-billing-header"))
	require.Equal(t, "You are helpful.", items[1].Get("text").String())
}
