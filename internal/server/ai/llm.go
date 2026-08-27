package ai

import (
	"context"
	"fmt"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
)

// llmClient 封装 openai-go，兼容任意 OpenAI 兼容接口（通过自定义 base_url）。
type llmClient struct {
	client    openai.Client
	model     string
	maxTokens int64
}

func newLLM(baseURL, apiKey, model string, maxTokens int) *llmClient {
	opts := []option.RequestOption{option.WithAPIKey(apiKey)}
	if baseURL != "" {
		opts = append(opts, option.WithBaseURL(baseURL))
	}
	return &llmClient{
		client:    openai.NewClient(opts...),
		model:     model,
		maxTokens: int64(maxTokens),
	}
}

// stream 发起一次流式 chat completion：逐个把文本增量交给 onToken，结束后返回
// 累积的完整消息（含 tool_calls）与 token 用量，供 Agent 决定是否继续工具循环。
func (l *llmClient) stream(
	ctx context.Context,
	msgs []openai.ChatCompletionMessageParamUnion,
	tools []openai.ChatCompletionToolParam,
	onToken func(string),
) (openai.ChatCompletionMessage, openai.CompletionUsage, error) {
	params := openai.ChatCompletionNewParams{
		Messages: msgs,
		Model:    openai.ChatModel(l.model),
		// 请求在流式响应尾部带上 usage，用于 token 消耗记录（见 §7.3）。
		StreamOptions: openai.ChatCompletionStreamOptionsParam{
			IncludeUsage: openai.Bool(true),
		},
	}
	if l.maxTokens > 0 {
		params.MaxTokens = openai.Int(l.maxTokens)
	}
	if len(tools) > 0 {
		params.Tools = tools
	}

	stream := l.client.Chat.Completions.NewStreaming(ctx, params)
	acc := openai.ChatCompletionAccumulator{}
	for stream.Next() {
		chunk := stream.Current()
		acc.AddChunk(chunk)
		if len(chunk.Choices) > 0 {
			if d := chunk.Choices[0].Delta.Content; d != "" {
				onToken(d)
			}
		}
	}
	if err := stream.Err(); err != nil {
		return openai.ChatCompletionMessage{}, openai.CompletionUsage{}, err
	}
	if len(acc.Choices) == 0 {
		return openai.ChatCompletionMessage{}, acc.Usage, fmt.Errorf("llm returned no choices")
	}
	return acc.Choices[0].Message, acc.Usage, nil
}
