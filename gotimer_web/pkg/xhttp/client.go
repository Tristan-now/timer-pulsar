package xhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"time"
)

// 单次读取限制 4M
const (
	defaultReadLimitBytes                = 4 * 1024 * 1024
	defaultTimeoutDuration time.Duration = 5 * time.Second
)

type JSONClient struct {
	timeoutDuration time.Duration
	readLimitBytes  int64
}

func NewJSONClient(opts ...Option) *JSONClient {
	j := JSONClient{}
	for _, opt := range opts {
		opt(&j)
	}

	repair(&j)
	return &j
}

func (j *JSONClient) Get(ctx context.Context, url string, header map[string]string, params map[string]string, resp interface{}) error {
	return j.Do(ctx, http.MethodGet, getCompleteURL(url, params), header, nil, resp)
}

func (j *JSONClient) Post(ctx context.Context, url string, header map[string]string, req, resp interface{}) error {
	return j.Do(ctx, http.MethodPost, url, header, req, resp)
}

func (j *JSONClient) Patch(ctx context.Context, url string, header map[string]string, req, resp interface{}) error {
	return j.Do(ctx, http.MethodPatch, url, header, req, resp)
}

func (j *JSONClient) Delete(ctx context.Context, url string, header map[string]string, req, resp interface{}) error {
	return j.Do(ctx, http.MethodDelete, url, header, req, resp)
}

func (j *JSONClient) Do(ctx context.Context, method string, url string, header map[string]string, req, resp interface{}) error {
	tCtx, cancel := context.WithTimeout(ctx, j.timeoutDuration)
	defer cancel()

	reqBody, err := json.Marshal(req)
	if err != nil {
		return err
	}

	request, err := http.NewRequestWithContext(tCtx, method, url, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}

	for k, v := range header {
		request.Header.Add(k, v)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}

	respBody, err := io.ReadAll(io.LimitReader(response.Body, j.readLimitBytes))
	if err != nil {
		return err
	}

	return json.Unmarshal(respBody, resp)
}

func getCompleteURL(originURL string, params map[string]string) string {
	values := neturl.Values{}
	for k, v := range params {
		values.Add(k, v)
	}

	queriesStr, _ := neturl.QueryUnescape(values.Encode())
	if len(queriesStr) == 0 {
		return originURL
	}
	return fmt.Sprintf("%s?%s", originURL, queriesStr)
}

/*
包含一个用于发起HTTP请求的客户端，特别是针对JSON数据格式的请求。这个客户端提供了一些方法来配置和发送HTTP请求，并处理JSON响应。下面是对这些文件中定义的逻辑的详细解释：

xhttp.go
常量定义：

defaultReadLimitBytes：定义了单次读取请求体的最大限制为4MB。
defaultTimeoutDuration：定义了默认的HTTP请求超时时间为5秒。
JSONClient结构体：

timeoutDuration：用于存储HTTP请求的超时时间。
readLimitBytes：用于存储读取响应内容时的限制大小。
NewJSONClient函数：

用于创建并返回一个新的JSONClient实例。
接受可变数量的Option参数，允许用户自定义客户端的行为。
HTTP请求方法：

Get、Post、Patch、Delete：这些方法分别用于发起对应的HTTP请求。
它们都调用了Do方法来实际执行请求。
Do方法：

用于执行实际的HTTP请求。
接受HTTP方法、完整URL、请求头、请求体和响应目标对象。
使用context.WithTimeout来设置请求的超时。
将请求体序列化为JSON，并创建一个新的http.Request。
设置请求头。
使用http.DefaultClient发送请求并获取响应。
读取响应内容，并确保不超过readLimitBytes指定的字节限制。
将响应内容反序列化为指定的响应对象。
getCompleteURL函数：

用于构建完整的请求URL，包括查询参数。
接受原始URL和参数映射作为输入。
使用net/url包来编码查询参数并附加到URL上。
第二个文件：options.go
Option类型：

定义了一个函数类型Option，它接受一个指向JSONClient的指针，并返回nil。
这个类型用于提供配置JSONClient的方法。
WithTimeout函数：

用于设置JSONClient的超时时间。
如果传入的持续时间小于或等于0，它会使用默认的超时时间。
WithReadLimitBytes函数：

用于设置JSONClient的读取限制。
如果传入的限制小于或等于0，它会使用默认的读取限制。
repair函数：

用于修复或设置JSONClient的默认选项。
如果readLimitBytes或timeoutDuration小于或等于0，它会使用默认值。

*/
