package credential

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	acr "github.com/alibabacloud-go/cr-20181201/v3/client"
	"github.com/alibabacloud-go/tea/dara"
)

func TestACRTokenProviderBuildsRequestAndParsesResponse(t *testing.T) {
	t.Parallel()

	expiresAt := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	client := &fakeAuthorizationTokenClient{
		response: (&acr.GetAuthorizationTokenResponse{}).
			SetStatusCode(200).
			SetBody((&acr.GetAuthorizationTokenResponseBody{}).
				SetIsSuccess(true).
				SetCode("success").
				SetTempUsername("temporary-user").
				SetAuthorizationToken("temporary-password").
				SetExpireTime(expiresAt.UnixMilli())),
	}
	provider := &ACRTokenProvider{newClient: func(request Request) (authorizationTokenClient, error) {
		if request.AccessKeyID != "access-key" || request.AccessKeySecret != "access-secret" {
			t.Fatalf("client factory request = %+v, want configured AK/SK", request)
		}
		return client, nil
	}}
	request := Request{
		Key:             testRegistryKey,
		AccessKeyID:     "access-key",
		AccessKeySecret: "access-secret",
	}

	token, err := provider.GetAuthorizationToken(context.Background(), request)
	if err != nil {
		t.Fatalf("GetAuthorizationToken() error = %v", err)
	}
	if token.Username != "temporary-user" || token.Password != "temporary-password" || !token.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("token = %+v, want parsed ACR response", token)
	}
	if client.request == nil || client.request.GetInstanceId() == nil || *client.request.GetInstanceId() != testRegistryKey.InstanceID {
		t.Fatalf("ACR request InstanceId = %v, want %q", client.request, testRegistryKey.InstanceID)
	}
	if client.request.GetExpiresInHours() == nil || *client.request.GetExpiresInHours() != authorizationTokenValidityHours {
		t.Fatalf("ACR request ExpiresInHours = %v, want %d", client.request.GetExpiresInHours(), authorizationTokenValidityHours)
	}
}

func TestACRTokenProviderRejectsInvalidResponses(t *testing.T) {
	t.Parallel()

	validBody := func() *acr.GetAuthorizationTokenResponseBody {
		return (&acr.GetAuthorizationTokenResponseBody{}).
			SetIsSuccess(true).
			SetTempUsername("user").
			SetAuthorizationToken("password").
			SetExpireTime(time.Now().Add(time.Hour).UnixMilli())
	}
	tests := map[string]*acr.GetAuthorizationTokenResponse{
		"nil response":        nil,
		"HTTP failure":        (&acr.GetAuthorizationTokenResponse{}).SetStatusCode(500).SetBody(validBody()),
		"nil body":            (&acr.GetAuthorizationTokenResponse{}).SetStatusCode(200),
		"unsuccessful result": (&acr.GetAuthorizationTokenResponse{}).SetStatusCode(200).SetBody((&acr.GetAuthorizationTokenResponseBody{}).SetIsSuccess(false).SetCode("Denied")),
		"empty username":      (&acr.GetAuthorizationTokenResponse{}).SetStatusCode(200).SetBody(validBody().SetTempUsername("")),
		"empty password":      (&acr.GetAuthorizationTokenResponse{}).SetStatusCode(200).SetBody(validBody().SetAuthorizationToken("")),
		"invalid expiration":  (&acr.GetAuthorizationTokenResponse{}).SetStatusCode(200).SetBody(validBody().SetExpireTime(0)),
	}

	for name, response := range tests {
		response := response
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := tokenFromACRResponse(response); err == nil {
				t.Fatal("tokenFromACRResponse() error = nil, want validation error")
			}
		})
	}
}

func TestACRTokenProviderReturnsClientAndAPIErrors(t *testing.T) {
	t.Parallel()

	provider := &ACRTokenProvider{newClient: func(Request) (authorizationTokenClient, error) {
		return nil, errors.New("client failure containing access-secret")
	}}
	request := Request{Key: testRegistryKey, AccessKeyID: "key", AccessKeySecret: "secret"}
	if _, err := provider.GetAuthorizationToken(context.Background(), request); err == nil || strings.Contains(err.Error(), "access-secret") {
		t.Fatal("client construction error was not returned")
	}

	provider.newClient = func(Request) (authorizationTokenClient, error) {
		return &fakeAuthorizationTokenClient{err: errors.New("API failure containing temporary-password")}, nil
	}
	if _, err := provider.GetAuthorizationToken(context.Background(), request); err == nil || strings.Contains(err.Error(), "temporary-password") {
		t.Fatal("ACR API error was not returned")
	}

	sdkError := dara.NewSDKError(map[string]interface{}{
		"statusCode": 403,
		"code":       "Forbidden",
		"message":    "message containing access-secret",
	})
	safeError := safeACRRequestError(sdkError)
	if strings.Contains(safeError.Error(), "access-secret") || !strings.Contains(safeError.Error(), "Forbidden") {
		t.Fatalf("safeACRRequestError() = %q, want code without SDK message", safeError)
	}
}

func TestACRClientUsesRegistryRegionEndpoint(t *testing.T) {
	t.Parallel()

	client, err := newACRAuthorizationTokenClient(Request{
		Key:             testRegistryKey,
		AccessKeyID:     "access-key",
		AccessKeySecret: "access-secret",
	})
	if err != nil {
		t.Fatalf("newACRAuthorizationTokenClient() error = %v", err)
	}
	sdkClient, ok := client.(*acr.Client)
	if !ok {
		t.Fatalf("ACR client type = %T, want *client.Client", client)
	}
	endpoint := ""
	if sdkClient.Endpoint != nil {
		endpoint = *sdkClient.Endpoint
	}
	if endpoint != "cr.cn-hangzhou.aliyuncs.com" {
		t.Fatalf("ACR client endpoint = %q, want cr.cn-hangzhou.aliyuncs.com", endpoint)
	}
}

type fakeAuthorizationTokenClient struct {
	request  *acr.GetAuthorizationTokenRequest
	response *acr.GetAuthorizationTokenResponse
	err      error
}

func (c *fakeAuthorizationTokenClient) GetAuthorizationTokenWithContext(
	_ context.Context,
	request *acr.GetAuthorizationTokenRequest,
	_ *dara.RuntimeOptions,
) (*acr.GetAuthorizationTokenResponse, error) {
	c.request = request
	return c.response, c.err
}
