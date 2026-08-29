package credential

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	acr "github.com/alibabacloud-go/cr-20181201/v3/client"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/utils"
	"github.com/alibabacloud-go/tea/dara"
)

const authorizationTokenValidityHours int32 = 1

type authorizationTokenClient interface {
	GetAuthorizationTokenWithContext(
		ctx context.Context,
		request *acr.GetAuthorizationTokenRequest,
		runtime *dara.RuntimeOptions,
	) (*acr.GetAuthorizationTokenResponse, error)
}

type authorizationTokenClientFactory func(Request) (authorizationTokenClient, error)

// ACRTokenProvider implements TokenProvider with Alibaba Cloud's ACR V2 SDK.
// A client is built per call because each configured Registry can use a
// different AK/SK pair and Region.
type ACRTokenProvider struct {
	newClient authorizationTokenClientFactory
}

func NewACRTokenProvider() *ACRTokenProvider {
	return &ACRTokenProvider{newClient: newACRAuthorizationTokenClient}
}

func (p *ACRTokenProvider) GetAuthorizationToken(ctx context.Context, request Request) (Token, error) {
	if request.Key.RegionID == "" || request.Key.InstanceID == "" {
		return Token{}, errors.New("registry region and instance ID must not be empty")
	}
	if request.AccessKeyID == "" || request.AccessKeySecret == "" {
		return Token{}, errors.New("registry access key ID and secret must not be empty")
	}

	client, err := p.newClient(request)
	if err != nil {
		return Token{}, errors.New("create ACR client failed")
	}

	response, err := client.GetAuthorizationTokenWithContext(
		ctx,
		(&acr.GetAuthorizationTokenRequest{}).
			SetInstanceId(request.Key.InstanceID).
			SetExpiresInHours(authorizationTokenValidityHours),
		&dara.RuntimeOptions{},
	)
	if err != nil {
		return Token{}, safeACRRequestError(err)
	}
	return tokenFromACRResponse(response)
}

// safeACRRequestError deliberately excludes the SDK message and response data,
// either of which may contain request or response fields. Status and error code
// are sufficient for operational diagnosis without risking credential output.
func safeACRRequestError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("ACR GetAuthorizationToken request ended: %w", err)
	}

	var sdkError *dara.SDKError
	if errors.As(err, &sdkError) {
		statusCode := 0
		if sdkError.StatusCode != nil {
			statusCode = *sdkError.StatusCode
		}
		code := "unknown"
		if sdkError.Code != nil && *sdkError.Code != "" {
			code = *sdkError.Code
		}
		return fmt.Errorf("ACR GetAuthorizationToken request failed: HTTP status %d, code %q", statusCode, code)
	}
	return errors.New("ACR GetAuthorizationToken request failed")
}

func newACRAuthorizationTokenClient(request Request) (authorizationTokenClient, error) {
	sdkConfig := (&openapi.Config{}).
		SetAccessKeyId(request.AccessKeyID).
		SetAccessKeySecret(request.AccessKeySecret).
		SetRegionId(request.Key.RegionID)
	return acr.NewClient(sdkConfig)
}

func tokenFromACRResponse(response *acr.GetAuthorizationTokenResponse) (Token, error) {
	if response == nil {
		return Token{}, errors.New("ACR GetAuthorizationToken returned an empty response")
	}
	if response.StatusCode != nil && (*response.StatusCode < http.StatusOK || *response.StatusCode >= http.StatusMultipleChoices) {
		return Token{}, fmt.Errorf("ACR GetAuthorizationToken returned HTTP status %d", *response.StatusCode)
	}
	if response.Body == nil {
		return Token{}, errors.New("ACR GetAuthorizationToken returned an empty body")
	}
	if response.Body.IsSuccess == nil || !*response.Body.IsSuccess {
		code := "unknown"
		if response.Body.Code != nil && *response.Body.Code != "" {
			code = *response.Body.Code
		}
		return Token{}, fmt.Errorf("ACR GetAuthorizationToken was unsuccessful: code %q", code)
	}
	if response.Body.TempUsername == nil || strings.TrimSpace(*response.Body.TempUsername) == "" {
		return Token{}, errors.New("ACR GetAuthorizationToken returned an empty username")
	}
	if response.Body.AuthorizationToken == nil || *response.Body.AuthorizationToken == "" {
		return Token{}, errors.New("ACR GetAuthorizationToken returned an empty token")
	}
	if response.Body.ExpireTime == nil || *response.Body.ExpireTime <= 0 {
		return Token{}, errors.New("ACR GetAuthorizationToken returned an invalid expiration time")
	}

	return Token{
		Username:  *response.Body.TempUsername,
		Password:  *response.Body.AuthorizationToken,
		ExpiresAt: time.UnixMilli(*response.Body.ExpireTime),
	}, nil
}
