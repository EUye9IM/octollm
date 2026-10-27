package loadbalancer

import "github.com/infinigence/octollm/pkg/octollm"

// selectedBackendMeta returns the backend name published for req, as an any so
// existing assertions keep their shape.
func selectedBackendMeta(req *octollm.Request) (any, bool) {
	name, ok := GetSelectedBackendName(req)
	if !ok {
		return nil, false
	}
	return name, true
}
