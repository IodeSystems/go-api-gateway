package gateway

import (
	"bytes"
	"encoding/json"
	"html/template"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/IodeSystems/graphql-go/v2"
)

// graphqlBodyBufPool keeps a per-request body buffer alive across
// requests so io.Copy reuses the same backing slice instead of
// io.ReadAll allocating a fresh one every call. JSON Unmarshal
// copies string values out, so it's safe to Put the buffer back
// once decoding finishes. Caps overgrown buffers at 64 KiB to keep
// one fat upload from pinning a fat alloc in steady state.
var graphqlBodyBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

const graphqlBodyBufPoolMax = 64 << 10

// Vendored from github.com/graphql-go/handler@v0.2.4 — the upstream
// module imports the original graphql-go/graphql, which is no longer
// the path used by our local fork (renamed to
// github.com/IodeSystems/graphql-go/v2). We use a vanishingly small
// surface of that module (GraphiQL UI render + an HTTP request
// parser); pulling the bits in-tree drops a dep + sidesteps the type
// mismatch between handler's `graphql.Schema` and the fork's.
//
// No behavior change vs the upstream copy; structure is reshaped
// only to drop unused features (playground, RootObjectFn, custom
// error formatter, result callback) and the cached *Handler is
// dropped in favor of a per-request render — the GraphiQL UI is a
// cold browser path, not the JSON hot path.

const (
	contentTypeJSON           = "application/json"
	contentTypeGraphQL        = "application/graphql"
	contentTypeFormURLEncoded = "application/x-www-form-urlencoded"
	contentTypeMultipartForm  = "multipart/form-data"
)

// graphqlRequestOptions is the parsed shape of a GraphQL HTTP
// request body / query string.
type graphqlRequestOptions struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables"`
	OperationName string         `json:"operationName"`
}

// graphqlRequestOptionsCompat is the workaround shape for clients
// that send `variables` as a JSON-encoded string rather than a
// nested object. handler@v0.2.4 carried the same shim.
type graphqlRequestOptionsCompat struct {
	Query         string `json:"query"`
	Variables     string `json:"variables"`
	OperationName string `json:"operationName"`
}

func graphqlOptionsFromForm(values url.Values) *graphqlRequestOptions {
	query := values.Get("query")
	if query == "" {
		return nil
	}
	variables := make(map[string]any, len(values))
	variablesStr := values.Get("variables")
	_ = json.Unmarshal([]byte(variablesStr), &variables)
	return &graphqlRequestOptions{
		Query:         query,
		Variables:     variables,
		OperationName: values.Get("operationName"),
	}
}

// parseGraphqlRequest parses an http.Request into GraphQL request
// options. Drop-in replacement for the upstream
// `handler.NewRequestOptions`. Non-nil err means the request is
// malformed in a way the caller should surface (today: only the
// multipart path); JSON / form parse failures still return an empty
// options struct, matching the upstream behaviour.
func parseGraphqlRequest(r *http.Request) (*graphqlRequestOptions, error) {
	// r.URL.Query() always allocates a url.Values map; skip when there
	// is no query string to parse.
	if r.URL.RawQuery != "" {
		if reqOpt := graphqlOptionsFromForm(r.URL.Query()); reqOpt != nil {
			return reqOpt, nil
		}
	}
	if r.Method != http.MethodPost {
		return &graphqlRequestOptions{}, nil
	}
	if r.Body == nil {
		return &graphqlRequestOptions{}, nil
	}
	contentTypeStr := r.Header.Get("Content-Type")
	// Fast path: the canonical "application/json" with no parameters
	// dodges mime.ParseMediaType, which allocates the params map even
	// when there are none.
	var (
		contentType string
		ctParams    map[string]string
	)
	if contentTypeStr == contentTypeJSON {
		contentType = contentTypeJSON
	} else {
		contentType, ctParams, _ = mime.ParseMediaType(contentTypeStr)
		if contentType == "" {
			contentType = strings.TrimSpace(strings.SplitN(contentTypeStr, ";", 2)[0])
		}
	}
	switch contentType {
	case contentTypeMultipartForm:
		// graphql-multipart-request-spec: files arrive as additional
		// form parts and substitute into Variables. parseGraphqlMultipart
		// owns the request body lifetime once it starts reading.
		opts, err := parseGraphqlMultipart(r, ctParams["boundary"])
		if err != nil {
			return &graphqlRequestOptions{}, err
		}
		return opts, nil
	case contentTypeGraphQL:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return &graphqlRequestOptions{}, nil
		}
		return &graphqlRequestOptions{Query: string(body)}, nil
	case contentTypeFormURLEncoded:
		if err := r.ParseForm(); err != nil {
			return &graphqlRequestOptions{}, nil
		}
		if reqOpt := graphqlOptionsFromForm(r.PostForm); reqOpt != nil {
			return reqOpt, nil
		}
		return &graphqlRequestOptions{}, nil
	case contentTypeJSON:
		fallthrough
	default:
		var opts graphqlRequestOptions
		buf := graphqlBodyBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer func() {
			if buf.Cap() <= graphqlBodyBufPoolMax {
				graphqlBodyBufPool.Put(buf)
			}
		}()
		if _, err := io.Copy(buf, r.Body); err != nil {
			return &opts, nil
		}
		body := buf.Bytes()
		if err := json.Unmarshal(body, &opts); err != nil {
			// `variables` may have been sent as a JSON-encoded string.
			var optsCompat graphqlRequestOptionsCompat
			_ = json.Unmarshal(body, &optsCompat)
			_ = json.Unmarshal([]byte(optsCompat.Variables), &opts.Variables)
		}
		return &opts, nil
	}
}

// graphiqlServer renders the GraphiQL browser UI for a schema.
// Replaces the upstream `handler.Handler` for the one path gwag
// uses it on: browser requests accepting text/html. The JSON hot
// path skips this entirely.
type graphiqlServer struct {
	schema *graphql.Schema
}

func newGraphiqlServer(schema *graphql.Schema) *graphiqlServer {
	return &graphiqlServer{schema: schema}
}

// ServeHTTP renders the GraphiQL page. When the request carries a
// query (e.g. a shared "?query=…" link), the query is executed
// server-side and its result embedded into the page; otherwise the
// browser fetches against `/api/graphql` itself.
func (s *graphiqlServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	opts, _ := parseGraphqlRequest(r)
	params := graphql.Params{
		Schema:         *s.schema,
		RequestString:  opts.Query,
		VariableValues: opts.Variables,
		OperationName:  opts.OperationName,
		Context:        r.Context(),
	}
	renderGraphiQL(w, params)
}

// graphiqlPageData is the template payload for graphiqlTemplate.
type graphiqlPageData struct {
	GraphiqlVersion string
	QueryString     string
	VariablesString string
	OperationName   string
	ResultString    string
}

// graphiqlVersion pins the CDN-loaded GraphiQL build. Matches the
// upstream handler@v0.2.4 value.
const graphiqlVersion = "0.11.11"

var graphiqlTmpl = template.Must(template.New("GraphiQL").Parse(graphiqlTemplate))

func renderGraphiQL(w http.ResponseWriter, params graphql.Params) {
	vars, err := json.MarshalIndent(params.VariableValues, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	varsString := string(vars)
	if varsString == "null" {
		varsString = ""
	}
	var resString string
	if params.RequestString != "" {
		result, err := json.MarshalIndent(graphql.Do(params), "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resString = string(result)
	}
	d := graphiqlPageData{
		GraphiqlVersion: graphiqlVersion,
		QueryString:     params.RequestString,
		ResultString:    resString,
		VariablesString: varsString,
		OperationName:   params.OperationName,
	}
	if err := graphiqlTmpl.ExecuteTemplate(w, "index", d); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// graphiqlTemplate is the rendered HTML for the GraphiQL UI. The
// page loads GraphiQL itself from a CDN and wires its fetcher to
// the current URL, so the gateway only has to serve the bootstrap.
const graphiqlTemplate = `
{{ define "index" }}
<!--
The request to this GraphQL server provided the header "Accept: text/html"
and as a result has been presented GraphiQL - an in-browser IDE for
exploring GraphQL.

If you wish to receive JSON, provide the header "Accept: application/json" or
add "&raw" to the end of the URL within a browser.
-->
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>GraphiQL</title>
  <meta name="robots" content="noindex" />
  <meta name="referrer" content="origin">
  <style>
    body {
      height: 100%;
      margin: 0;
      overflow: hidden;
      width: 100%;
    }
    #graphiql {
      height: 100vh;
    }
  </style>
  <link href="//cdn.jsdelivr.net/npm/graphiql@{{ .GraphiqlVersion }}/graphiql.css" rel="stylesheet" />
  <script src="//cdn.jsdelivr.net/es6-promise/4.0.5/es6-promise.auto.min.js"></script>
  <script src="//cdn.jsdelivr.net/fetch/0.9.0/fetch.min.js"></script>
  <script src="//cdn.jsdelivr.net/react/15.4.2/react.min.js"></script>
  <script src="//cdn.jsdelivr.net/react/15.4.2/react-dom.min.js"></script>
  <script src="//cdn.jsdelivr.net/npm/graphiql@{{ .GraphiqlVersion }}/graphiql.min.js"></script>
</head>
<body>
  <div id="graphiql">Loading...</div>
  <script>
    var parameters = {};
    window.location.search.substr(1).split('&').forEach(function (entry) {
      var eq = entry.indexOf('=');
      if (eq >= 0) {
        parameters[decodeURIComponent(entry.slice(0, eq))] =
          decodeURIComponent(entry.slice(eq + 1));
      }
    });

    function locationQuery(params) {
      return '?' + Object.keys(params).filter(function (key) {
        return Boolean(params[key]);
      }).map(function (key) {
        return encodeURIComponent(key) + '=' +
          encodeURIComponent(params[key]);
      }).join('&');
    }

    var graphqlParamNames = {
      query: true,
      variables: true,
      operationName: true
    };

    var otherParams = {};
    for (var k in parameters) {
      if (parameters.hasOwnProperty(k) && graphqlParamNames[k] !== true) {
        otherParams[k] = parameters[k];
      }
    }
    var fetchURL = locationQuery(otherParams);

    function graphQLFetcher(graphQLParams) {
      return fetch(fetchURL, {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(graphQLParams),
        credentials: 'include',
      }).then(function (response) {
        return response.text();
      }).then(function (responseBody) {
        try {
          return JSON.parse(responseBody);
        } catch (error) {
          return responseBody;
        }
      });
    }

    function onEditQuery(newQuery) {
      parameters.query = newQuery;
      updateURL();
    }

    function onEditVariables(newVariables) {
      parameters.variables = newVariables;
      updateURL();
    }

    function onEditOperationName(newOperationName) {
      parameters.operationName = newOperationName;
      updateURL();
    }

    function updateURL() {
      history.replaceState(null, null, locationQuery(parameters));
    }

    ReactDOM.render(
      React.createElement(GraphiQL, {
        fetcher: graphQLFetcher,
        onEditQuery: onEditQuery,
        onEditVariables: onEditVariables,
        onEditOperationName: onEditOperationName,
        query: {{ .QueryString }},
        response: {{ .ResultString }},
        variables: {{ .VariablesString }},
        operationName: {{ .OperationName }},
      }),
      document.getElementById('graphiql')
    );
  </script>
</body>
</html>
{{ end }}
`
