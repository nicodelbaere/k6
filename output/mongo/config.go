package mongo

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gopkg.in/guregu/null.v3"
	"github.com/kelseyhightower/envconfig"
    "github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"go.k6.io/k6/lib/types"
)

type config struct {
	// Connection.
	URI              null.String        `json:"uri" envconfig:"K6_MONGO_URI"`
	Username         null.String        `json:"username,omitempty" envconfig:"K6_MONGO_USERNAME"`
	Password         null.String        `json:"password,omitempty" envconfig:"K6_MONGO_PASSWORD"`
	Insecure         null.Bool          `json:"insecure,omitempty" envconfig:"K6_MONGO_INSECURE"`
	CollectionName   null.String        `json:"collectionName,omitempty" envconfig:"K6_MONGO_COLLECTION_NAME"`
	DatabaseName     null.String        `json:"databaseName,omitempty" envconfig:"K6_MONGO_DATABASE_NAME"`

	PayloadSize      null.Int           `json:"payloadSize,omitempty" envconfig:"K6_MONGO_PAYLOAD_SIZE"`
	PushInterval     types.NullDuration `json:"pushInterval,omitempty" envconfig:"K6_MONGO_PUSH_INTERVAL"`
	ConcurrentWrites null.Int           `json:"concurrentWrites,omitempty" envconfig:"K6_MONGO_CONCURRENT_WRITES"`

	// Samples.
	Precision    null.String `json:"precision,omitempty" envconfig:"K6_MONGO_PRECISION"`
	Retention    null.String `json:"retention,omitempty" envconfig:"K6_MONGO_RETENTION"`
	Consistency  null.String `json:"consistency,omitempty" envconfig:"K6_MONGO_CONSISTENCY"`
	TagsAsFields []string    `json:"tagsAsFields,omitempty" envconfig:"K6_MONGO_TAGS_AS_FIELDS"`
}

func NewConfig() config {
	c := config{
		URI:              null.NewString("mongodb://mongo:27017/", false),
		CollectionName:   null.NewString("k6_col", false),
		DatabaseName:     null.NewString("k6", false),
		TagsAsFields:     []string{"vu", "iter", "url"},
		ConcurrentWrites: null.NewInt(10, false),
		PushInterval:     types.NewNullDuration(time.Second, false),
	}
	return c
}


func (c config) Apply(cfg config) config {

	if cfg.URI.Valid {
		c.URI = cfg.URI
    }
	if cfg.Username.Valid {
		c.Username = cfg.Username
    }
	if cfg.Password.Valid {
		c.Password = cfg.Password
    }
	if cfg.Insecure.Valid {
		c.Insecure = cfg.Insecure
    }
	if cfg.CollectionName.Valid {
		c.CollectionName = cfg.CollectionName
    }
	if cfg.DatabaseName.Valid {
		c.DatabaseName = cfg.DatabaseName
    }
	if cfg.PayloadSize.Valid {
		c.PayloadSize = cfg.PayloadSize
    }
	if cfg.PushInterval.Valid {
		c.PushInterval = cfg.PushInterval
    }
	if cfg.ConcurrentWrites.Valid {
		c.ConcurrentWrites = cfg.ConcurrentWrites
    }

	if cfg.Precision.Valid {
		c.Precision = cfg.Precision
    }
	if cfg.Retention.Valid {
		c.Retention = cfg.Retention
    }
	if cfg.Consistency.Valid {
		c.Consistency = cfg.Consistency
    }
	if len(cfg.TagsAsFields) > 0 {
		c.TagsAsFields = cfg.TagsAsFields
	}
	return c
}

func ParseJSON(data json.RawMessage) (config, error) {
	conf := config{}
	err := json.Unmarshal(data, &conf)
	return conf, err
}

func ParseURL(text string) (config, error) {
	c := config{}
	u, err := url.Parse(text)
	if err != nil {
		return c, err
	}
	if u.User != nil {
		c.Username = null.StringFrom(u.User.Username())
		pass, _ := u.User.Password()
		c.Password = null.StringFrom(pass)
	}
	if u.Host != "" {
        var user_password = ""
        if (c.Username != null.StringFrom("") && c.Password != null.StringFrom("")) {
            user_password = c.Username.String + ":" + c.Password.String + "@"
        }
		c.URI = null.StringFrom(u.Scheme + "://" + user_password + u.Host )
	}
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		c.DatabaseName = null.StringFrom(db)
	}
	for k, vs := range u.Query() {
		switch k {
		case "insecure":
			switch vs[0] {
			case "":
			case "false":
				c.Insecure = null.BoolFrom(false)
			case "true":
				c.Insecure = null.BoolFrom(true)
			default:
				return c, errors.Errorf("insecure must be true or false, not %s", vs[0])
			}
		case "payload_size":
			var size int
			size, err = strconv.Atoi(vs[0])
			if err != nil {
				return c, err
			}
			c.PayloadSize = null.IntFrom(int64(size))
		case "precision":
			c.Precision = null.StringFrom(vs[0])
		case "retention":
			c.Retention = null.StringFrom(vs[0])
		case "consistency":
			c.Consistency = null.StringFrom(vs[0])

		case "pushInterval":
			err = c.PushInterval.UnmarshalText([]byte(vs[0]))
			if err != nil {
				return c, err
			}
		case "concurrentWrites":
			var writes int
			writes, err = strconv.Atoi(vs[0])
			if err != nil {
				return c, err
			}
			c.ConcurrentWrites = null.IntFrom(int64(writes))
		case "tagsAsFields":
			c.TagsAsFields = vs
		default:
			return c, errors.Errorf("unknown query parameter: %s", k)
		}
	}
	logrus.Println("c",c)
	return c, err
}

// GetConsolidatedConfig combines {default config values + JSON config +
// environment vars + URL config values}, and returns the final result.
func GetConsolidatedConfig(jsonRawConf json.RawMessage, env map[string]string, url string) (config, error) {
	result := NewConfig()
	if jsonRawConf != nil {
		jsonConf, err := ParseJSON(jsonRawConf)
		if err != nil {
			return result, err
		}
		result = result.Apply(jsonConf)
	}

	envConfig := config{}
	if err := envconfig.Process("", &envConfig); err != nil {
		// TODO: get rid of envconfig and actually use the env parameter...
		return result, err
	}
	result = result.Apply(envConfig)

	if url != "" {
		urlConf, err := ParseURL(url)
		if err != nil {
			return result, err
		}
		result = result.Apply(urlConf)
	}

	return result, nil
}
