

package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	uuid "github.com/nu7hatch/gouuid"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"go.k6.io/k6/stats"
	"go.k6.io/k6/output"
)

// Output is the mongo Output struct
type Output struct {
	output.SampleBuffer

	periodicFlusher *output.PeriodicFlusher

	Client          *mongo.Client
	MongoContext    context.Context
	MongoCollection *mongo.Collection
	//Context     context.TODO()
	config config
	logger logrus.FieldLogger

	semaphoreCh chan struct{}
	UUID        string
}



// New returns new mongo output
func New(params output.Params) (output.Output, error) {
	return newOutput(params)
}

// New creates a new mongo output.
func newOutput(params output.Params) (*Output, error) {
	logger := params.Logger.WithFields(logrus.Fields{"output": "mongo"})
	conf, err := GetConsolidatedConfig(params.JSONConfig, params.Environment, params.ConfigArgument)
	if err != nil {
		return nil, err
	}
	logger.Debug("New")


	client, err := mongo.NewClient(options.Client().ApplyURI(conf.URI.String))
	if err != nil {
		logger.Fatal(err)
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 1000*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		logger.Fatal(err)
		return nil, err
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, err
	}

	if conf.ConcurrentWrites.Int64 <= 0 {
		return nil, errors.New("mongodb's ConcurrentWrites must be a positive number")
	}
	//TODO remove ?
	//defer func() {
	//    if err = client.Disconnect(ctx); err != nil {
	//        return nil, err
	//    }
	//}()
	//todo disconnect at the end
	//	defer func() { _ = client.Disconnect(ctx) }()
	uu, _ := uuid.NewV4()

	return &Output{
		logger: logger,
		Client:       client,
		MongoContext: ctx,
		config:       conf,
		semaphoreCh:  make(chan struct{}, conf.ConcurrentWrites.Int64),
		UUID:         uu.String(),
	}, nil
}

// Description returns a human-readable description of the output.
func (o *Output) Description() string {
	return fmt.Sprintf("Mongo (%s)", o.config.URI.String)
}

// Start tries to open the specified JSON file and starts the goroutine for
// metric flushing. If gzip encoding is specified, it also handles that.
func (o *Output) Start() error {
	o.logger.Debug("Starting...")

	o.logger.Debug("Init", o.config.DatabaseName.String, o.config.CollectionName.String)
	// Prereq: Create collections.
	wcMajority := writeconcern.New(writeconcern.WMajority(), writeconcern.WTimeout(1*time.Second))
	wcMajorityCollectionOpts := options.Collection().SetWriteConcern(wcMajority)
	o.MongoCollection = o.Client.Database(o.config.DatabaseName.String).Collection(o.config.CollectionName.String, wcMajorityCollectionOpts)

	pf, err := output.NewPeriodicFlusher(time.Duration(o.config.PushInterval.Duration), o.flushMetrics)
	if err != nil {
		return err //nolint:wrapcheck
	}
	o.logger.Debug("Started!")
	o.periodicFlusher = pf

	return nil
}

func (o *Output) flushMetrics() {
	samples := o.GetBufferedSamples()
	// let first get the data and then wait our turn
	o.semaphoreCh <- struct{}{}
	defer func() {
		<-o.semaphoreCh
	}()
	o.logger.Debug("MongoDB: Committing...")
	o.logger.WithField("samples", len(samples)).Debug("MongoDB: Writing...")
	startTime := time.Now()
	if err := o.batchFromSamples(samples); err != nil {
		o.logger.WithError(err).Error("MongoDB: Couldn't write stats")
	}
	t := time.Since(startTime)
	o.logger.WithField("t", t).Debug("MongoDB: Batch written!")
}


func (o *Output) batchFromSamples(containers []stats.SampleContainer) error {
	o.logger.Debug("batchFromSamples")

	return o.Client.UseSessionWithOptions(
		o.MongoContext, options.Session().SetDefaultReadPreference(readpref.Primary()),
		func(sctx mongo.SessionContext) error {

			return o.RunTransactionWithRetry(sctx, containers, o.AddData)
		},
	)
}

func (o *Output) RunTransactionWithRetry(sctx mongo.SessionContext, containers []stats.SampleContainer, txnFn func(mongo.SessionContext, []stats.SampleContainer) error) error {
	o.logger.Debug("RunTransactionWithRetry")
	for {
		err := sctx.StartTransaction()
		if err != nil {
			o.logger.Error(err)
			return nil
		}

		err = txnFn(sctx, containers) // Performs transaction.
		if err == nil {
			return nil
		}

		o.logger.Error("Transaction aborted. Caught exception during transaction.")

		// If transient error, retry the whole transaction
		if cmdErr, ok := err.(mongo.CommandError); ok && cmdErr.HasErrorLabel("TransientTransactionError") {
			o.logger.Error("TransientTransactionError, retrying transaction...")
			continue
		}
		return err
	}
}

func (o *Output) AddData(sctx mongo.SessionContext, containers []stats.SampleContainer) error {
	type cacheItem struct {
		tags   map[string]string
		values map[string]interface{}
	}
	cache := map[*stats.SampleTags]cacheItem{}
	o.logger.Debug("samples:", containers)
	for _, container := range containers {
		samples := container.GetSamples()
		for _, sample := range samples {
			var tags map[string]string
			var values = make(map[string]interface{})
			if cached, ok := cache[sample.Tags]; ok {
				tags = cached.tags
				for k, v := range cached.values {
					values[k] = v
				}
			} else {
				tags = sample.Tags.CloneTags()
				o.extractTagsToValues(tags, values)
				cache[sample.Tags] = cacheItem{tags, values}
			}
			values["value"] = sample.Value
			//TODO add scenarios name and uid
			_, err := o.MongoCollection.InsertOne(o.MongoContext, bson.D{{"name", sample.Metric.Name},
				{"tags", tags}, {"values", values}, {"uuid", o.UUID}, {"time", sample.Time}})
			if err != nil {
				o.logger.Error(err)
			}
		}
	}
	return o.CommitWithRetry(sctx)
}


func (o *Output) extractTagsToValues(tags map[string]string, values map[string]interface{}) map[string]interface{} {
	o.logger.Debug("extractTagsToValues")
	for _, tag := range o.config.TagsAsFields {
		if val, ok := tags[tag]; ok {
			values[tag] = val
			delete(tags, tag)
		}
	}
	return values
}

// CommitWithRetry is an example function demonstrating transaction commit with retry logic.
func (o *Output) CommitWithRetry(sctx mongo.SessionContext) error {
	o.logger.Debug("CommitWithRetry")
	for {
		err := sctx.CommitTransaction(sctx)
		switch e := err.(type) {
		case nil:
			o.logger.Debug("Transaction committed.")
			return nil
		case mongo.CommandError:
			// Can retry commit
			if e.HasErrorLabel("UnknownTransactionCommitResult") {
				o.logger.Debug("UnknownTransactionCommitResult, retrying commit operation...")
				continue
			}
			o.logger.Error("Error during commit...")
			return e
		default:
			o.logger.Error("Error during commit...")
			return e
		}
	}
}

// Stop flushes any remaining metrics and stops the goroutine.
func (o *Output) Stop() error {
	o.logger.Debug("Stopping...")
	defer o.logger.Debug("Stopped!")
	o.periodicFlusher.Stop()
	return nil
}
