package config

import (
	"cmp"
	"fmt"
)

type validateFn func() error

func (c *Config) Validate(mode string) error {
	validations := []validateFn{
		notEmpty("mapping.path", c.Mapping.Path),
		inRange("tracing.sampling_rate", 0, 1, c.Tracing.SamplingRate),
	}

	switch mode {
	case "store":
		validations = append(validations, c.storeValidations()...)
	case "proxy":
		validations = append(validations, c.proxyValidations()...)
	case "single":
		validations = append(validations, c.proxyValidations()...)
		validations = append(validations, c.storeValidations()...)
	default:
		panic("unknown mode")
	}

	for _, fn := range validations {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Config) proxyValidations() []validateFn {
	return []validateFn{
		inRange("compression.docs_zstd_compression_level", -7, 22, c.Compression.DocsZstdCompressionLevel),
		inRange("compression.metas_zstd_compression_level", -7, 22, c.Compression.MetasZstdCompressionLevel),

		greaterThan("limits.query_rate", 0, c.Limits.QueryRate),
		greaterThan("limits.inflight_bulks", 0, c.Limits.InflightBulks),
		greaterThan("limits.doc_size", 0, c.Limits.DocSize),
	}
}

func (c *Config) storeValidations() []validateFn {
	validations := []validateFn{
		notEmpty("storage.data_dir", c.Storage.DataDir),
		greaterThan("storage.frac_size", 0, c.Storage.FracSize),
		greaterThan("storage.total_size", 0, c.Storage.TotalSize),

		greaterThan("limits.search_requests", 0, c.Limits.SearchRequests),
		greaterThan("limits.bulk_requests", 0, c.Limits.BulkRequests),
		greaterThan("limits.fraction_hits", 0, c.Limits.FractionHits),
		greaterThan("limits.search_docs", 0, c.Limits.SearchDocs),

		greaterThan("limits.aggregation.field_tokens", 0, c.Limits.Aggregation.FieldTokens),
		greaterThan("limits.aggregation.group_tokens", 0, c.Limits.Aggregation.GroupTokens),
		greaterThan("limits.aggregation.fraction_tokens", 0, c.Limits.Aggregation.FractionTokens),

		greaterThan("resources.reader_workers", 0, c.Resources.ReaderWorkers),
		greaterThan("resources.search_workers", 0, c.Resources.SearchWorkers),
		greaterThan("resources.cache_size", 0, c.Resources.CacheSize),

		inRange("compression.sealed_zstd_compression_level", -7, 22, c.Compression.SealedZstdCompressionLevel),
		inRange("compression.doc_block_zstd_compression_level", -7, 22, c.Compression.DocBlockZstdCompressionLevel),
	}

	if c.Offloading.Enabled {
		validations = append(validations,
			notEmpty("offloading.bucket", c.Offloading.Bucket),
			notEmpty("offloading.access_key", c.Offloading.AccessKey),
			notEmpty("offloading.secret_key", c.Offloading.SecretKey),
		)
	}

	return validations
}

func notEmpty[T comparable](field string, v T) validateFn {
	return func() error {
		var z T
		if v == z {
			return fmt.Errorf("field %q is required", field)
		}
		return nil
	}
}

func greaterThan[T cmp.Ordered](field string, base, v T) validateFn {
	return func() error {
		if v <= base {
			return fmt.Errorf(
				"field %q must be greater than %v",
				field, base,
			)
		}
		return nil
	}
}

func inRange[T cmp.Ordered](field string, from, to, v T) validateFn {
	return func() error {
		if v < from || to < v {
			return fmt.Errorf(
				"field %q must be in range [%v; %v]",
				field, from, to,
			)
		}
		return nil
	}
}
